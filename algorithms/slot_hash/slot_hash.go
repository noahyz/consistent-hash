package slot_hash

import (
	"consistent-hash/models"
	"consistent-hash/utils"
	"math"
	"sort"
	"strconv"
)

const (
	defaultNodeWeightBaseNum = 1000
	defaultSlotNum           = 1000
)

type slotDiff struct {
	slot    int
	nodeNum int64
	diff    float64
}

type slotNodeScore struct {
	slot    int
	nodeNum int64
	score   float64
}

type nkScore struct {
	nk    string
	score float64
}

type SlotHash[T models.HashNode] struct {
	nodeInstanceMap     map[string]T         // node -> 节点实例
	nodeSlotScoreMap    map[string][]float64 // node -> 每个槽位得分
	nodeReplicaQuotaMap map[string]int       // node -> 应加入槽位数量
	slotNodeTable       []*utils.Set[string] // 槽位 -> 可用node列表
	hashFunc            func([]byte) uint64  // 哈希函数
}

func NewSlotHash[T models.HashNode](nodes []T, hashFunc func([]byte) uint64) *SlotHash[T] {
	r := &SlotHash[T]{
		nodeInstanceMap:     make(map[string]T),
		nodeSlotScoreMap:    make(map[string][]float64),
		nodeReplicaQuotaMap: make(map[string]int),
		slotNodeTable:       make([]*utils.Set[string], defaultSlotNum),
		hashFunc:            hashFunc,
	}
	r.buildSlotHash(nodes)
	return r
}

func (r *SlotHash[T]) buildSlotHash(nodes []T) {
	for _, node := range nodes {
		nodeKey := node.GetKey()
		r.nodeInstanceMap[nodeKey] = node
	}
	for nk, nodeInstance := range r.nodeInstanceMap {
		r.nodeReplicaQuotaMap[nk] = getNodeQuota(nodeInstance.GetWeight())
		r.nodeSlotScoreMap[nk] = getNodeSlotScore(r.hashFunc, nk)
	}
	for slot := 0; slot < defaultSlotNum; slot++ {
		r.slotNodeTable[slot] = utils.NewSet[string]()
	}
	for _, node := range nodes {
		nk := node.GetKey()
		delta := r.nodeReplicaQuotaMap[nk]
		r.takeSlots(nk, delta)
	}
}

func (r *SlotHash[T]) Get(key string) []T {
	h := r.hashFunc([]byte(key))
	slot := int(h % uint64(defaultSlotNum))
	nodeKeys := r.slotNodeTable[slot]
	results := make([]T, 0)
	for _, nk := range nodeKeys.List() {
		node := r.nodeInstanceMap[nk]
		if node.IsEnabled() {
			results = append(results, node)
		}
	}
	return results
}

func (r *SlotHash[T]) AddNode(node T) {
	nodeKey := node.GetKey()
	r.nodeInstanceMap[nodeKey] = node
	r.nodeReplicaQuotaMap[nodeKey] = getNodeQuota(node.GetWeight())
	r.nodeSlotScoreMap[nodeKey] = getNodeSlotScore(r.hashFunc, nodeKey)
	delta := r.nodeReplicaQuotaMap[nodeKey]
	r.takeSlots(nodeKey, delta)
}

func (r *SlotHash[T]) SoftRemoveNode(nodeKey string) {
	r.nodeInstanceMap[nodeKey].SetEnabled(false)
}

func (r *SlotHash[T]) SoftRecoverNode(nodeKey string) {
	r.nodeInstanceMap[nodeKey].SetEnabled(true)
}

func (r *SlotHash[T]) HardRemoveNode(nodeKey string) {
	// 清理此节点
	delete(r.nodeInstanceMap, nodeKey)
	delete(r.nodeSlotScoreMap, nodeKey)
	delete(r.nodeReplicaQuotaMap, nodeKey)
	for _, nodeTable := range r.slotNodeTable {
		nodeTable.Remove(nodeKey)
	}
}

func (r *SlotHash[T]) UpdateNode(newNode T) {
	nodeKey := newNode.GetKey()
	oldNode, ok := r.nodeInstanceMap[nodeKey]
	if !ok || oldNode.GetWeight() == newNode.GetWeight() {
		return
	}
	r.nodeInstanceMap[nodeKey] = newNode
	oldQuota := r.nodeReplicaQuotaMap[nodeKey]
	// 重新计算quota
	newQuota := getNodeQuota(newNode.GetWeight())
	r.nodeReplicaQuotaMap[nodeKey] = newQuota
	if newQuota > oldQuota {
		// 抢占 delta 个槽位
		delta := newQuota - oldQuota
		r.takeSlots(nodeKey, delta)
	} else if newQuota < oldQuota {
		// 释放 delta 个槽位
		delta := oldQuota - newQuota
		r.releaseSlots(nodeKey, delta)
	}
}

func (r *SlotHash[T]) GetSlotTable() [][]string {
	slotNodeTable := make([][]string, len(r.slotNodeTable))
	for slot, nodeSet := range r.slotNodeTable {
		nodeKeys := make([]string, 0, nodeSet.Len())
		for _, nk := range nodeSet.List() {
			nodeKeys = append(nodeKeys, nk)
		}
		slotNodeTable[slot] = nodeKeys
	}
	return slotNodeTable
}

func (r *SlotHash[T]) GetNodeSlot(nodeKey string) []uint32 {
	slotList := make([]uint32, 0)
	for slot, nodeSet := range r.slotNodeTable {
		if nodeSet.Has(nodeKey) {
			slotList = append(slotList, uint32(slot))
		}
	}
	return slotList
}

func (r *SlotHash[T]) GetNodeWeight(nodeKey string) int {
	if nodeInstance, ok := r.nodeInstanceMap[nodeKey]; ok {
		return nodeInstance.GetWeight()
	}
	return 0
}

func (r *SlotHash[T]) RebalancedSlot(rebalancedBatchSlotSize int) int {
	// 收集空槽和多节点槽位
	emptySlots := make([]int, 0)
	overloadedSlots := make([]struct{ slot, extras int }, 0)
	for slot := 0; slot < defaultSlotNum; slot++ {
		sz := r.slotNodeTable[slot].Len()
		if sz == 0 {
			emptySlots = append(emptySlots, slot)
		} else if sz > 1 {
			overloadedSlots = append(overloadedSlots, struct{ slot, extras int }{slot, int(sz - 1)})
		}
	}
	// 如果没有空槽或者没有多节点槽，则直接返回
	if len(emptySlots) == 0 || len(overloadedSlots) == 0 {
		return 0
	}
	// 按照节点数量对槽位排序
	sort.Slice(overloadedSlots, func(i, j int) bool {
		return overloadedSlots[i].extras > overloadedSlots[j].extras
	})
	// 迁移节点
	moved := 0
	assign := 0
	for _, ols := range overloadedSlots {
		if moved >= rebalancedBatchSlotSize || assign >= len(emptySlots) {
			break
		}
		slot := ols.slot
		// 拿到当前槽中所有节点，按照当前槽位节点的哈希值排序
		itemNodes := make([]nkScore, 0)
		for _, nk := range r.slotNodeTable[slot].List() {
			itemNodes = append(itemNodes, nkScore{nk: nk, score: r.nodeSlotScoreMap[nk][slot]})
		}
		sort.Slice(itemNodes, func(i, j int) bool {
			return itemNodes[i].score < itemNodes[j].score
		})
		for i := 0; i < len(itemNodes)-1; i++ {
			if moved >= rebalancedBatchSlotSize || assign >= len(emptySlots) {
				break
			}
			nk := itemNodes[i].nk
			// 从原槽位删除，并添加到新槽位
			r.slotNodeTable[slot].Remove(nk)
			newSlot := emptySlots[assign]
			r.slotNodeTable[newSlot].Push(nk)
			assign++
			moved++
		}
	}
	return moved
}

// takeSlots 为节点抢占槽位
func (r *SlotHash[T]) takeSlots(nodeKey string, count int) {
	// 按照当前节点的槽位打分进行排序
	diffs := make([]*slotDiff, 0, defaultSlotNum)
	for slot := 0; slot < defaultSlotNum; slot++ {
		// 排除已经有此节点的槽位
		if r.slotNodeTable[slot].Has(nodeKey) {
			continue
		}
		// 按照分数进行排序
		maxScore := 0.0
		for _, scores := range r.nodeSlotScoreMap {
			if scores[slot] > maxScore {
				maxScore = scores[slot]
			}
		}
		scoreDiff := r.nodeSlotScoreMap[nodeKey][slot] - maxScore
		diffs = append(diffs, &slotDiff{slot: slot, nodeNum: r.slotNodeTable[slot].Len(), diff: scoreDiff})
	}
	sort.Slice(diffs, func(i, j int) bool {
		if diffs[i].nodeNum == diffs[j].nodeNum {
			return diffs[i].diff > diffs[j].diff
		}
		return diffs[i].nodeNum < diffs[j].nodeNum
	})
	// 先填充空槽位，再填充分值较大的槽位
	assigned := 0
	for _, it := range diffs {
		if assigned >= count {
			break
		}
		slot := it.slot
		r.slotNodeTable[slot].Push(nodeKey)
		assigned++
	}
}

func (r *SlotHash[T]) releaseSlots(nodeKey string, count int) {
	held := make([]*slotNodeScore, 0)
	for slot, chosen := range r.slotNodeTable {
		if chosen.Has(nodeKey) {
			held = append(held, &slotNodeScore{
				slot:    slot,
				nodeNum: chosen.Len(),
				score:   r.nodeSlotScoreMap[nodeKey][slot]})
		}
	}
	sort.Slice(held, func(i, j int) bool {
		if held[i].nodeNum == held[j].nodeNum {
			return held[i].score < held[j].score
		}
		return held[i].nodeNum > held[j].nodeNum
	})
	// 优先释放多节点的槽位，其次再是得分较低的槽位
	released := 0
	for _, it := range held {
		if released >= count {
			break
		}
		slot := it.slot
		r.slotNodeTable[slot].Remove(nodeKey)
		released++
	}
}

func (r *SlotHash[T]) getEmptySlotNum() int {
	number := 0
	for _, nodeTable := range r.slotNodeTable {
		if nodeTable.Empty() {
			number++
		}
	}
	return number
}

func (r *SlotHash[T]) countAssigned(nodeKey string) int {
	cnt := 0
	for _, chosen := range r.slotNodeTable {
		if chosen.Has(nodeKey) {
			cnt++
		}
	}
	return cnt
}

func (r *SlotHash[T]) totalWeight() int {
	weight := 0
	for _, node := range r.nodeInstanceMap {
		weight += node.GetWeight()
	}
	return weight
}

func (r *SlotHash[T]) sortedNodeByWeight() []string {
	nodes := make([]T, 0, len(r.nodeInstanceMap))
	for _, node := range r.nodeInstanceMap {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].GetWeight() > nodes[j].GetWeight()
	})
	nodeKeys := make([]string, 0, len(nodes))
	for _, node := range nodes {
		nodeKeys = append(nodeKeys, node.GetKey())
	}
	return nodeKeys
}

// pickBest 挑选槽上最佳节点
func (r *SlotHash[T]) pickBest(slot int) string {
	bestNodeKey, bestScore := "", -1.0
	for nk, arr := range r.nodeSlotScoreMap {
		if sc := arr[slot]; sc > bestScore {
			bestScore = sc
			bestNodeKey = nk
		}
	}
	return bestNodeKey
}

func (r *SlotHash[T]) pickBestByMinOverQuota(slot int) string {
	type cand struct {
		nk    string
		score float64
		over  int
	}
	var best cand
	best.score = -1
	best.over = math.MaxInt32
	for _, nk := range r.sortedNodeByWeight() {
		score := r.nodeSlotScoreMap[nk][slot]
		over := r.countAssigned(nk) - r.nodeReplicaQuotaMap[nk]
		if over < best.over || (over == best.over && score > best.score) {
			best.nk = nk
			best.over = over
			best.score = score
		}
	}
	return best.nk
}

func getNodeQuota(nodeWeight int) int {
	// 更新节点配额
	estimatedQuota := int(math.Round(float64(nodeWeight) / float64(defaultNodeWeightBaseNum) * float64(defaultSlotNum)))
	finalQuota := int(math.Max(float64(estimatedQuota), float64(1)))
	return finalQuota
}

func getNodeSlotScore(hashFunc func([]byte) uint64, nodeKey string) []float64 {
	slotScores := make([]float64, defaultSlotNum)
	for slot := 0; slot < defaultSlotNum; slot++ {
		score := getHashScore(hashFunc, nodeKey, slot)
		slotScores[slot] = score
	}
	return slotScores
}

func getHashScore(hashFunc func([]byte) uint64, nodeKey string, slot int) float64 {
	nodeSlot := []byte(nodeKey + "_" + strconv.Itoa(slot))
	rawHash := hashFunc(nodeSlot)
	// h 的取值范围是 [0, 2^64-1]。加1后是 [1, 2^64]
	// 将哈希值规整到范围 (0, 1]
	normalizedHash := (float64(rawHash) + 1) / float64(math.MaxUint64)
	logScore := 1.0 / -math.Log(normalizedHash)
	return logScore
}

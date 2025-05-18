package rendezvous_hash_v2

import (
	"consistent-hash/models"
	"consistent-hash/utils"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"
)

const (
	defaultNodeWeightBaseNum = 100000
	defaultSlotNum           = 100
)

type slotDiff struct {
	slot    int
	nodeNum int
	diff    float64
}

type slotNodeScore struct {
	slot    int
	nodeNum int
	score   float64
}

type nkScore struct {
	nk    string
	score float64
}

type timeWindow struct {
	start time.Duration
	end   time.Duration
}

type RendezvousHashV2[T models.HashNode] struct {
	nodeInstanceMap          map[string]T         // node -> 节点实例
	nodeSlotScoreMap         map[string][]float64 // node -> 每个槽位得分
	nodeReplicaQuotaMap      map[string]int       // node -> 应加入槽位数量
	slotNodeTable            []*utils.Set[string] // 槽位 -> 可用node列表
	hashFunc                 func([]byte) uint64
	rebalancedIntervalSecond int // 重平衡的时间周期
	rebalancedBatchSlotSize  int // 单次重平衡调整的槽位数量
	rebalancedForbidden      []timeWindow
	rebalancedStopCh         chan struct{}
}

func NewRendezvousHashV2[T models.HashNode](nodes []T, hashFunc func([]byte) uint64) *RendezvousHashV2[T] {
	r := &RendezvousHashV2[T]{
		nodeInstanceMap:          make(map[string]T),
		nodeSlotScoreMap:         make(map[string][]float64),
		nodeReplicaQuotaMap:      make(map[string]int),
		slotNodeTable:            make([]*utils.Set[string], defaultSlotNum),
		hashFunc:                 hashFunc,
		rebalancedIntervalSecond: 10,
		rebalancedBatchSlotSize:  5,
		rebalancedForbidden:      make([]timeWindow, 0),
		rebalancedStopCh:         make(chan struct{}),
	}
	r.buildRendezvousHash(nodes)
	r.StartAutoRebalanced()
	return r
}

func (r *RendezvousHashV2[T]) buildRendezvousHash(nodes []T) {
	for _, node := range nodes {
		nodeKey := node.GetKey()
		r.nodeInstanceMap[nodeKey] = node
	}
	nodeKeys := r.sortedNodeByWeight()
	for _, nk := range nodeKeys {
		r.updateNodeQuota(nk)
		r.updateNodeSlotScore(nk)
	}
	for slot := 0; slot < defaultSlotNum; slot++ {
		r.slotNodeTable[slot] = utils.NewSet[string]()
	}
	for _, nk := range nodeKeys {
		delta := r.nodeReplicaQuotaMap[nk]
		r.takeSlots(nk, delta)
	}
}

func (r *RendezvousHashV2[T]) StartAutoRebalanced() {
	if r.rebalancedIntervalSecond <= 0 || r.rebalancedBatchSlotSize <= 0 {
		return
	}
	ticker := time.NewTicker(time.Duration(r.rebalancedIntervalSecond) * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				// 跳过禁止时间段
				if r.isForbiddenNow() {
					continue
				}
				r.RebalancedSlot()
			case <-r.rebalancedStopCh:
				ticker.Stop()
				return
			}
		}
	}()
}

func (r *RendezvousHashV2[T]) StopAutoRebalanced() {
	if r.rebalancedStopCh != nil {
		r.rebalancedStopCh <- struct{}{}
	}
}

func (r *RendezvousHashV2[T]) isForbiddenNow() bool {
	now := time.Now()
	sinceMid := time.Duration(now.Hour())*time.Hour + time.Duration(now.Minute())*time.Minute + time.Duration(now.Second())*time.Second
	for _, w := range r.rebalancedForbidden {
		if sinceMid >= w.start && sinceMid < w.end {
			return true
		}
	}
	return false
}

//func (r *RendezvousHashV2[T]) buildNodeSlotTable(nodeKey string, count int) {
//	// 按照当前节点的槽位打分进行排序
//	diffs := make([]*slotDiff, 0, defaultSlotNum)
//	for slot := 0; slot < defaultSlotNum; slot++ {
//		maxScore := 0.0
//		for _, scores := range r.nodeSlotScoreMap {
//			if scores[slot] > maxScore {
//				maxScore = scores[slot]
//			}
//		}
//		scoreDiff := r.nodeSlotScoreMap[nodeKey][slot] - maxScore
//		diffs = append(diffs, &slotDiff{slot: slot, nodeNum: r.slotNodeTable[slot].Size(), diff: scoreDiff})
//	}
//	sort.Slice(diffs, func(i, j int) bool {
//		if diffs[i].nodeNum == diffs[j].nodeNum {
//			return diffs[i].diff > diffs[j].diff
//		}
//		return diffs[i].nodeNum < diffs[j].nodeNum
//	})
//	// 先填充空槽位，再填充分值较大的槽位
//	assigned := 0
//	for _, it := range diffs {
//		if assigned >= count {
//			break
//		}
//		slot := it.slot
//		r.slotNodeTable[slot].Add(nodeKey)
//		assigned++
//	}
//}

func (r *RendezvousHashV2[T]) getSlotTable() [][]string {
	slotNodeTable := make([][]string, defaultSlotNum)
	for slot, nodes := range r.slotNodeTable {
		chosen := nodes.Items()
		slotNodeTable[slot] = chosen
	}
	return slotNodeTable
}

func (r *RendezvousHashV2[T]) printSlotNodeTable() {
	slotNodeTable := make([][]string, len(r.slotNodeTable))
	for slot, nodes := range r.slotNodeTable {
		slotNodeTable[slot] = nodes.Items()
	}
	fmt.Printf("%v\n", slotNodeTable)
}

func (r *RendezvousHashV2[T]) debug() {
	//fmt.Printf("RendezvousHashV2 debug start\n")
	//slots := make([]int, 0, len(r.slotNodeTable))
	//for k := range r.slotNodeTable {
	//	slots = append(slots, k)
	//}
	//sort.Ints(slots)
	//
	//file, err := os.Create("slot_keys.txt")
	//if err != nil {
	//	fmt.Printf("create file err: %v\n", err)
	//	return
	//}
	//defer file.Close()

	//for _, k := range slots {
	//	_, err = fmt.Fprintf(file, "%v: %v\n", k, r.slotNodeTable[k])
	//	if err != nil {
	//		fmt.Printf("write file err: %v\n", err)
	//	}
	//}

	//nodeMap := make(map[string]int64)
	//for _, nodes := range r.slotNodeTable {
	//	for _, node := range nodes {
	//		nodeMap[node]++
	//	}
	//}
	//for k, v := range nodeMap {
	//	fmt.Printf("build node: %v, count: %v\n", k, v)
	//}
	//fmt.Printf("total slot num: %v\n", len(r.slotNodeTable))
	//emptySlots := 0
	//for slot := 0; slot < defaultSlotNum; slot++ {
	//	if len(r.slotNodeTable[slot]) == 0 {
	//		emptySlots++
	//	}
	//}
	//fmt.Printf("empty slot num: %v\n", emptySlots)
	//fmt.Printf("RendezvousHashV2 debug end\n\n")

	// 每个节点所占的槽位
	nodeNumMap := make(map[string]int64)
	for _, nks := range r.slotNodeTable {
		for _, nk := range nks.Items() {
			nodeNumMap[nk]++
		}
	}
	for k, v := range nodeNumMap {
		fmt.Printf("build node: %v, count: %v\n", k, v)
	}
	// 空槽位的个数
	emptySlots := 0
	for slot := 0; slot < defaultSlotNum; slot++ {
		if r.slotNodeTable[slot].Empty() {
			emptySlots++
		}
	}
	fmt.Printf("build empty slot num: %v\n", emptySlots)
	fmt.Println()
}

func (r *RendezvousHashV2[T]) Get(key string, hashFunc func([]byte) uint64) []T {
	// 获取主节点
	h := hashFunc([]byte(key))
	slot := int(h % uint64(defaultSlotNum))
	nodeKeys := r.slotNodeTable[slot]
	results := make([]T, 0)
	for _, nk := range nodeKeys.Items() {
		node := r.nodeInstanceMap[nk]
		if node.IsEnabled() {
			results = append(results, node)
		}
	}
	return results
}

func (r *RendezvousHashV2[T]) AddNode(node T) {
	nodeKey := node.GetKey()
	r.nodeInstanceMap[nodeKey] = node
	r.updateNodeQuota(nodeKey)
	r.updateNodeSlotScore(nodeKey)
	delta := r.nodeReplicaQuotaMap[nodeKey]
	r.takeSlots(nodeKey, delta)
}

func (r *RendezvousHashV2[T]) SoftRemoveNode(nodeKey string) {
	r.nodeInstanceMap[nodeKey].SetEnabled(false)
}

func (r *RendezvousHashV2[T]) SoftRecoverNode(nodeKey string) {
	r.nodeInstanceMap[nodeKey].SetEnabled(true)
}

func (r *RendezvousHashV2[T]) HardRemoveNode(nodeKey string) {
	// 清理此节点
	delete(r.nodeInstanceMap, nodeKey)
	delete(r.nodeSlotScoreMap, nodeKey)
	delete(r.nodeReplicaQuotaMap, nodeKey)
	for _, nodeTable := range r.slotNodeTable {
		nodeTable.Remove(nodeKey)
	}
}

func (r *RendezvousHashV2[T]) RebalancedSlot() {
	// 收集空槽和多节点槽位
	emptySlots := make([]int, 0)
	overloadedSlots := make([]struct{ slot, extras int }, 0)
	for slot := 0; slot < defaultSlotNum; slot++ {
		sz := r.slotNodeTable[slot].Size()
		if sz == 0 {
			emptySlots = append(emptySlots, slot)
		} else if sz > 1 {
			overloadedSlots = append(overloadedSlots, struct{ slot, extras int }{slot, sz - 1})
		}
	}
	fmt.Printf("empty slots: %v\n", len(emptySlots))
	// 如果没有空槽或者没有多节点槽，则直接返回
	if len(emptySlots) == 0 || len(overloadedSlots) == 0 {
		return
	}
	// 按照节点数量对槽位排序
	sort.Slice(overloadedSlots, func(i, j int) bool {
		return overloadedSlots[i].extras > overloadedSlots[j].extras
	})
	// 迁移节点
	moved := 0
	assign := 0
	for _, ols := range overloadedSlots {
		if moved >= r.rebalancedBatchSlotSize || assign >= len(emptySlots) {
			break
		}
		slot := ols.slot
		// 拿到当前槽中所有节点，按照当前槽位节点的哈希值排序
		itemNodes := make([]nkScore, 0)
		for _, nk := range r.slotNodeTable[slot].Items() {
			itemNodes = append(itemNodes, nkScore{nk: nk, score: r.nodeSlotScoreMap[nk][slot]})
		}
		sort.Slice(itemNodes, func(i, j int) bool {
			return itemNodes[i].score > itemNodes[j].score
		})
		for i := 0; i < len(itemNodes); i++ {
			if moved >= r.rebalancedBatchSlotSize || assign >= len(emptySlots) {
				break
			}
			nk := itemNodes[i].nk
			// 从原槽位删除，并添加到新槽位
			r.slotNodeTable[slot].Remove(nk)
			newSlot := emptySlots[assign]
			r.slotNodeTable[newSlot].Add(nk)
			assign++
			moved++
		}
	}

	//// 此时如果存在空槽位，并且空槽位和节点总权重不相符，则进行填补空槽位
	//emptySlotNum := r.getEmptySlotNum()
	//expectFillSlotNum := int(math.Ceil(float64(r.totalWeight()) / float64(defaultNodeWeightBaseNum) * float64(defaultSlotNum)))
	//if expectFillSlotNum >= defaultSlotNum {
	//	expectFillSlotNum = defaultSlotNum
	//}
	//expectEmptySlotNum := defaultSlotNum - expectFillSlotNum
	//if emptySlotNum <= 0 || emptySlotNum <= expectEmptySlotNum {
	//	return
	//}
	//// 填补空槽位：只处理那些具有多个节点的槽位
	//needFillSlotNum := expectEmptySlotNum - emptySlotNum
	//fmt.Printf("needFillSlotNum: %v\n", needFillSlotNum)

}

func (r *RendezvousHashV2[T]) UpdateNode(newNode T) {
	nodeKey := newNode.GetKey()
	oldNode, ok := r.nodeInstanceMap[nodeKey]
	if !ok || oldNode.GetWeight() == newNode.GetWeight() {
		return
	}
	r.nodeInstanceMap[nodeKey] = newNode
	oldQuota := r.nodeReplicaQuotaMap[nodeKey]
	// 重新计算quota
	r.updateNodeQuota(nodeKey)
	newQuota := r.nodeReplicaQuotaMap[nodeKey]
	if newQuota > oldQuota {
		// 抢占 delta 个槽位
		delta := newQuota - oldQuota
		r.takeSlots(nodeKey, delta)
	} else if newQuota < oldQuota {
		// 释放 delta 个槽位
		delta := oldQuota - newQuota
		r.releaseSlots(nodeKey, delta)
	} else {
		fmt.Printf("The weight change is too small and there is no need to move the slot")
	}
}

func (r *RendezvousHashV2[T]) hashScore(nodeKey string, slot int) float64 {
	//node := r.nodeInstanceMap[nodeKey]
	nodeSlot := []byte(nodeKey + "_" + strconv.Itoa(slot))
	rawHash := r.hashFunc(nodeSlot)
	// h 的取值范围是 [0, 2^64-1]。加1后是 [1, 2^64]
	// 将哈希值规整到范围 (0, 1]
	normalizedHash := (float64(rawHash) + 1) / float64(math.MaxUint64)
	logScore := 1.0 / -math.Log(normalizedHash)
	return logScore
	//weightedScore := logScore * float64(node.GetWeight())
	//return weightedScore
}

func (r *RendezvousHashV2[T]) updateNodeQuota(nodeKey string) {
	// 更新节点配额
	node := r.nodeInstanceMap[nodeKey]
	estimatedQuota := int(math.Floor(float64(node.GetWeight()) / float64(defaultNodeWeightBaseNum) * float64(defaultSlotNum)))
	finalQuota := int(math.Max(float64(estimatedQuota), float64(1)))
	r.nodeReplicaQuotaMap[nodeKey] = finalQuota
}

func (r *RendezvousHashV2[T]) updateNodeSlotScore(nodeKey string) {
	slotScores := make([]float64, defaultSlotNum)
	for slot := 0; slot < defaultSlotNum; slot++ {
		score := r.hashScore(nodeKey, slot)
		slotScores[slot] = score
	}
	r.nodeSlotScoreMap[nodeKey] = slotScores
}

// takeSlots 为节点抢占槽位
func (r *RendezvousHashV2[T]) takeSlots(nodeKey string, count int) {
	// 按照当前节点的槽位打分进行排序
	diffs := make([]*slotDiff, 0, defaultSlotNum)
	for slot := 0; slot < defaultSlotNum; slot++ {
		maxScore := 0.0
		for _, scores := range r.nodeSlotScoreMap {
			if scores[slot] > maxScore {
				maxScore = scores[slot]
			}
		}
		scoreDiff := r.nodeSlotScoreMap[nodeKey][slot] - maxScore
		diffs = append(diffs, &slotDiff{slot: slot, nodeNum: r.slotNodeTable[slot].Size(), diff: scoreDiff})
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
		r.slotNodeTable[slot].Add(nodeKey)
		assigned++
	}
}

func (r *RendezvousHashV2[T]) releaseSlots(nodeKey string, count int) {
	held := make([]*slotNodeScore, 0)
	for slot, chosen := range r.slotNodeTable {
		if chosen.Contains(nodeKey) {
			held = append(held, &slotNodeScore{
				slot:    slot,
				nodeNum: chosen.Size(),
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
	//
	//totalWeight := r.totalWeight()
	//for _, it := range held {
	//	if released >= count {
	//		break
	//	}
	//	slot := it.slot
	//	r.slotNodeTable[slot] = removeKey(r.slotNodeTable[slot], nodeKey)
	//	released++
	//	if len(r.slotNodeTable[slot]) == 0 && totalWeight >= defaultNodeWeightBaseNum {
	//		bestNodeKey := r.pickBestByMinOverQuota(slot)
	//		r.slotNodeTable[slot] = []string{bestNodeKey}
	//	}
	//}
}

func (r *RendezvousHashV2[T]) getEmptySlotNum() int {
	number := 0
	for _, nodeTable := range r.slotNodeTable {
		if nodeTable.Empty() {
			number++
		}
	}
	return number
}

func (r *RendezvousHashV2[T]) countAssigned(nodeKey string) int {
	cnt := 0
	for _, chosen := range r.slotNodeTable {
		if chosen.Contains(nodeKey) {
			cnt++
		}
	}
	return cnt
}

func (r *RendezvousHashV2[T]) totalWeight() int {
	weight := 0
	for _, node := range r.nodeInstanceMap {
		weight += node.GetWeight()
	}
	return weight
}

func (r *RendezvousHashV2[T]) sortedNodeByWeight() []string {
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
func (r *RendezvousHashV2[T]) pickBest(slot int) string {
	bestNodeKey, bestScore := "", -1.0
	for nk, arr := range r.nodeSlotScoreMap {
		if sc := arr[slot]; sc > bestScore {
			bestScore = sc
			bestNodeKey = nk
		}
	}
	return bestNodeKey
}

func (r *RendezvousHashV2[T]) pickBestByMinOverQuota(slot int) string {
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

func removeKey(list []string, key string) []string {
	results := make([]string, 0, len(list))
	for _, k := range list {
		if k != key {
			results = append(results, k)
		}
	}
	return results
}

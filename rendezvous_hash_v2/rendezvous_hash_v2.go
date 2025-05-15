package rendezvous_hash_v2

import (
	"consistent-hash/models"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
)

const (
	defaultNodeWeightBaseNum = 100000
	defaultSlotNum           = 100000
)

type slotDiff struct {
	slot int
	diff float64
}

type SlotScore struct {
	slot  int
	score float64
}

type RendezvousHashV2[T models.HashNode] struct {
	nodeMap      map[string]T         // node -> 节点实例
	scoreMap     map[string][]float64 // node -> 每个槽位得分
	replicaQuota map[string]int       // node -> 应加入槽位数量
	slotTable    map[int][]string     // 槽位 -> 可用node列表
	hashFunc     func([]byte) uint64
}

func NewRendezvousHashV2[T models.HashNode](nodes []T, hashFunc func([]byte) uint64) *RendezvousHashV2[T] {
	r := &RendezvousHashV2[T]{
		nodeMap:      make(map[string]T),
		scoreMap:     make(map[string][]float64),
		replicaQuota: make(map[string]int),
		slotTable:    make(map[int][]string),
		hashFunc:     hashFunc,
	}
	r.buildRendezvousHash(nodes)
	return r
}

func (r *RendezvousHashV2[T]) buildRendezvousHash(nodes []T) {
	for _, node := range nodes {
		nodeKey := node.GetKey()
		r.nodeMap[nodeKey] = node
		r.scoreMap[nodeKey] = make([]float64, defaultSlotNum)
	}
	r.slotTable = make(map[int][]string)
	for nk := range r.nodeMap {
		r.updateQuotaAndScores(nk)
	}
	for nk := range r.nodeMap {
		delta := r.replicaQuota[nk]
		r.takeSlots(nk, delta)
	}
}

func (r *RendezvousHashV2[T]) Get(key string, hashFunc func([]byte) uint64) []T {
	// 获取主节点
	h := hashFunc([]byte(key))
	slot := int(h % uint64(defaultSlotNum))
	nodeKeys := r.slotTable[slot]
	results := make([]T, 0)
	for _, nk := range nodeKeys {
		results = append(results, r.nodeMap[nk])
	}
	return results
}

func (r *RendezvousHashV2[T]) getSlotTable() map[int][]string {
	slotTable := make(map[int][]string)
	for k, nodeKeys := range r.slotTable {
		chosen := make([]string, 0)
		chosen = append(chosen, nodeKeys...)
		slotTable[k] = chosen
	}
	return slotTable
}

func (r *RendezvousHashV2[T]) debug() {
	fmt.Printf("RendezvousHashV2 debug start\n")
	slots := make([]int, 0, len(r.slotTable))
	for k := range r.slotTable {
		slots = append(slots, k)
	}
	sort.Ints(slots)
	file, err := os.Create("slot_keys.txt")
	if err != nil {
		fmt.Printf("create file err: %v\n", err)
		return
	}
	defer file.Close()

	for _, k := range slots {
		_, err = fmt.Fprintf(file, "%v: %v\n", k, r.slotTable[k])
		if err != nil {
			fmt.Printf("write file err: %v\n", err)
		}
	}

	nodeMap := make(map[string]int64)
	for _, nodes := range r.slotTable {
		for _, node := range nodes {
			nodeMap[node]++
		}
	}
	for k, v := range nodeMap {
		fmt.Printf("node: %v, count: %v\n", k, v)
	}
	fmt.Printf("RendezvousHashV2 debug end\n")
}

func (r *RendezvousHashV2[T]) AddNode(node T) {
	nodeKey := node.GetKey()
	r.nodeMap[nodeKey] = node
	r.scoreMap[nodeKey] = make([]float64, defaultSlotNum)
	r.updateQuotaAndScores(nodeKey)
	delta := r.replicaQuota[nodeKey]
	r.takeSlots(nodeKey, delta)
}

func (r *RendezvousHashV2[T]) RemoveNode(nodeKey string) {
	delete(r.nodeMap, nodeKey)
	delete(r.scoreMap, nodeKey)
	delete(r.replicaQuota, nodeKey)
	for slot, chosen := range r.slotTable {
		for j, nk := range chosen {
			if nk == nodeKey {
				r.slotTable[slot] = append(chosen[:j], chosen[j+1:]...)
				break
			}
		}
	}
}

func (r *RendezvousHashV2[T]) UpdateNode(newNode T) {
	nodeKey := newNode.GetKey()
	oldNode, ok := r.nodeMap[nodeKey]
	if !ok || oldNode.GetWeight() == newNode.GetWeight() {
		return
	}
	r.nodeMap[nodeKey] = newNode
	oldQuota := r.replicaQuota[nodeKey]
	// 重新计算quota和scores
	r.updateQuotaAndScores(nodeKey)
	newQuota := r.replicaQuota[nodeKey]
	if newQuota > oldQuota {
		// 抢占几个槽位
		delta := newQuota - oldQuota
		r.takeSlots(nodeKey, delta)
	} else if newQuota < oldQuota {
		// 释放几个槽位
		delta := oldQuota - newQuota
		r.releaseSlots(nodeKey, delta)
	} else {
		fmt.Printf("The weight change is too small and there is no need to move the slot")
	}
}

func (r *RendezvousHashV2[T]) hashScore(nodeKey string, slot int) float64 {
	node := r.nodeMap[nodeKey]
	nodeSlot := []byte(nodeKey + "_" + strconv.Itoa(slot))
	rawHash := r.hashFunc(nodeSlot)
	// h 的取值范围是 [0, 2^64-1]。加1后是 [1, 2^64]
	// 将哈希值规整到范围 (0, 1]
	normalizedHash := (float64(rawHash) + 1) / float64(math.MaxUint64)
	logScore := 1.0 / -math.Log(normalizedHash)
	weightedScore := logScore * float64(node.GetWeight())
	return weightedScore
}

func (r *RendezvousHashV2[T]) updateQuotaAndScores(nodeKey string) {
	node := r.nodeMap[nodeKey]
	newQuota := int(math.Floor(float64(node.GetWeight()) / float64(defaultNodeWeightBaseNum) * float64(defaultSlotNum)))
	r.replicaQuota[nodeKey] = newQuota
	for slot := 0; slot < defaultSlotNum; slot++ {
		score := r.hashScore(nodeKey, slot)
		r.scoreMap[nodeKey][slot] = score
	}
}

// takeSlots 为节点抢占槽位
func (r *RendezvousHashV2[T]) takeSlots(nodeKey string, count int) {
	diffs := make([]*slotDiff, 0, defaultSlotNum)
	for slot := 0; slot < defaultSlotNum; slot++ {
		maxScore := 0.0
		for _, nk := range r.slotTable[slot] {
			if sc := r.scoreMap[nk][slot]; sc > maxScore {
				maxScore = sc
			}
		}
		scoreDiff := r.scoreMap[nodeKey][slot] - maxScore
		diffs = append(diffs, &slotDiff{slot: slot, diff: scoreDiff})
	}
	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].diff > diffs[j].diff
	})
	totalWeight := r.getTotalNodeWeight()
	assigned := 0
	for i := 0; assigned < count && i < len(diffs); i++ {
		slot := diffs[i].slot
		// 当权重不满，则仅抢占空槽位
		if totalWeight <= defaultNodeWeightBaseNum {
			if len(r.slotTable[slot]) == 0 {
				r.slotTable[slot] = append(r.slotTable[slot], nodeKey)
				assigned++
			}
		} else {
			r.slotTable[slot] = append(r.slotTable[slot], nodeKey)
			assigned++
		}
	}
}

// releaseSlots 释放节点对应的最弱的槽位
func (r *RendezvousHashV2[T]) releaseSlots(nodeKey string, count int) {
	scores := make([]*SlotScore, 0, len(r.slotTable))
	for slot, chosen := range r.slotTable {
		for _, nk := range chosen {
			if nodeKey == nk {
				scores = append(scores, &SlotScore{slot: slot, score: r.scoreMap[nodeKey][slot]})
			}
		}
	}
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score < scores[j].score
	})
	totalWeight := r.getTotalNodeWeight()
	for i := 0; i < count && i < len(scores); i++ {
		slot := scores[i].slot
		chosen := r.slotTable[slot]
		for j, nk := range chosen {
			if nk == nodeKey {
				r.slotTable[slot] = append(chosen[:j], chosen[j+1:]...)
				break
			}
		}
		if len(r.slotTable[slot]) == 0 && totalWeight > defaultNodeWeightBaseNum {
			bestNodeKey, bestScore := "", -1.0
			for nk, arr := range r.scoreMap {
				if sc := arr[slot]; sc > bestScore {
					bestScore = sc
					bestNodeKey = nk
				}
			}
			r.slotTable[slot] = []string{bestNodeKey}
		}
	}
}

func (r *RendezvousHashV2[T]) getTotalNodeWeight() int {
	weight := 0
	for _, node := range r.nodeMap {
		weight += node.GetWeight()
	}
	return weight
}

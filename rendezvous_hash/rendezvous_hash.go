package rendezvous_hash

import (
	"consistent-hash/models"
	"math"
	"strconv"
)

type pair struct {
	diff float64
	slot int
}

type RendezvousHash[T models.HashNode] struct {
	slotNum   int                  // 槽位数量
	nodeMap   map[string]T         // key为机房名称
	scoreMap  map[string][]float64 // key为nodeId，value为某个槽位的得分
	slotTable []string             // 某个槽位的主节点
	hashFunc  func([]byte) uint64
}

func NewRendezvousHash[T models.HashNode](slotNum int, nodes []T, hashFunc func([]byte) uint64) *RendezvousHash[T] {
	r := &RendezvousHash[T]{
		slotNum:   slotNum,
		nodeMap:   make(map[string]T),
		scoreMap:  make(map[string][]float64),
		slotTable: make([]string, slotNum),
		hashFunc:  hashFunc,
	}
	for _, node := range nodes {
		nodeKey := node.GetKey()
		r.nodeMap[nodeKey] = node
		r.scoreMap[nodeKey] = make([]float64, r.slotNum)
	}
	r.buildRendezvousHash()

	//keyMap := make(map[string]int64)
	//for idx, item := range r.slotTable {
	//	if item == "node_3" {
	//		fmt.Printf("%v ", idx)
	//	}
	//	if _, ok := keyMap[item]; !ok {
	//		keyMap[item] = 1
	//	} else {
	//		keyMap[item]++
	//	}
	//}
	//fmt.Println()
	//for key, num := range keyMap {
	//	fmt.Printf("key: %v, num: %v\n", key, num)
	//}

	return r
}

func (r *RendezvousHash[T]) buildRendezvousHash() {
	// 计算机房得分
	for _, node := range r.nodeMap {
		r.computeScore(node)
	}
	// 为每个槽位选择最高得分的机房
	for i := 0; i < r.slotNum; i++ {
		bestNode := ""
		bestScore := -1.0
		for _, node := range r.nodeMap {
			nodeKey := node.GetKey()
			if score := r.scoreMap[nodeKey][i]; score > bestScore {
				bestScore = score
				bestNode = nodeKey
			}
		}
		r.slotTable[i] = bestNode
	}
}

func (r *RendezvousHash[T]) computeScore(node T) {
	nodeKey := node.GetKey()
	w := node.GetWeight()
	for i := 0; i < r.slotNum; i++ {
		h := r.hashScore(nodeKey, i)
		score := 1.0 / -math.Log(h)
		r.scoreMap[nodeKey][i] = score * float64(w)
		// fmt.Printf("node: %v, slot: %v, h: %v, score: %v\n", nodeKey, i, h, score)
	}
}

func (r *RendezvousHash[T]) hashScore(nodeKey string, slot int) float64 {
	nodeSlot := []byte(nodeKey + "_" + strconv.Itoa(slot))
	h := r.hashFunc(nodeSlot)
	// h 的取值范围是 [0, 2^64-1]。加1后是 [1, 2^64]
	// 返回值的范围是 (0, 1]
	return (float64(h) + 1) / float64(math.MaxUint64)
}

func (r *RendezvousHash[T]) getSlotTable() map[int]string {
	slotTable := make(map[int]string)
	for k, nodeKey := range r.slotTable {
		slotTable[k] = nodeKey
	}
	return slotTable
}

func (r *RendezvousHash[T]) AddNode(node T) {
	// 注册新节点
	r.nodeMap[node.GetKey()] = node
	r.scoreMap[node.GetKey()] = make([]float64, r.slotNum)
	r.computeScore(node)
	// 计算总权重并得出新机房应占槽位数
	totalWeight := 0.0
	for _, tempNode := range r.nodeMap {
		totalWeight += float64(tempNode.GetWeight())
	}
	k := int(math.Floor(float64(node.GetWeight()) / totalWeight * float64(r.slotNum)))
	// 计算新节点与当前节点的得分差，选择前k个最高差值槽位
	diffs := make([]pair, r.slotNum)
	for i := 0; i < r.slotNum; i++ {
		diffs[i] = pair{
			diff: r.scoreMap[node.GetKey()][i] - r.scoreMap[r.slotTable[i]][i],
			slot: i,
		}
	}
	// 排序后选择前k个
	for i := 0; i < k; i++ {
		maxIdx := i
		for j := i + 1; j < r.slotNum; j++ {
			if diffs[j].diff > diffs[maxIdx].diff {
				maxIdx = j
			}
		}
		temp := diffs[i]
		diffs[i] = diffs[maxIdx]
		diffs[maxIdx] = temp
		r.slotTable[diffs[i].slot] = node.GetKey()
	}
}

func (r *RendezvousHash[T]) RemoveNode(node T) {
	// 收集受影响的槽位
	assign := make([]int, 0)
	for i, nodeKey := range r.slotTable {
		if nodeKey == node.GetKey() {
			assign = append(assign, i)
		}
	}
	// 从列表中剔除
	delete(r.nodeMap, node.GetKey())
	delete(r.scoreMap, node.GetKey())
	// 对受影响的槽位重新选取分数最高的机房
	for _, i := range assign {
		bestNode := ""
		bestScore := -1.0
		for _, tempNode := range r.nodeMap {
			if score := r.scoreMap[tempNode.GetKey()][i]; score > bestScore {
				bestScore = score
				bestNode = tempNode.GetKey()
			}
		}
		r.slotTable[i] = bestNode
	}
}

// UpdateNode 调整机房的权重
func (r *RendezvousHash[T]) UpdateNode(node T) {
	// 节点的权重是否变化
	nodeKey := node.GetKey()
	nodeNewWeight := node.GetWeight()
	nodeOldWeight := r.nodeMap[nodeKey].GetWeight()
	if nodeOldWeight == nodeNewWeight {
		return
	}
	// 总权重
	oldWeightSum, newWeightSum := 0.0, 0.0
	for _, tempNode := range r.nodeMap {
		oldWeightSum += float64(tempNode.GetWeight())
	}
	r.nodeMap[nodeKey] = node
	for _, tempNode := range r.nodeMap {
		newWeightSum += float64(tempNode.GetWeight())
	}
	// 计算新/旧应得槽位数
	oldK := int(math.Floor(float64(nodeOldWeight) / oldWeightSum * float64(r.slotNum)))
	newK := int(math.Floor(float64(nodeNewWeight) / newWeightSum * float64(r.slotNum)))
	delta := newK - oldK
	// 更新 node 权重
	r.computeScore(node)
	// 仅重新映射 delta 个槽位
	if delta > 0 {
		// 抢占: 选 diff 最大的 delta 个槽位
		candidates := make([]pair, 0)
		for i := 0; i < r.slotNum; i++ {
			if r.slotTable[i] != nodeKey {
				candidates = append(candidates, pair{
					diff: r.scoreMap[nodeKey][i] - r.scoreMap[r.slotTable[i]][i],
					slot: i,
				})
			}
		}
		for i := 0; i < delta && i < len(candidates); i++ {
			maxIdx := i
			for j := i + 1; j < len(candidates); j++ {
				if candidates[j].diff > candidates[maxIdx].diff {
					maxIdx = j
				}
			}
			temp := candidates[i]
			candidates[i] = candidates[maxIdx]
			candidates[maxIdx] = temp
			r.slotTable[candidates[i].slot] = nodeKey
		}
	} else {
		// 释放：选得分最小的 delta 个槽位
		owned := make([]int, 0)
		for i, currNodeKey := range r.slotTable {
			if nodeKey == currNodeKey {
				owned = append(owned, i)
			}
		}
		release := -delta
		if release > len(owned) {
			release = len(owned)
		}
		// 选择最小的 release 个
		for k := 0; k < release; k++ {
			minIdx := k
			for j := k + 1; j < len(owned); j++ {
				if r.scoreMap[nodeKey][owned[j]] < r.scoreMap[nodeKey][owned[minIdx]] {
					minIdx = j
				}
			}
			temp := owned[k]
			owned[k] = owned[minIdx]
			owned[minIdx] = temp
			slot := owned[k]
			// 分配给次优机房
			bestNodeKey := ""
			bestScore := -1.0
			for _, other := range r.nodeMap {
				if other.GetKey() == nodeKey {
					continue
				}
				if score := r.scoreMap[other.GetKey()][slot]; score > bestScore {
					bestScore = score
					bestNodeKey = other.GetKey()
				}
			}
			r.slotTable[slot] = bestNodeKey
		}
	}
}

func (r *RendezvousHash[T]) Get(key string, number int, hashFunc func([]byte) uint64) []T {
	if number > len(r.nodeMap) {
		number = len(r.nodeMap)
	}
	results := make([]T, 0, number)
	// 获取主节点
	h := hashFunc([]byte(key))
	slot := h % uint64(r.slotNum)
	masterNodeKey := r.slotTable[slot]
	masterNode := r.nodeMap[masterNodeKey]
	results = append(results, masterNode)
	// TODO 获取对应槽位的备节点
	return results
}

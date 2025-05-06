package ring_hash

import (
	"consistent-hash/models"
	"sort"
	"strconv"
)

type RingHash[T models.HashNode] struct {
	vnodeBaseNum    int          // 虚拟节点基数
	ringFloorLimit  int          // 建环的下限
	totalNodes      int          // 真实节点数
	nodeList        []T          // 节点信息(无需建环的场景)
	vnodeSortedList []uint64     // 已排序的虚拟节点哈希数组
	nodeMap         map[uint64]T // 从虚拟节点哈希到真实节点的映射，key为hashCode
}

func NewRingHash[T models.HashNode](vnodeBaseNum, ringFloorLimit int, nodes []T,
	hashFunc func([]byte) uint64) *RingHash[T] {
	obj := &RingHash[T]{
		vnodeBaseNum:   vnodeBaseNum,
		ringFloorLimit: ringFloorLimit,
		totalNodes:     len(nodes),
		nodeMap:        make(map[uint64]T),
	}
	obj.buildRingHash(nodes, hashFunc)
	return obj
}

func (r *RingHash[T]) buildRingHash(nodeList []T, hashFunc func([]byte) uint64) {
	if len(nodeList) <= r.ringFloorLimit {
		r.nodeList = nodeList
		return
	}
	for _, node := range nodeList {
		r.addVNode(node, hashFunc)
	}
	sort.Slice(r.vnodeSortedList, func(i, j int) bool {
		return r.vnodeSortedList[i] < r.vnodeSortedList[j]
	})
}

func (r *RingHash[T]) addVNode(node T, hashFunc func([]byte) uint64) {
	vnodeCount := node.GetWeight() * r.vnodeBaseNum
	for i := 0; i < vnodeCount; i++ {
		vnodeKey := []byte(node.GetKey() + "_" + strconv.Itoa(i))
		hashCode := hashFunc(vnodeKey)
		r.vnodeSortedList = append(r.vnodeSortedList, hashCode)
		r.nodeMap[hashCode] = node
	}
}

func (r *RingHash[T]) Get(key string, number int, hashFunc func([]byte) uint64) []T {
	// 期望获取的节点数量和真实节点数量做比较
	if number > r.totalNodes {
		number = r.totalNodes
	}
	// 无需从环上取
	if len(r.nodeList) > 0 {
		if number > len(r.nodeList) {
			number = len(r.nodeList)
		}
		return r.nodeList[:number]
	}
	// 判断环是否为空
	if len(r.vnodeSortedList) <= 0 {
		return []T{}
	}
	hashCode := hashFunc([]byte(key))
	// 二分法找到第一个大于等于 hashCode 的索引。如果都小于，则返回 0(环形)
	idx := sort.Search(len(r.vnodeSortedList), func(i int) bool {
		return r.vnodeSortedList[i] >= hashCode
	})
	if idx == len(r.vnodeSortedList) {
		idx = 0
	}
	// 顺时针收集不重复的节点
	results := make([]T, 0, number)
	seen := make(map[string]struct{})
	vnodeCount := len(r.vnodeSortedList)
	for i := 0; len(results) < number; i++ {
		vnodeHashCode := r.vnodeSortedList[(idx+i)%vnodeCount]
		node := r.nodeMap[vnodeHashCode]
		keyStr := node.GetKey()
		if _, ok := seen[keyStr]; !ok {
			seen[keyStr] = struct{}{}
			results = append(results, node)
		}
	}
	return results
}

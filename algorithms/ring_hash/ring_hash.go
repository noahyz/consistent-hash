package ring_hash

import (
	"consistent-hash/models"
	"fmt"
	"sort"
	"strconv"
)

type RingHash[T models.HashNode] struct {
	vnodeBaseNum    int                 // 虚拟节点基数
	ringFloorLimit  int                 // 建环的下限
	totalNodes      int                 // 真实节点数
	nodeMap         map[string]T        // 节点信息, key为nodeKey
	vnodeSortedList []uint64            // 已排序的虚拟节点哈希数组
	vnodeHashMap    map[uint64]T        // 从虚拟节点哈希到真实节点的映射，key为hashCode
	hashFunc        func([]byte) uint64 // 哈希函数
}

func NewRingHash[T models.HashNode](vnodeBaseNum, ringFloorLimit int, nodes []T,
	hashFunc func([]byte) uint64) *RingHash[T] {
	obj := &RingHash[T]{
		vnodeBaseNum:    vnodeBaseNum,
		ringFloorLimit:  ringFloorLimit,
		totalNodes:      len(nodes),
		nodeMap:         make(map[string]T),
		vnodeSortedList: make([]uint64, 0),
		vnodeHashMap:    make(map[uint64]T),
		hashFunc:        hashFunc,
	}
	obj.buildRingHash(nodes)
	return obj
}

func (r *RingHash[T]) buildRingHash(nodeList []T) {
	for _, node := range nodeList {
		r.nodeMap[node.GetKey()] = node
	}
	// 如果节点数较少，无需建环
	if len(r.nodeMap) <= r.ringFloorLimit {
		return
	}
	for _, node := range nodeList {
		vnodeHashKeys := r.generateVNodeHashKeys(node)
		for _, hashKey := range vnodeHashKeys {
			r.vnodeHashMap[hashKey] = node
		}
	}
	r.updateSortedHashKeys()
}

func (r *RingHash[T]) updateSortedHashKeys() {
	r.vnodeSortedList = make([]uint64, 0, len(r.vnodeHashMap))
	for hashKey := range r.vnodeHashMap {
		r.vnodeSortedList = append(r.vnodeSortedList, hashKey)
	}
	sort.Slice(r.vnodeSortedList, func(i, j int) bool {
		return r.vnodeSortedList[i] < r.vnodeSortedList[j]
	})
}

func (r *RingHash[T]) generateVNodeHashKeys(node T) []uint64 {
	vnodeHashKeys := make([]uint64, 0)
	vnodeCount := node.GetWeight() * r.vnodeBaseNum
	for i := 0; i < vnodeCount; i++ {
		vnodeKey := []byte(node.GetKey() + "_" + strconv.Itoa(i))
		hashCode := r.hashFunc(vnodeKey)
		vnodeHashKeys = append(vnodeHashKeys, hashCode)
	}
	return vnodeHashKeys
}

func (r *RingHash[T]) AddNode(node T) {
	nodeKey := node.GetKey()
	if _, ok := r.nodeMap[nodeKey]; ok {
		return
	}
	r.nodeMap[nodeKey] = node
	// 如果节点数较少，无需建环
	if len(r.nodeMap) <= r.ringFloorLimit {
		return
	}
	// 添加虚拟节点
	vnodeHashKeys := r.generateVNodeHashKeys(node)
	for _, hashKey := range vnodeHashKeys {
		r.vnodeHashMap[hashKey] = node
	}
	r.updateSortedHashKeys()
}

func (r *RingHash[T]) RemoveNode(node T) {
	nodeKey := node.GetKey()
	if _, ok := r.nodeMap[nodeKey]; !ok {
		return
	}
	delete(r.nodeMap, nodeKey)
	// 删除虚拟节点
	vnodeHashKeys := r.generateVNodeHashKeys(node)
	for _, hashKey := range vnodeHashKeys {
		delete(r.vnodeHashMap, hashKey)
	}
}

func (r *RingHash[T]) Get(key string, number int) ([]T, error) {
	// 期望获取的节点数量和真实节点数量做比较
	if number > r.totalNodes {
		number = r.totalNodes
	}
	// 无需从环上取
	if len(r.nodeMap) <= r.ringFloorLimit {
		if number > len(r.nodeMap) {
			number = len(r.nodeMap)
		}
		results := make([]T, 0, number)
		for _, node := range r.nodeMap {
			results = append(results, node)
		}
		return results, nil
	}
	// 判断环是否为空
	if len(r.vnodeSortedList) <= 0 {
		return []T{}, fmt.Errorf("ring is empty")
	}
	hashCode := r.hashFunc([]byte(key))
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
		node := r.vnodeHashMap[vnodeHashCode]
		keyStr := node.GetKey()
		if _, ok := seen[keyStr]; !ok {
			seen[keyStr] = struct{}{}
			results = append(results, node)
		}
	}
	return results, nil
}

func (r *RingHash[T]) GetSortedKeyCount() int {
	return len(r.vnodeSortedList)
}

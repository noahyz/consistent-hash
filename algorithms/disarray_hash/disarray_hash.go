package disarray_hash

import (
	"consistent-hash/models"
	"sort"
	"strconv"
)

const (
	shrinkOfHashCode int = 54
	hashSlotNumber   int = 1000
)

type pair struct {
	hash uint64
	idx  int
}

type DisarrayHash[T models.HashNode] struct {
	vnodeBaseNum   int    // 虚拟节点基数
	ringFloorLimit int    // 建环的下限
	slots          []int  // slots[i] 表示 hash 值 i 属于那个 node
	totalNodeNum   int    // 节点的数量
	nodes          []T    // 节点列表
	hashList       []pair // 排序后的节点哈希列表
}

func NewDisarrayHash[T models.HashNode](vnodeBaseNum, ringFloorLimit int,
	nodes []T, hashFunc func([]byte) uint64) *DisarrayHash[T] {
	r := &DisarrayHash[T]{
		vnodeBaseNum:   vnodeBaseNum,
		ringFloorLimit: ringFloorLimit,
		slots:          make([]int, 0),
		totalNodeNum:   0,
		nodes:          make([]T, 0),
		hashList:       make([]pair, 0),
	}
	r.nodes = append(r.nodes, nodes...)
	r.totalNodeNum = len(r.nodes)
	if r.totalNodeNum > r.ringFloorLimit {
		r.buildDisarrayHash(hashFunc)
	}
	return r
}

func (r *DisarrayHash[T]) buildDisarrayHash(hashFunc func([]byte) uint64) {
	// 生成所有虚拟节点哈希
	for idx, node := range r.nodes {
		vnodeCount := node.GetWeight() * r.vnodeBaseNum
		for j := 0; j < vnodeCount; j++ {
			vnodeKey := []byte(node.GetKey() + "_" + strconv.Itoa(j))
			hashCode := hashFunc(vnodeKey)
			r.hashList = append(r.hashList, pair{hash: hashCode, idx: idx})
		}
	}
	// 按照哈希值进行排序
	sort.Slice(r.hashList, func(i, j int) bool {
		return r.hashList[i].hash < r.hashList[j].hash
	})
	// 填充 slots: 对于每个槽位，找到顺时针的第一个vnode
	n := len(r.hashList)
	r.slots = make([]int, hashSlotNumber)
	for pos, item := range r.hashList {
		next := r.hashList[(pos+1)%n]
		start := item.hash >> shrinkOfHashCode
		end := next.hash >> shrinkOfHashCode
		if start >= uint64(hashSlotNumber) || end >= uint64(hashSlotNumber) {
			continue
		}
		// 区间 [start, end]
		if start < end {
			for j := start; j < end; j++ {
				r.slots[j] = item.idx
			}
		} else if start == end {
			r.slots[start] = item.idx
		} else {
			// wrap around
			for j := start; j < uint64(hashSlotNumber); j++ {
				r.slots[j] = item.idx
			}
			for j := uint64(0); j < end; j++ {
				r.slots[j] = item.idx
			}
		}
	}
}

func (r *DisarrayHash[T]) Get(key string, number int, hashFunc func([]byte) uint64) []T {
	// 判断节点列表是否为空
	if len(r.nodes) <= 0 {
		return []T{}
	}
	// 期望获取的节点数量和真实节点数量做比较
	if number > r.totalNodeNum {
		number = r.totalNodeNum
	}
	// 无需从离散哈希上获取
	if r.totalNodeNum <= r.ringFloorLimit {
		return r.nodes[:number]
	}
	hashCode := hashFunc([]byte(key)) >> shrinkOfHashCode
	results := make([]T, 0, number)
	seen := make(map[string]struct{})
	for i := 0; len(results) < number; i++ {
		idx := r.slots[(hashCode+uint64(i))%uint64(hashSlotNumber)]
		node := r.nodes[idx]
		keyStr := node.GetKey()
		if _, ok := seen[keyStr]; !ok {
			seen[keyStr] = struct{}{}
			results = append(results, node)
		}
	}
	return results
}

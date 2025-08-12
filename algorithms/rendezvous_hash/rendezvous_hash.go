package rendezvous_hash

import (
	"consistent-hash/models"
	"fmt"
)

type RendezvousHash[T models.HashNode] struct {
	nodeList []T                 // 节点列表
	nodeMap  map[string]struct{} // 节点映射，key为nodeKey
	hashFunc func([]byte) uint64 // 哈希函数
}

func NewRendezvousHash[T models.HashNode](nodeList []T, hashFunc func([]byte) uint64) *RendezvousHash[T] {
	obj := &RendezvousHash[T]{
		nodeList: make([]T, 0),
		nodeMap:  make(map[string]struct{}),
		hashFunc: hashFunc,
	}
	for _, node := range nodeList {
		obj.AddNode(node)
	}
	return obj
}

func (r *RendezvousHash[T]) computeWeight(key string, node T) uint64 {
	merged := key + "-" + node.GetKey()
	hashCode := r.hashFunc([]byte(merged))
	return hashCode * uint64(node.GetWeight())
}

func (r *RendezvousHash[T]) AddNode(node T) {
	nodeKey := node.GetKey()
	if _, ok := r.nodeMap[nodeKey]; ok {
		return
	}
	r.nodeMap[nodeKey] = struct{}{}
	r.nodeList = append(r.nodeList, node)
}

func (r *RendezvousHash[T]) RemoveNode(node T) {
	nodeKey := node.GetKey()
	if _, ok := r.nodeMap[nodeKey]; !ok {
		return
	}
	delete(r.nodeMap, nodeKey)
	idx := -1
	for i, n := range r.nodeList {
		if n.GetKey() == nodeKey {
			idx = i
			break
		}
	}
	if idx == -1 {
		return
	}
	r.nodeList = append(r.nodeList[:idx], r.nodeList[idx+1:]...)
}

func (r *RendezvousHash[T]) Get(key string) (T, error) {
	if len(r.nodeList) <= 0 {
		var zero T
		return zero, fmt.Errorf("nodeList is empty")
	}
	maxWeight := uint64(0)
	var selectNode T
	for _, node := range r.nodeList {
		weight := r.computeWeight(key, node)
		if weight > maxWeight {
			maxWeight = weight
			selectNode = node
		}
	}
	return selectNode, nil
}

func (r *RendezvousHash[T]) GetNodeCount() int {
	return len(r.nodeList)
}

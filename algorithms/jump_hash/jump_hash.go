package jump_hash

import (
	"consistent-hash/models"
	"fmt"
)

type JumpHash[T models.HashNode] struct {
	nodeList []T                 // 节点列表
	nodeMap  map[string]struct{} // 节点映射，key为nodeKey
	hashFunc func([]byte) uint64 // 哈希函数
}

func NewJumpHash[T models.HashNode](nodeList []T, hashFunc func([]byte) uint64) *JumpHash[T] {
	obj := &JumpHash[T]{
		nodeList: make([]T, 0),
		nodeMap:  make(map[string]struct{}),
		hashFunc: hashFunc,
	}
	for _, node := range nodeList {
		obj.AddNode(node)
	}
	return obj
}

func (r *JumpHash[T]) generateJumpConsistentHash(keyHash uint64, numBuckets int) int {
	if numBuckets <= 0 {
		return -1
	}
	var b int64 = -1
	var j int64 = 0
	for j < int64(numBuckets) {
		b = j
		keyHash = keyHash*2862933555777941757 + 1
		j = int64((float64(b+1) * float64(1<<31)) / float64((keyHash>>33)+1))
	}
	return int(b)
}

func (r *JumpHash[T]) AddNode(node T) {
	nodeKey := node.GetKey()
	if _, ok := r.nodeMap[nodeKey]; ok {
		return
	}
	r.nodeMap[nodeKey] = struct{}{}
	r.nodeList = append(r.nodeList, node)
}

func (r *JumpHash[T]) RemoveNode(node T) {
	nodeKey := node.GetKey()
	if _, ok := r.nodeMap[nodeKey]; !ok {
		return
	}
	delete(r.nodeMap, nodeKey)
	for i, n := range r.nodeList {
		if n.GetKey() == nodeKey {
			r.nodeList = append(r.nodeList[:i], r.nodeList[i+1:]...)
			return
		}
	}
}

func (r *JumpHash[T]) Get(key string) (T, error) {
	var zero T
	if len(r.nodeList) <= 0 {
		return zero, fmt.Errorf("nodeList is empty")
	}
	keyHash := r.hashFunc([]byte(key))
	idx := r.generateJumpConsistentHash(keyHash, len(r.nodeList))
	if idx < 0 || idx >= len(r.nodeList) {
		return zero, fmt.Errorf("generate jumpHash idx error")
	}
	return r.nodeList[idx], nil
}

func (r *JumpHash[T]) GetNodeCount() int {
	return len(r.nodeList)
}

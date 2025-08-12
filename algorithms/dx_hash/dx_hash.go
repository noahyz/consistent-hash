package dx_hash

import (
	"consistent-hash/models"
	"crypto/md5"
	"fmt"
)

type DxHash[T models.HashNode] struct {
	nodeKeyList    []string     // 节点列表
	nodeMap        map[string]T // 节点映射，key为nodeKey
	nsTable        []int        // NSArray查找表
	nsSize         int          // 当前NSArray的大小
	nodeCount      int          // 当前节点数量
	availableStack []int        // 栈，可用位置，用于优化节点添加/删除
}

func NewDxHash[T models.HashNode](nodeList []T, initSize int) *DxHash[T] {
	// 初始大小确保是2的幂
	size := 1
	for size < initSize {
		size <<= 1
	}
	dx := &DxHash[T]{
		nodeKeyList:    make([]string, size),
		nodeMap:        make(map[string]T),
		nsTable:        make([]int, size),
		nsSize:         size,
		nodeCount:      0,
		availableStack: make([]int, 0, size),
	}
	// 初始化可用位置栈
	for i := 0; i < size; i++ {
		dx.availableStack = append(dx.availableStack, i)
	}
	// 添加节点
	for _, node := range nodeList {
		dx.AddNode(node)
	}
	return dx
}

func (r *DxHash[T]) hash(key string, seed int) uint64 {
	h := md5.Sum([]byte(key + string(rune(seed))))
	// 确保结果为正数
	result := (uint64(h[0]) << 56) | (uint64(h[1]) << 48) | (uint64(h[2]) << 40) | (uint64(h[3]) << 32) |
		(uint64(h[4]) << 24) | (uint64(h[5]) << 16) | (uint64(h[6]) << 8) | uint64(h[7])
	return result & 0x7fffffffffffffff // 清除符号位确保为正数
}

func (r *DxHash[T]) resize() {
	newSize := r.nsSize * 2
	newTable := make([]int, newSize)
	newNodeKeyList := make([]string, newSize)
	// 复制旧数据
	copy(newTable, r.nsTable)
	copy(newNodeKeyList, r.nodeKeyList)
	// 初始化新的位置
	for i := r.nsSize; i < newSize; i++ {
		newTable[i] = -1
		// 将新位置添加到可用栈
		r.availableStack = append(r.availableStack, i)
	}
	r.nsTable = newTable
	r.nodeKeyList = newNodeKeyList
	r.nsSize = newSize
}

func (r *DxHash[T]) populateTable() {
	// 初始化表
	for i := range r.nsTable {
		r.nsTable[i] = -1
	}
	// 为每个节点在表中分配位置
	for i, nk := range r.nodeKeyList {
		// 使用节点索引作为种子计算位置
		pos := int(r.hash(nk, 0)) % r.nsSize
		r.nsTable[pos] = i
	}
}

func (r *DxHash[T]) AddNode(node T) {
	nodeKey := node.GetKey()
	if _, ok := r.nodeMap[nodeKey]; ok {
		return
	}
	// 检查是否需要扩容，负载因子超过0.5时启动扩容
	if r.nodeCount >= r.nsSize/2 {
		r.resize()
	}
	// 从栈中获取一个可用位置
	if len(r.availableStack) == 0 {
		return
	}
	// 从栈顶取出一个可用位置
	index := r.availableStack[len(r.availableStack)-1]
	r.availableStack = r.availableStack[:len(r.availableStack)-1]
	// 在获取的位置添加节点
	r.nodeKeyList[index] = nodeKey
	r.nodeMap[nodeKey] = node
	// 在NSArray数组中为节点分配位置
	pos := int(r.hash(nodeKey, 0)) % r.nsSize
	r.nsTable[pos] = index
	r.nodeCount++
}

func (r *DxHash[T]) RemoveNode(node T) {
	nodeKey := node.GetKey()
	if _, ok := r.nodeMap[nodeKey]; !ok {
		return
	}
	delete(r.nodeMap, nodeKey)
	index := -1
	for i := 0; i < r.nodeCount; i++ {
		if r.nodeKeyList[i] == nodeKey {
			index = i
			break
		}
	}
	if index == -1 {
		return
	}
	// 从NSArray中移除节点的位置
	pos := int(r.hash(nodeKey, 0)) % r.nsSize
	r.nsTable[pos] = -1
	// 将位置放回可用栈
	r.availableStack = append(r.availableStack, index)
	// 减少节点技术
	r.nodeCount--
}

func (r *DxHash[T]) Get(key string) (T, error) {
	var dummy T
	if r.nodeCount <= 0 {
		return dummy, fmt.Errorf("nodeCount less 0")
	}
	// 不预先生成伪随机序列，而是按需计算
	// 最大重试次数为8*当前节点数，按照论文建议
	maxRetries := 8 * r.nodeCount
	for i := 0; i < maxRetries; i++ {
		pos := int(r.hash(key, i)) % r.nsSize
		if r.nsTable[pos] != -1 && r.nsTable[pos] < len(r.nodeKeyList) {
			// 检查该位置是否有效
			nodeIndex := r.nsTable[pos]
			if nodeIndex >= 0 && nodeIndex < len(r.nodeKeyList) && r.nodeKeyList[nodeIndex] != "" {
				nk := r.nodeKeyList[nodeIndex]
				if node, ok := r.nodeMap[nk]; ok {
					return node, nil
				}
			}
		}
	}
	// 如果都没找到，返回第一个有效节点
	for i := 0; i < r.nodeCount; i++ {
		nk := r.nodeKeyList[i]
		if nk != "" {
			if node, ok := r.nodeMap[nk]; ok {
				return node, nil
			}
		}
	}
	return dummy, fmt.Errorf("not found available node")
}

func (r *DxHash[T]) GetNodeCount() int {
	return r.nodeCount
}

func (r *DxHash[T]) GetTableSize() int {
	return r.nsSize
}

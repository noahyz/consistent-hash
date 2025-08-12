package anchor_hash

import (
	"consistent-hash/utils"
	"fmt"
)

type AnchorHash struct {
	A        []uint32
	K        []uint32
	W        []uint32
	L        []uint32
	R        []uint32
	N        uint32
	maxSize  uint32              // 最大桶容量
	nodeList []string            // 节点数组
	nodeMap  map[string]uint32   // 节点key到桶的映射
	hashFunc func([]byte) uint64 // 哈希函数
}

func NewAnchorHash(nodeList []string, size int, hashFunc func([]byte) uint64) *AnchorHash {
	r := &AnchorHash{
		A:        make([]uint32, size),
		K:        make([]uint32, size),
		W:        make([]uint32, size),
		L:        make([]uint32, size),
		R:        make([]uint32, 0, size),
		N:        0,
		maxSize:  uint32(size),
		nodeList: make([]string, size),
		nodeMap:  make(map[string]uint32),
		hashFunc: hashFunc,
	}
	for i := uint32(0); i < uint32(size); i++ {
		r.K[i] = i
		r.W[i] = i
		r.L[i] = i
		r.A[i] = uint32(size) // 初始化所有桶为移除状态
	}
	for i := size - 1; i >= 0; i-- {
		r.R = append(r.R, uint32(i))
	}
	// 添加节点
	for _, nk := range nodeList {
		r.AddBucket(nk)
	}
	return r
}

func (r *AnchorHash) Get(key string) (string, error) {
	if r.N <= 0 {
		return "", fmt.Errorf("no exist node")
	}
	hashKey := r.hashFunc([]byte(key))
	ha, hb, hc, hd := utils.FleaInit(hashKey)
	b := utils.FastMod(uint64(hd), uint64(r.maxSize))
	iterations := uint32(0)
	maxIterations := r.maxSize * 2 // 防止无限循环
	for r.A[b] > 0 && iterations < maxIterations {
		ha, hb, hc, hd = utils.FleaRound(ha, hb, hc, hd)
		h := utils.FastMod(uint64(hd), uint64(r.A[b]))
		steps := uint32(0)
		maxSteps := r.maxSize // 防止无限循环
		for r.A[h] >= r.A[b] && steps < maxSteps {
			h = r.K[h]
			steps++
			if h < 0 || h >= uint32(len(r.K)) {
				// 防止数组越界
				return r.nodeList[b], nil
			}
		}
		if steps >= maxSteps {
			// 内存循环超过阈值，返回当前桶
			return r.nodeList[b], nil
		}
		b = h
		iterations++
	}
	if iterations >= maxIterations {
		// 外部循环超过阈值，返回默认节点
		if r.N > 0 {
			return r.nodeList[r.W[0]], nil
		}
		return "", fmt.Errorf("no exist available node")
	}
	// 确保返回的索引有效
	if b < uint32(len(r.nodeList)) {
		return r.nodeList[b], nil
	}
	return "", fmt.Errorf("can't select")
}

func (r *AnchorHash) GetPath(key uint64, pathBuffer []uint32) []uint32 {
	A, K := r.A, r.K
	ha, hb, hc, hd := utils.FleaInit(key)
	b := utils.FastMod(uint64(hd), uint64(len(A)))
	pathBuffer = append(pathBuffer, b)
	for A[b] > 0 {
		ha, hb, hc, hd = utils.FleaRound(ha, hb, hc, hd)
		h := utils.FastMod(uint64(hd), uint64(A[b]))
		pathBuffer = append(pathBuffer, h)
		for A[h] >= A[b] {
			h = K[h]
			pathBuffer = append(pathBuffer, h)
		}
		b = h
	}
	return pathBuffer
}

func (r *AnchorHash) AddBucket(nodeKey string) {
	if _, ok := r.nodeMap[nodeKey]; ok {
		return
	}
	// 达到最大节点数
	if r.N >= r.maxSize {
		return
	}
	// 从移除栈中取出一个桶
	b := r.R[len(r.R)-1]
	r.R = r.R[:len(r.R)-1]
	// 添加节点到工作集
	r.A[b] = 0
	r.L[r.W[r.N]] = r.N
	r.W[r.L[b]], r.K[b] = b, b
	r.nodeList[b] = nodeKey
	r.nodeMap[nodeKey] = b
	r.N++
}

func (r *AnchorHash) RemoveBucket(nodeKey string) {
	// 检查节点是否存在
	b, ok := r.nodeMap[nodeKey]
	if !ok {
		return
	}
	if r.A[b] != 0 {
		return
	}
	// 将桶放回移除栈
	r.R = append(r.R, b)
	r.N--
	r.A[b] = r.N
	r.W[r.L[b]], r.K[b] = r.W[r.N], r.W[r.N]
	r.L[r.W[r.N]] = r.L[b]
	// 从节点映射中删除
	delete(r.nodeMap, nodeKey)
	r.nodeList[b] = ""
}

func (r *AnchorHash) Print() {
	fmt.Printf("\nA: ")
	for _, item := range r.A {
		fmt.Printf("%v ", item)
	}
	fmt.Printf("\nK: ")
	for _, item := range r.K {
		fmt.Printf("%v ", item)
	}
	fmt.Printf("\nW: ")
	for _, item := range r.W {
		fmt.Printf("%v ", item)
	}
	fmt.Printf("\nL: ")
	for _, item := range r.L {
		fmt.Printf("%v ", item)
	}
	fmt.Printf("\nR: ")
	for _, item := range r.R {
		fmt.Printf("%v ", item)
	}
	fmt.Println()
}

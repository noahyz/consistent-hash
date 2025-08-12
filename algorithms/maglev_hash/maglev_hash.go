package maglev_hash

import (
	"crypto/md5"
	"fmt"
)

// NodePreference 节点的偏好信息
type NodePreference struct {
	offset  int // 偏移量
	skip    int // 跳跃步长
	nextIdx int // 下一个排列序号
}

type MaglevHash struct {
	nodeList       []string            // 节点列表
	nodeMap        map[string]struct{} // 节点映射，key为nodeKey
	preferenceList []*NodePreference   // 节点偏好信息
	tableSize      int                 // 查找表大小
	lookupTable    []string            // 查找表
}

func NewMaglevHash(nodeList []string, tableSize int) *MaglevHash {
	obj := &MaglevHash{
		nodeList:       make([]string, 0),
		nodeMap:        make(map[string]struct{}),
		preferenceList: make([]*NodePreference, 0),
		tableSize:      tableSize,
		lookupTable:    make([]string, tableSize),
	}
	for _, nk := range nodeList {
		obj.AddNode(nk)
	}
	return obj
}

func (r *MaglevHash) hash(key string, seed int) uint64 {
	h := md5.Sum([]byte(key + string(rune(seed))))
	// 确保结果为正数
	result := (uint64(h[0]) << 56) | (uint64(h[1]) << 48) | (uint64(h[2]) << 40) | (uint64(h[3]) << 32) |
		(uint64(h[4]) << 24) | (uint64(h[5]) << 16) | (uint64(h[6]) << 8) | uint64(h[7])
	return result & 0x7fffffffffffffff // 清除符号位确保为正数
}

func (r *MaglevHash) calculatePreference(nodeKey string) *NodePreference {
	offset := int(r.hash(nodeKey, 0)) % r.tableSize
	skip := int(r.hash(nodeKey, 1))%(r.tableSize-1) + 1
	return &NodePreference{
		offset:  offset,
		skip:    skip,
		nextIdx: 0,
	}
}

func (r *MaglevHash) getPermutationItem(preference *NodePreference, index int) int {
	return (preference.offset + index*preference.skip) % r.tableSize
}

func (r *MaglevHash) populateLookupTable() {
	if len(r.nodeList) <= 0 {
		for idx := range r.lookupTable {
			r.lookupTable[idx] = ""
		}
		return
	}
	// 初始化节点偏好信息
	r.preferenceList = make([]*NodePreference, len(r.nodeList))
	for idx, nodeKey := range r.nodeList {
		r.preferenceList[idx] = r.calculatePreference(nodeKey)
	}
	// 初始化查找表
	for idx := range r.lookupTable {
		r.lookupTable[idx] = ""
	}
	// 重置每个节点的下一个填充位置
	for idx := range r.preferenceList {
		r.preferenceList[idx].nextIdx = 0
	}
	// 按轮次填充查找表
	filledCount := 0
	round := 0
	maxRounds := r.tableSize * 10 // 设置阈值
	for filledCount < r.tableSize && round < maxRounds {
		for i := range r.nodeList {
			if filledCount >= r.tableSize {
				break
			}
			// 找到下一个可填充的位置
			for r.preferenceList[i].nextIdx < r.tableSize {
				pos := r.getPermutationItem(r.preferenceList[i], r.preferenceList[i].nextIdx)
				if r.lookupTable[pos] == "" {
					break
				}
				r.preferenceList[i].nextIdx++
			}
			// 如果还有可填充的位置，则填充
			if r.preferenceList[i].nextIdx < r.tableSize {
				pos := r.getPermutationItem(r.preferenceList[i], r.preferenceList[i].nextIdx)
				r.lookupTable[pos] = r.nodeList[i]
				r.preferenceList[i].nextIdx++
				filledCount++
			}
		}
		round++
	}
}

func (r *MaglevHash) AddNode(nodeKey string) {
	if _, ok := r.nodeMap[nodeKey]; ok {
		return
	}
	r.nodeMap[nodeKey] = struct{}{}
	r.nodeList = append(r.nodeList, nodeKey)
	r.populateLookupTable()
}

func (r *MaglevHash) RemoveNode(nodeKey string) {
	if _, ok := r.nodeMap[nodeKey]; !ok {
		return
	}
	delete(r.nodeMap, nodeKey)

	idx := -1
	for i, nk := range r.nodeList {
		if nk == nodeKey {
			idx = i
			break
		}
	}
	if idx == -1 {
		return
	}
	r.nodeList = append(r.nodeList[:idx], r.nodeList[idx+1:]...)
	r.populateLookupTable()
}

func (r *MaglevHash) Get(key string) (string, error) {
	if len(r.lookupTable) <= 0 {
		return "", fmt.Errorf("lookupTable is empty")
	}
	keyHash := int(r.hash(key, 0)) % r.tableSize
	return r.lookupTable[keyHash], nil
}

func (r *MaglevHash) GetNodeCount() int {
	return len(r.nodeList)
}

func (r *MaglevHash) GetLookupTableSize() int {
	return len(r.lookupTable)
}

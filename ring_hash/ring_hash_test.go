package ring_hash

import (
	"consistent-hash/utils"
	"strconv"
	"testing"
)

type NormalHashNode struct {
	key    string
	weight int
}

func NewNormalHashNode(key string, weight int) *NormalHashNode {
	return &NormalHashNode{
		key:    key,
		weight: weight,
	}
}

func (r *NormalHashNode) GetKey() string {
	return r.key
}

func (r *NormalHashNode) GetWeight() int {
	return r.weight
}

func TestRingHash_NormalFunction(t *testing.T) {
	vnodeBaseNum := 10
	ringFloorLimit := 1
	nodes := []*NormalHashNode{
		NewNormalHashNode("node_1", 3),
		NewNormalHashNode("node_2", 5),
		NewNormalHashNode("node_3", 2),
	}
	hashFunc := utils.GetHashCode
	obj := NewRingHash(vnodeBaseNum, ringFloorLimit, nodes, hashFunc)

	key := "photoId_1"
	results := obj.Get(key, 1, hashFunc)
	if len(results) <= 0 {
		t.Errorf("hash ring get empty result")
		return
	}
	t.Logf("result: %v", results[0])
}

func TestRingHash_AllocateRatio(t *testing.T) {
	vnodeBaseNum := 1000
	ringFloorLimit := 1
	nodes := []*NormalHashNode{
		NewNormalHashNode("node_1", 3),
		NewNormalHashNode("node_2", 5),
		NewNormalHashNode("node_3", 2),
	}
	hashFunc := utils.GetHashCode
	obj := NewRingHash(vnodeBaseNum, ringFloorLimit, nodes, hashFunc)

	// 测试节点的权重
	nodeResultMap := map[string]uint64{
		"node_1": 0,
		"node_2": 0,
		"node_3": 0,
	}
	for i := 0; i < 100000; i++ {
		key := "key_" + strconv.Itoa(i)
		results := obj.Get(key, 1, hashFunc)
		if len(results) <= 0 {
			t.Errorf("hash ring get empty result")
		} else {
			nodeResultMap[results[0].GetKey()]++
		}
	}
	t.Logf("node_1: %v", nodeResultMap["node_1"])
	t.Logf("node_2: %v", nodeResultMap["node_2"])
	t.Logf("node_3: %v", nodeResultMap["node_3"])
}

func TestRingHash_UpdateNode(t *testing.T) {
	vnodeBaseNum := 1000
	ringFloorLimit := 1
	nodes := []*NormalHashNode{
		NewNormalHashNode("node_1", 3),
		NewNormalHashNode("node_2", 6),
		NewNormalHashNode("node_3", 1),
	}
	hashFunc := utils.GetHashCode
	obj := NewRingHash(vnodeBaseNum, ringFloorLimit, nodes, hashFunc)

	// 第一次：文件与节点的对应关系
	fileNodeMap1 := make(map[string]string)
	for i := 0; i < 100000; i++ {
		key := "key_" + strconv.Itoa(i)
		results := obj.Get(key, 1, hashFunc)
		if len(results) <= 0 {
			t.Fatal("hash ring get empty result")
			return
		}
		fileNodeMap1[key] = results[0].GetKey()
	}

	// 第二次：文件与节点的对应关系
	// 更新节点的权重
	nodes2 := []*NormalHashNode{
		NewNormalHashNode("node_1", 4),
		NewNormalHashNode("node_2", 5),
		NewNormalHashNode("node_3", 1),
	}
	vnodeBaseNum2 := 1000
	obj2 := NewRingHash(vnodeBaseNum2, ringFloorLimit, nodes2, hashFunc)
	fileNodeMap2 := make(map[string]string)
	for i := 0; i < 100000; i++ {
		key := "key_" + strconv.Itoa(i)
		results := obj2.Get(key, 1, hashFunc)
		if len(results) <= 0 {
			t.Fatal("hash ring get empty result")
			return
		}
		fileNodeMap2[key] = results[0].GetKey()
	}
	// 文件变化
	adjustNum := 0
	for file1, node1 := range fileNodeMap1 {
		node2 := fileNodeMap2[file1]
		if node1 != node2 {
			adjustNum++
		}
	}
	t.Logf("adjust num: %v\n", adjustNum)

	// 第三次：文件与节点的对应关系
	// 更新节点的权重
	nodes3 := []*NormalHashNode{
		NewNormalHashNode("node_1", 3),
		NewNormalHashNode("node_2", 6),
		NewNormalHashNode("node_3", 1),
	}
	vnodeBaseNum3 := 1000
	obj3 := NewRingHash(vnodeBaseNum3, ringFloorLimit, nodes3, hashFunc)
	fileNodeMap3 := make(map[string]string)
	for i := 0; i < 100000; i++ {
		key := "key_" + strconv.Itoa(i)
		results := obj3.Get(key, 1, hashFunc)
		if len(results) <= 0 {
			t.Fatal("hash ring get empty result")
			return
		}
		fileNodeMap3[key] = results[0].GetKey()
	}
	// 文件变化
	adjustNum3 := 0
	for file1, node1 := range fileNodeMap1 {
		node3 := fileNodeMap3[file1]
		if node1 != node3 {
			adjustNum3++
		}
	}
	t.Logf("adjust num: %v\n", adjustNum3)
}

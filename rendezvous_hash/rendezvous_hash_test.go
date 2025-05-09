package rendezvous_hash

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

func TestRendezvousHash_NormalFunction(t *testing.T) {
	nodes := []*NormalHashNode{
		NewNormalHashNode("node_1", 3),
		NewNormalHashNode("node_2", 5),
		NewNormalHashNode("node_3", 2),
	}
	slotNum := 1000
	hashFunc := utils.GetHashCode
	obj := NewRendezvousHash(slotNum, nodes, hashFunc)

	key := "photoId_1"
	results := obj.Get(key, 1, hashFunc)
	if len(results) <= 0 {
		t.Errorf("hash ring get empty result")
		return
	}
	t.Logf("result: %v", results[0])
}

func TestRendezvousHash_AllocateRatio(t *testing.T) {
	nodes := []*NormalHashNode{
		NewNormalHashNode("node_1", 3),
		NewNormalHashNode("node_2", 6),
		NewNormalHashNode("node_3", 1),
	}
	slotNum := 10000
	hashFunc := utils.GetHashCode
	obj := NewRendezvousHash(slotNum, nodes, hashFunc)

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

func TestRendezvousHash_AddNode(t *testing.T) {
	// 构建哈希
	nodes := []*NormalHashNode{
		NewNormalHashNode("node_1", 3),
		NewNormalHashNode("node_2", 6),
		NewNormalHashNode("node_3", 1),
	}
	slotNum := 1000
	hashFunc := utils.GetHashCode
	obj := NewRendezvousHash(slotNum, nodes, hashFunc)

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
	// 新增节点
	obj.AddNode(NewNormalHashNode("node_4", 2))
	// 第二次：文件与节点的对应关系
	fileNodeMap2 := make(map[string]string)
	for i := 0; i < 100000; i++ {
		key := "key_" + strconv.Itoa(i)
		results := obj.Get(key, 1, hashFunc)
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
}

func TestRendezvousHash_DeleteNode(t *testing.T) {
	// 构建哈希
	nodes := []*NormalHashNode{
		NewNormalHashNode("node_1", 3),
		NewNormalHashNode("node_2", 6),
		NewNormalHashNode("node_3", 1),
	}
	slotNum := 1000
	hashFunc := utils.GetHashCode
	obj := NewRendezvousHash(slotNum, nodes, hashFunc)

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
	// 剔除节点
	obj.RemoveNode(NewNormalHashNode("node_1", 3))
	// 第二次：文件与节点的对应关系
	fileNodeMap2 := make(map[string]string)
	for i := 0; i < 100000; i++ {
		key := "key_" + strconv.Itoa(i)
		results := obj.Get(key, 1, hashFunc)
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
}

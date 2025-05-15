package rendezvous_hash_v2

import (
	"consistent-hash/utils"
	"fmt"
	"slices"
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

func TestRendezvousHashV2_NormalFunction(t *testing.T) {
	nodes := []*NormalHashNode{
		NewNormalHashNode("node_1", 30000),
		NewNormalHashNode("node_2", 50000),
		NewNormalHashNode("node_3", 20000),
	}
	hashFunc := utils.GetHashCode
	obj := NewRendezvousHashV2(nodes, hashFunc)

	key := "photoId_1"
	results := obj.Get(key, hashFunc)
	if len(results) <= 0 {
		t.Errorf("hash ring get empty result")
		return
	}
	t.Logf("result: %v", results[0])
}

func TestRendezvousHashV2_AllocateRatio(t *testing.T) {
	// 权重总和在 10w 以内
	nodes1 := []*NormalHashNode{
		NewNormalHashNode("node_1", 20000),
		NewNormalHashNode("node_2", 50000),
		NewNormalHashNode("node_3", 10000),
	}
	obj1 := NewRendezvousHashV2(nodes1, utils.GetHashCode)
	printAllocateResult(obj1)

	// 权重总和等于 10w
	nodes2 := []*NormalHashNode{
		NewNormalHashNode("node_1", 30000),
		NewNormalHashNode("node_2", 60000),
		NewNormalHashNode("node_3", 10000),
	}
	obj2 := NewRendezvousHashV2(nodes2, utils.GetHashCode)
	printAllocateResult(obj2)

	// 权重总和大于 10w
	nodes3 := []*NormalHashNode{
		NewNormalHashNode("node_1", 50000),
		NewNormalHashNode("node_2", 60000),
		NewNormalHashNode("node_3", 30000),
	}
	obj3 := NewRendezvousHashV2(nodes3, utils.GetHashCode)
	printAllocateResult(obj3)

	// 权重总和大于 20w
	nodes4 := []*NormalHashNode{
		NewNormalHashNode("node_1", 70000),
		NewNormalHashNode("node_2", 60000),
		NewNormalHashNode("node_3", 80000),
	}
	obj4 := NewRendezvousHashV2(nodes4, utils.GetHashCode)
	printAllocateResult(obj4)

	// 权重相差特别大
	nodes5 := []*NormalHashNode{
		NewNormalHashNode("node_1", 70000),
		NewNormalHashNode("node_2", 60000),
		NewNormalHashNode("node_3", 10),
	}
	obj5 := NewRendezvousHashV2(nodes5, utils.GetHashCode)
	printAllocateResult(obj5)
}

func TestRendezvousHashV2_UpdateNode(t *testing.T) {
	// 构建哈希
	nodes := []*NormalHashNode{
		NewNormalHashNode("node_1", 30000),
		NewNormalHashNode("node_2", 40000),
		NewNormalHashNode("node_3", 20000),
	}
	hashFunc := utils.GetHashCode
	obj := NewRendezvousHashV2(nodes, hashFunc)

	// 第一次：文件与节点的对应关系
	obj.debug()
	slotTables1 := obj.getSlotTable()
	fileNodeMap1 := printAllocateResult(obj)

	// 第二次：文件与节点的对应关系。更新节点的权重
	obj.UpdateNode(&NormalHashNode{"node_1", 20000})
	obj.debug()
	slotTables2 := obj.getSlotTable()
	fileNodeMap2 := printAllocateResult(obj)

	// 槽位变化、文件变化
	printFileNodeDiff(fileNodeMap1, fileNodeMap2)
	printSlotDiff(slotTables1, slotTables2)

	// 第三次: 文件与节点的对应关系
	obj.UpdateNode(&NormalHashNode{"node_1", 30000})
	obj.debug()
	slotTables3 := obj.getSlotTable()
	fileNodeMap3 := printAllocateResult(obj)

	// 槽位变化、文件变化
	printFileNodeDiff(fileNodeMap1, fileNodeMap3)
	printSlotDiff(slotTables1, slotTables3)

}

func TestRendezvousHashV2_AddNode(t *testing.T) {
	nodes := []*NormalHashNode{
		NewNormalHashNode("node_1", 30000),
		NewNormalHashNode("node_2", 60000),
		NewNormalHashNode("node_3", 10000),
	}
	hashFunc := utils.GetHashCode
	obj := NewRendezvousHashV2(nodes, hashFunc)

	// 第一次：文件与节点的对应关系
	slotTable1 := obj.getSlotTable()
	fileNodeMap1 := printAllocateResult(obj)

	// 新增节点
	obj.AddNode(NewNormalHashNode("node_4", 20000))
	// 第二次：文件与节点的对应关系
	slotTable2 := obj.getSlotTable()
	fileNodeMap2 := printAllocateResult(obj)

	// 文件变化、槽位变化
	printFileNodeDiff(fileNodeMap1, fileNodeMap2)
	printSlotDiff(slotTable1, slotTable2)
}

func TestRendezvousHashV2_RemoveNode(t *testing.T) {
	nodes := []*NormalHashNode{
		NewNormalHashNode("node_1", 30000),
		NewNormalHashNode("node_2", 60000),
		NewNormalHashNode("node_3", 10000),
	}
	hashFunc := utils.GetHashCode
	obj := NewRendezvousHashV2(nodes, hashFunc)

	// 第一次：文件与节点的对应关系
	slotTable1 := obj.getSlotTable()
	fileNodeMap1 := printAllocateResult(obj)

	// 新增节点
	obj.RemoveNode("node_1")
	// 第二次：文件与节点的对应关系
	slotTable2 := obj.getSlotTable()
	fileNodeMap2 := printAllocateResult(obj)

	// 文件变化、槽位变化
	printFileNodeDiff(fileNodeMap1, fileNodeMap2)
	printSlotDiff(slotTable1, slotTable2)
}

func printAllocateResult(obj *RendezvousHashV2[*NormalHashNode]) map[string]uint64 {
	hashFunc := utils.GetHashCode
	// 测试节点的权重
	nodeResultMap := make(map[string]uint64)
	for i := 0; i < 10000000; i++ {
		key := "key_" + strconv.Itoa(i)
		results := obj.Get(key, hashFunc)
		if len(results) <= 0 {
			nodeResultMap["outsource"]++
		} else {
			for _, nk := range results {
				nodeResultMap[nk.GetKey()]++
			}
		}
	}
	for k, v := range nodeResultMap {
		fmt.Printf("node: %v, count: %v\n", k, v)
	}
	fmt.Printf("\n\n")
	return nodeResultMap
}

func printSlotDiff(slotTable1, slotTable2 map[int][]string) {
	adjustNum := 0
	for slot1, nodes1 := range slotTable1 {
		nodes2 := slotTable2[slot1]
		if !slices.Equal(nodes1, nodes2) {
			adjustNum++
		}
	}
	fmt.Printf("slotDiff adjust num: %v\n", adjustNum)
}

func printFileNodeDiff(fileNodeMap1, fileNodeMap2 map[string]uint64) {
	adjustNum := 0
	for file1, node1 := range fileNodeMap1 {
		node2 := fileNodeMap2[file1]
		if node1 != node2 {
			adjustNum++
		}
	}
	fmt.Printf("fileNodeDiff adjust num: %v\n", adjustNum)
}

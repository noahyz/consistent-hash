package rendezvous_hash_v2

import (
	"consistent-hash/models"
	"consistent-hash/utils"
	"fmt"
	"sort"
	"strconv"
	"testing"
)

func TestRendezvousHashV2_NormalFunction(t *testing.T) {
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 10000, true),
		models.NewNormalHashNode("node_2", 50000, true),
		models.NewNormalHashNode("node_3", 10000, true),
	}
	hashFunc := utils.GetHashCode
	obj := NewRendezvousHashV2(nodes, hashFunc)
	obj.debug()

	key := "photoId_1"
	results := obj.Get(key, hashFunc)
	if len(results) <= 0 {
		t.Errorf("hash ring get empty result")
		return
	}
	t.Logf("result: %v", results[0])
}

func TestRendezvousHashV2_Rebuild(t *testing.T) {
	// 相同的参数，重建之后检验是否有变化
	nodes1 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 20000, true),
		models.NewNormalHashNode("node_2", 50000, true),
		models.NewNormalHashNode("node_3", 10000, true),
	}
	obj1 := NewRendezvousHashV2(nodes1, utils.GetHashCode)
	obj1.debug()
	slotTables1 := obj1.getSlotTable()
	fileNodeMap1 := printAllocateResult(obj1)

	nodes2 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 20000, true),
		models.NewNormalHashNode("node_2", 50000, true),
		models.NewNormalHashNode("node_3", 10000, true),
	}
	obj2 := NewRendezvousHashV2(nodes2, utils.GetHashCode)
	obj2.debug()
	slotTables2 := obj2.getSlotTable()
	fileNodeMap2 := printAllocateResult(obj2)

	printSlotDiff(slotTables1, slotTables2)
	printFileNodeDiff(fileNodeMap1, fileNodeMap2)
}

func TestRendezvousHashV2_Rebuild2(t *testing.T) {
	// 相同的参数，重建之后检验是否有变化
	nodes1 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 20000, true),
		models.NewNormalHashNode("node_2", 50000, true),
		models.NewNormalHashNode("node_3", 30000, true),
		models.NewNormalHashNode("node_4", 80000, true),
	}
	obj1 := NewRendezvousHashV2(nodes1, utils.GetHashCode)
	obj1.debug()
	slotTables1 := obj1.getSlotTable()
	fileNodeMap1 := printAllocateResult(obj1)

	nodes2 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_2", 50000, true),
		models.NewNormalHashNode("node_3", 30000, true),
		models.NewNormalHashNode("node_4", 80000, true),
	}
	obj2 := NewRendezvousHashV2(nodes2, utils.GetHashCode)
	obj2.debug()
	slotTables2 := obj2.getSlotTable()
	fileNodeMap2 := printAllocateResult(obj2)

	printSlotDiff(slotTables1, slotTables2)
	printFileNodeDiff(fileNodeMap1, fileNodeMap2)
}

func TestRendezvousHashV2_Rebuild3(t *testing.T) {
	// 相同的参数，重建之后检验是否有变化
	nodes1 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_2", 50000, true),
		models.NewNormalHashNode("node_3", 30000, true),
		models.NewNormalHashNode("node_4", 80000, true),
	}
	obj1 := NewRendezvousHashV2(nodes1, utils.GetHashCode)

	obj1.UpdateNode(models.NewNormalHashNode("node_2", 70000, true))
	obj1.UpdateNode(models.NewNormalHashNode("node_2", 10000, true))
	obj1.UpdateNode(models.NewNormalHashNode("node_3", 10000, true))
	obj1.UpdateNode(models.NewNormalHashNode("node_4", 10000, true))
	obj1.UpdateNode(models.NewNormalHashNode("node_2", 50000, true))
	obj1.UpdateNode(models.NewNormalHashNode("node_3", 30000, true))
	obj1.UpdateNode(models.NewNormalHashNode("node_4", 80000, true))

	obj1.debug()
	slotTables1 := obj1.getSlotTable()
	fileNodeMap1 := printAllocateResult(obj1)

	nodes2 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_2", 50000, true),
		models.NewNormalHashNode("node_3", 30000, true),
		models.NewNormalHashNode("node_4", 80000, true),
	}
	obj2 := NewRendezvousHashV2(nodes2, utils.GetHashCode)
	obj2.debug()
	slotTables2 := obj2.getSlotTable()
	fileNodeMap2 := printAllocateResult(obj2)

	printSlotDiff(slotTables1, slotTables2)
	printFileNodeDiff(fileNodeMap1, fileNodeMap2)
}

func TestRendezvousHashV2_AllocateRatio(t *testing.T) {
	// 权重总和在 10w 以内
	nodes1 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 20000, true),
		models.NewNormalHashNode("node_2", 50000, true),
		models.NewNormalHashNode("node_3", 10000, true),
	}
	obj1 := NewRendezvousHashV2(nodes1, utils.GetHashCode)
	printAllocateResult(obj1)

	// 权重总和等于 10w
	nodes2 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 30000, true),
		models.NewNormalHashNode("node_2", 60000, true),
		models.NewNormalHashNode("node_3", 10000, true),
	}
	obj2 := NewRendezvousHashV2(nodes2, utils.GetHashCode)
	printAllocateResult(obj2)

	// 权重总和大于 10w
	nodes3 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 50000, true),
		models.NewNormalHashNode("node_2", 60000, true),
		models.NewNormalHashNode("node_3", 30000, true),
	}
	obj3 := NewRendezvousHashV2(nodes3, utils.GetHashCode)
	printAllocateResult(obj3)

	// 权重总和大于 20w
	nodes4 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 70000, true),
		models.NewNormalHashNode("node_2", 60000, true),
		models.NewNormalHashNode("node_3", 80000, true),
	}
	obj4 := NewRendezvousHashV2(nodes4, utils.GetHashCode)
	printAllocateResult(obj4)

	// 权重相差特别大
	nodes5 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 70000, true),
		models.NewNormalHashNode("node_2", 60000, true),
		models.NewNormalHashNode("node_3", 10, true),
	}
	obj5 := NewRendezvousHashV2(nodes5, utils.GetHashCode)
	printAllocateResult(obj5)
}

func TestRendezvousHashV2_UpdateNode(t *testing.T) {
	// 构建哈希
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 30000, true),
		models.NewNormalHashNode("node_2", 40000, true),
		models.NewNormalHashNode("node_3", 20000, true),
	}
	hashFunc := utils.GetHashCode
	obj := NewRendezvousHashV2(nodes, hashFunc)

	// 第一次：文件与节点的对应关系
	obj.debug()
	slotTables1 := obj.getSlotTable()
	fileNodeMap1 := printAllocateResult(obj)

	// 第二次：文件与节点的对应关系。更新节点的权重
	obj.UpdateNode(models.NewNormalHashNode("node_1", 20000, true))
	obj.debug()
	slotTables2 := obj.getSlotTable()
	fileNodeMap2 := printAllocateResult(obj)

	// 槽位变化、文件变化
	printFileNodeDiff(fileNodeMap1, fileNodeMap2)
	printSlotDiff(slotTables1, slotTables2)

	// 第三次: 文件与节点的对应关系
	obj.UpdateNode(models.NewNormalHashNode("node_1", 30000, true))
	obj.debug()
	slotTables3 := obj.getSlotTable()
	fileNodeMap3 := printAllocateResult(obj)

	// 槽位变化、文件变化
	printFileNodeDiff(fileNodeMap1, fileNodeMap3)
	printSlotDiff(slotTables1, slotTables3)

}

func TestRendezvousHashV2_AddNode(t *testing.T) {
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 30000, true),
		models.NewNormalHashNode("node_2", 60000, true),
		models.NewNormalHashNode("node_3", 10000, true),
	}
	hashFunc := utils.GetHashCode
	obj := NewRendezvousHashV2(nodes, hashFunc)

	// 第一次：文件与节点的对应关系
	slotTable1 := obj.getSlotTable()
	fileNodeMap1 := printAllocateResult(obj)

	// 新增节点
	obj.AddNode(models.NewNormalHashNode("node_4", 20000, true))
	// 第二次：文件与节点的对应关系
	slotTable2 := obj.getSlotTable()
	fileNodeMap2 := printAllocateResult(obj)

	// 文件变化、槽位变化
	printFileNodeDiff(fileNodeMap1, fileNodeMap2)
	printSlotDiff(slotTable1, slotTable2)
}

func TestRendezvousHashV2_HardRemoveNode(t *testing.T) {
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 20000, true),
		models.NewNormalHashNode("node_2", 50000, true),
		models.NewNormalHashNode("node_3", 10000, true),
		models.NewNormalHashNode("node_4", 70000, true),
	}
	hashFunc := utils.GetHashCode
	obj := NewRendezvousHashV2(nodes, hashFunc)

	// 第一次：文件与节点的对应关系
	slotTable1 := obj.getSlotTable()
	fileNodeMap1 := printAllocateResult(obj)
	obj.debug()
	obj.printSlotNodeTable()

	// 新增节点
	obj.HardRemoveNode("node_4")
	// 第二次：文件与节点的对应关系
	slotTable2 := obj.getSlotTable()
	fileNodeMap2 := printAllocateResult(obj)
	obj.debug()

	// 文件变化、槽位变化
	printFileNodeDiff(fileNodeMap1, fileNodeMap2)
	printSlotDiff(slotTable1, slotTable2)
	obj.printSlotNodeTable()
}

func printAllocateResult(obj *RendezvousHashV2[*models.NormalHashNode]) map[string][]string {
	hashFunc := utils.GetHashCode
	// 测试节点的权重
	fileNodeMap := make(map[string][]string)
	nodeResultMap := make(map[string]uint64)
	for i := 0; i < 1000000; i++ {
		key := "key_" + strconv.Itoa(i)
		results := obj.Get(key, hashFunc)
		nodeKeys := make([]string, 0)
		if len(results) <= 0 {
			nodeResultMap["outsource"]++
			nodeKeys = append(nodeKeys, "outsource")
		} else {
			for _, nk := range results {
				nodeResultMap[nk.GetKey()]++
				nodeKeys = append(nodeKeys, nk.GetKey())
			}
		}
		fileNodeMap[key] = nodeKeys
	}
	for k, v := range nodeResultMap {
		fmt.Printf("req node: %v, count: %v\n", k, v)
	}
	fmt.Printf("\n")
	return fileNodeMap
}

func printSlotDiff(slotTable1, slotTable2 [][]string) {
	nodeSlots1 := make(map[string][]int)
	for slot, nodes1 := range slotTable1 {
		for _, node := range nodes1 {
			nodeSlots1[node] = append(nodeSlots1[node], slot)
		}
	}
	for node, slots := range nodeSlots1 {
		fmt.Printf("node: %v, slots: %v\n", node, slots)
	}
	nodeSlots2 := make(map[string][]int)
	for slot, nodes2 := range slotTable2 {
		for _, node := range nodes2 {
			nodeSlots2[node] = append(nodeSlots2[node], slot)
		}
	}
	for node, slots := range nodeSlots2 {
		fmt.Printf("node: %v, slots: %v\n", node, slots)
	}

	adjustNum := 0
	for slot2, nodes2 := range slotTable2 {
		nodes1 := slotTable1[slot2]
		if len(utils.SymmetricDifference(nodes1, nodes2)) > 0 {
			adjustNum++
		}
	}
	fmt.Printf("total slotDiff adjust num: %v\n", adjustNum)

	nodeSlotMap1 := make(map[string][]int)
	for slot1, nodes1 := range slotTable1 {
		for _, node1 := range nodes1 {
			nodeSlotMap1[node1] = append(nodeSlotMap1[node1], slot1)
		}
	}
	nodeSlotMap2 := make(map[string][]int)
	for slot2, nodes2 := range slotTable2 {
		for _, node2 := range nodes2 {
			nodeSlotMap2[node2] = append(nodeSlotMap2[node2], slot2)
		}
	}
	for node, slots2 := range nodeSlotMap2 {
		slots1 := nodeSlotMap1[node]
		commonSlots := utils.CountCommon(slots1, slots2)
		diffSlots := len(slots1) - commonSlots
		fmt.Printf("node: %v slotDiff num: %v\n", node, diffSlots)
	}
	fmt.Printf("\n")
}

func printFileNodeDiff(fileNodeMap1, fileNodeMap2 map[string][]string) {
	adjustNum := 0
	for file2, node2 := range fileNodeMap2 {
		node1 := fileNodeMap1[file2]
		if len(node1) == 0 && len(node2) == 0 {
			continue
		}
		if len(node1) == 0 || len(node2) == 0 {
			adjustNum++
		}
		sort.Strings(node1)
		sort.Strings(node2)
		if node1[0] != node2[0] {
			adjustNum++
		}
	}
	fmt.Printf("fileNodeDiff adjust num: %v\n\n", adjustNum)
}

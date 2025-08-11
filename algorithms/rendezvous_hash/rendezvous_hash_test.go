package rendezvous_hash

import (
	"consistent-hash/models"
	"consistent-hash/utils"
	"fmt"
	"sort"
	"strconv"
	"testing"
)

func TestRendezvousHash_NormalFunction(t *testing.T) {
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 3, true),
		models.NewNormalHashNode("node_2", 5, true),
		models.NewNormalHashNode("node_3", 2, true),
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
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 30, true),
		models.NewNormalHashNode("node_2", 69, true),
		models.NewNormalHashNode("node_3", 1, true),
	}
	slotNum := 1000
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
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 3, true),
		models.NewNormalHashNode("node_2", 6, true),
		models.NewNormalHashNode("node_3", 1, true),
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
	obj.AddNode(models.NewNormalHashNode("node_4", 2, true))
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

func TestRendezvousHash_RemoveNode(t *testing.T) {
	// 构建哈希
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 3, true),
		models.NewNormalHashNode("node_2", 6, true),
		models.NewNormalHashNode("node_3", 1, true),
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
	obj.RemoveNode(models.NewNormalHashNode("node_1", 3, true))
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

func TestRendezvousHash_UpdateNode(t *testing.T) {
	// 构建哈希
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 3, true),
		models.NewNormalHashNode("node_2", 6, true),
		models.NewNormalHashNode("node_3", 1, true),
	}
	slotNum := 1000
	hashFunc := utils.GetHashCode
	obj := NewRendezvousHash(slotNum, nodes, hashFunc)

	// 第一次：文件与节点的对应关系
	fileNodeMap1 := make(map[string]string)
	for i := 0; i < 1000000; i++ {
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
	nodes2 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 4, true),
		models.NewNormalHashNode("node_2", 5, true),
		models.NewNormalHashNode("node_3", 1, true),
	}
	slotNum2 := 1000
	obj2 := NewRendezvousHash(slotNum2, nodes2, hashFunc)
	fileNodeMap2 := make(map[string]string)
	for i := 0; i < 1000000; i++ {
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

	// 第三次: 文件与节点的对应关系
	nodes3 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 3, true),
		models.NewNormalHashNode("node_2", 6, true),
		models.NewNormalHashNode("node_3", 1, true),
	}
	slotNum3 := 1000
	obj3 := NewRendezvousHash(slotNum3, nodes3, hashFunc)
	fileNodeMap3 := make(map[string]string)
	for i := 0; i < 1000000; i++ {
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

func TestRendezvousHash_Rebuild(t *testing.T) {
	slotNum := 100000
	// 相同的参数，重建之后检验是否有变化
	nodes1 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_2", 5, true),
		models.NewNormalHashNode("node_3", 3, true),
		models.NewNormalHashNode("node_4", 2, true),
	}
	obj1 := NewRendezvousHash(slotNum, nodes1, utils.GetHashCode)

	obj1.UpdateNode(models.NewNormalHashNode("node_2", 7, true))
	obj1.UpdateNode(models.NewNormalHashNode("node_2", 1, true))
	obj1.UpdateNode(models.NewNormalHashNode("node_3", 1, true))
	obj1.UpdateNode(models.NewNormalHashNode("node_4", 1, true))
	obj1.UpdateNode(models.NewNormalHashNode("node_2", 5, true))
	obj1.UpdateNode(models.NewNormalHashNode("node_3", 3, true))
	obj1.UpdateNode(models.NewNormalHashNode("node_4", 2, true))

	slotTables1 := obj1.getSlotTable()
	fileNodeMap1 := printAllocateResult(obj1)

	nodes2 := []*models.NormalHashNode{
		models.NewNormalHashNode("node_2", 5, true),
		models.NewNormalHashNode("node_3", 3, true),
		models.NewNormalHashNode("node_4", 2, true),
	}
	obj2 := NewRendezvousHash(slotNum, nodes2, utils.GetHashCode)
	slotTables2 := obj2.getSlotTable()
	fileNodeMap2 := printAllocateResult(obj2)

	printSlotDiff(slotTables1, slotTables2)
	printFileNodeDiff(fileNodeMap1, fileNodeMap2)
}

func printAllocateResult(obj *RendezvousHash[*models.NormalHashNode]) map[string][]string {
	hashFunc := utils.GetHashCode
	// 测试节点的权重
	fileNodeMap := make(map[string][]string)
	nodeResultMap := make(map[string]uint64)
	for i := 0; i < 1000000; i++ {
		key := "key_" + strconv.Itoa(i)
		results := obj.Get(key, 1, hashFunc)
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

func printSlotDiff(slotTable1, slotTable2 map[int]string) {
	adjustNum := 0
	for slot2, node2 := range slotTable2 {
		node1 := slotTable1[slot2]
		if node1 != node2 {
			adjustNum++
		}
	}
	fmt.Printf("total slotDiff adjust num: %v\n", adjustNum)

	nodeSlotMap1 := make(map[string][]int)
	for slot1, node1 := range slotTable1 {
		nodeSlotMap1[node1] = append(nodeSlotMap1[node1], slot1)
	}
	nodeSlotMap2 := make(map[string][]int)
	for slot2, node2 := range slotTable2 {
		nodeSlotMap2[node2] = append(nodeSlotMap2[node2], slot2)
	}
	for node, slots2 := range nodeSlotMap2 {
		slots1 := nodeSlotMap1[node]
		diffSlots := utils.SymmetricDifference(slots1, slots2)
		fmt.Printf("node: %v slotDiff num: %v\n", node, len(diffSlots))
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

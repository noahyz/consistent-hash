package rendezvous_hash_v3

import (
	"consistent-hash/models"
	"consistent-hash/utils"
	"fmt"
	"os"
	"slices"
	"sort"
	"testing"
)

// deepCopySlots 返回槽位映射的深度拷贝
func deepCopySlots(src [][]string) [][]string {
	copySlots := make([][]string, len(src))
	for i, nodes := range src {
		newNodes := make([]string, len(nodes))
		copy(newNodes, nodes)
		copySlots[i] = newNodes
	}
	return copySlots
}

// countSlotDifferences 统计两个映射之间不同槽位的数量
func countSlotDifferences(a, b [][]string) {
	count := 0
	for i := range a {
		sort.Strings(a[i])
		sort.Strings(b[i])
		if !slices.Equal(a[i], b[i]) {
			count++
		}
	}
	fmt.Printf("slot diff count: %v\n", count)
}

func countNodeSlot(a [][]string) {
	nodeSlotMap := make(map[string][]int)
	for slot, nodes := range a {
		for _, node := range nodes {
			nodeSlotMap[node] = append(nodeSlotMap[node], slot)
		}
	}
	fmt.Printf("\n")
	for node, slots := range nodeSlotMap {
		fmt.Printf("node: %v slot num: %v\n", node, len(slots))
	}
	fmt.Printf("\n")
}

func countNodeDifferences(a, b [][]string) {
	node1SlotMap := make(map[string][]int)
	node2SlotMap := make(map[string][]int)
	for slot, nodes1 := range a {
		for _, node1 := range nodes1 {
			node1SlotMap[node1] = append(node1SlotMap[node1], slot)
		}
	}
	for slot, nodes2 := range b {
		for _, node2 := range nodes2 {
			node2SlotMap[node2] = append(node2SlotMap[node2], slot)
		}
	}
	fmt.Printf("\n")
	for node, slots2 := range node2SlotMap {
		slots1 := node1SlotMap[node]
		diffSlots := utils.SymmetricDifference(slots1, slots2)
		fmt.Printf("node: %v slotDiff num: %v\n", node, len(diffSlots))
	}
	fmt.Printf("\n")
}

func printSlotNodes(slotNodes [][]string, fileName string) {
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Printf("create file err: %v\n", err)
		return
	}
	defer file.Close()

	emptyNodeNum := 0
	for slot, nodes := range slotNodes {
		_, err = fmt.Fprintf(file, "%v: %v\n", slot, nodes)
		if err != nil {
			fmt.Printf("write file err: %v\n", err)
		}
		if len(nodes) == 0 {
			emptyNodeNum++
		}
	}
	fmt.Printf("empty node num: %v\n", emptyNodeNum)
}

func TestWeightCycleRestoresMapping(t *testing.T) {
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("A", 30000, true),
		models.NewNormalHashNode("B", 50000, true),
		models.NewNormalHashNode("C", 70000, true),
	}
	r := NewRendezvousHashV3(nodes)
	// 初始添加3个节点
	initial := deepCopySlots(r.slotNodeTable)

	// 多次调整权重
	r.UpdateWeight("A", 40000)
	r.UpdateWeight("B", 40000)
	r.UpdateWeight("C", 30000)
	r.UpdateWeight("A", 30000)
	r.UpdateWeight("B", 50000)
	r.UpdateWeight("C", 70000)

	// 再次重建
	final := deepCopySlots(r.slotNodeTable)
	countSlotDifferences(initial, final)
}

func TestRebuildIdempotency(t *testing.T) {
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("A", 10000, true),
		models.NewNormalHashNode("B", 20000, true),
	}
	r1 := NewRendezvousHashV3(nodes)
	before := deepCopySlots(r1.slotNodeTable)

	r2 := NewRendezvousHashV3(nodes)
	after := deepCopySlots(r2.slotNodeTable)

	countSlotDifferences(before, after)
}

//func TestSlotCountScenarios(t *testing.T) {
//	slots := 100
//	cases := []struct {
//		weights map[string]float64
//		expectK int     // 基础应分配数 k0
//		expectR float64 // 小数部分 r
//	}{
//		// 总和 < 100%
//		{map[string]float64{"A": 30, "B": 20}, 0, 0.5},
//		// 总和 = 100%
//		{map[string]float64{"A": 60, "B": 40}, 1, 0},
//		// 总和 > 100%
//		{map[string]float64{"A": 70, "B": 50}, 1, 0.2},
//		// 总和 > 200%
//		{map[string]float64{"A": 120, "B": 100}, 2, 0.2},
//		// 极端差异
//		{map[string]float64{"A": 99, "B": 1}, 1, 0},
//	}
//	for _, c := range cases {
//		ch := New(slots)
//		for id, w := range c.weights {
//			ch.AddNode(id, w)
//		}
//		// 验证 computeK 逻辑
//		if ch.k0 != c.expectK {
//			t.Errorf("weights=%v: expected k0=%d, got=%d", c.weights, c.expectK, ch.k0)
//		}
//		if math.Abs(ch.r-c.expectR) > 1e-6 {
//			t.Errorf("weights=%v: expected r=%.2f, got=%.2f", c.weights, c.expectR, ch.r)
//		}
//		// 验证槽位每个归属数在 {k0,k0+1}
//		for i, nodes := range ch.slots {
//			if len(nodes) < ch.k0 || len(nodes) > ch.k0+1 {
//				t.Errorf("slot %d: node count %d not in [%d,%d]", i, len(nodes), ch.k0, ch.k0+1)
//			}
//		}
//	}
//}

func TestSlotChangesOnAddRemove(t *testing.T) {
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("A", 10000, true),
		models.NewNormalHashNode("B", 20000, true),
	}
	r := NewRendezvousHashV3(nodes)
	fmt.Printf("init start \n")
	base := deepCopySlots(r.slotNodeTable)
	countNodeSlot(base)
	printSlotNodes(base, "base1.txt")
	fmt.Printf("init end \n\n")

	fmt.Printf("add1 start \n")
	r.AddNode("N1", 50000)
	//r.AddNode("N2", 50000)
	added1 := deepCopySlots(r.slotNodeTable)
	countNodeSlot(added1)
	countSlotDifferences(base, added1)
	countNodeDifferences(base, added1)
	printSlotNodes(added1, "added1.txt")
	fmt.Printf("add1 end \n\n")

	// 添加新节点
	fmt.Printf("add2 start \n")
	r.AddNode("N3", 50000)
	added2 := deepCopySlots(r.slotNodeTable)
	countNodeSlot(added2)
	countSlotDifferences(added1, added2)
	countNodeDifferences(added1, added2)
	fmt.Printf("add2 end \n\n")

	// 删除节点
	fmt.Printf("remove start \n")
	r.RemoveNode("N2")
	removed := deepCopySlots(r.slotNodeTable)
	countNodeSlot(removed)
	countSlotDifferences(added2, removed)
	countNodeDifferences(added2, removed)
	fmt.Printf("remove end \n")
}

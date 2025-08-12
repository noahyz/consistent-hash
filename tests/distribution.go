package tests

import (
	"consistent-hash/algorithms/anchor_hash"
	"consistent-hash/algorithms/dx_hash"
	"consistent-hash/algorithms/jump_hash"
	"consistent-hash/algorithms/maglev_hash"
	"consistent-hash/algorithms/rendezvous_hash"
	"consistent-hash/algorithms/ring_hash"
	"consistent-hash/algorithms/slot_hash"
	"consistent-hash/models"
	"consistent-hash/utils"
	"fmt"
	"math"
)

func testDistribution() {
	fmt.Println("1. 分布均匀性测试:")

	nodeCount := 1000
	keyCount := 100000

	// 节点
	nodeList := make([]models.HashNode, 0, nodeCount)
	nodeKeyList := make([]string, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeKey := fmt.Sprintf("node_%d", i)
		nodeKeyList = append(nodeKeyList, nodeKey)
		nodeList = append(nodeList, models.NewNormalHashNode(nodeKey, 1, true))
	}

	// 创建各种算法实例
	ringHash40 := ring_hash.NewRingHash(40, 1, nodeList, utils.GetHashCode) // 添加40个虚拟节点的测试
	ringHash160 := ring_hash.NewRingHash(160, 1, nodeList, utils.GetHashCode)
	rendezvousHash := rendezvous_hash.NewRendezvousHash(nodeList, utils.GetHashCode)
	jumpHash := jump_hash.NewJumpHash(nodeList, utils.GetHashCode)
	maglevHash2039 := maglev_hash.NewMaglevHash(nodeKeyList, 2039)
	maglevHash65537 := maglev_hash.NewMaglevHash(nodeKeyList, 65537)
	anchorHash2000 := anchor_hash.NewAnchorHash(nodeKeyList, 2000, utils.GetHashCode)
	dxHash := dx_hash.NewDxHash(nodeList, nodeCount)
	slotHash := slot_hash.NewSlotHash(nodeList, utils.GetHashCode)

	// 生成测试键
	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	// 测试哈希环分布(40个虚拟节点)
	ringHash40Distribution := make(map[string]int)
	for _, key := range keys {
		node, err := ringHash40.Get(key, 1)
		if err != nil {
			fmt.Printf("ringHash40 get err: %v\n", err)
			return
		}
		ringHash40Distribution[node[0].GetKey()]++
	}

	// 测试哈希环分布(160个虚拟节点)
	ringHash160Distribution := make(map[string]int)
	for _, key := range keys {
		node, err := ringHash160.Get(key, 1)
		if err != nil {
			fmt.Printf("ringHash160 get err: %v\n", err)
			return
		}
		ringHash160Distribution[node[0].GetKey()]++
	}

	// 测试Rendezvous哈希分布
	rendezvousHashDistribution := make(map[string]int)
	for _, key := range keys {
		node, err := rendezvousHash.Get(key)
		if err != nil {
			fmt.Printf("rendezvousHash get err: %v\n", err)
			return
		}
		rendezvousHashDistribution[node.GetKey()]++
	}

	// 测试跳跃哈希分布
	jumpHashDistribution := make(map[string]int)
	for _, key := range keys {
		node, err := jumpHash.Get(key)
		if err != nil {
			fmt.Printf("jumpHash get err: %v\n", err)
			return
		}
		jumpHashDistribution[node.GetKey()]++
	}

	// 测试Maglev哈希分布(2039表长)
	maglevHash2039Distribution := make(map[string]int)
	for _, key := range keys {
		node, err := maglevHash2039.Get(key)
		if err != nil {
			fmt.Printf("maglevHash2039 get err: %v\n", err)
			return
		}
		maglevHash2039Distribution[node]++
	}

	// 测试Maglev哈希分布(65537表长)
	maglevHash65537Distribution := make(map[string]int)
	for _, key := range keys {
		node, err := maglevHash65537.Get(key)
		if err != nil {
			fmt.Printf("maglevHash65537 get err: %v\n", err)
			return
		}
		maglevHash65537Distribution[node]++
	}

	// 测试AnchorHash分布(2000长度)
	anchorHashDistribution := make(map[string]int)
	for _, key := range keys {
		node, err := anchorHash2000.Get(key)
		if err != nil {
			fmt.Printf("anchorHash2000 get err: %v\n", err)
			return
		}
		anchorHashDistribution[node]++
	}

	// 测试DxHash分布
	dxHashDistribution := make(map[string]int)
	for _, key := range keys {
		node, err := dxHash.Get(key)
		if err != nil {
			fmt.Printf("anchorHash2000 get err: %v\n", err)
			return
		}
		dxHashDistribution[node.GetKey()]++
	}

	// 测试SlotHash分布
	slotHashDistribution := make(map[string]int)
	for _, key := range keys {
		nodes := slotHash.Get(key)
		for _, node := range nodes {
			slotHashDistribution[node.GetKey()]++
		}
	}

	// 计算平均值
	avg := keyCount / nodeCount

	// 计算标准差（峰均值比相关）
	ringHash40StdDev := calculateStdDev(ringHash40Distribution, avg, nodeCount)
	ringHash160StdDev := calculateStdDev(ringHash160Distribution, avg, nodeCount)
	rendezvousHashStdDev := calculateStdDev(rendezvousHashDistribution, avg, nodeCount)
	jumpHashStdDev := calculateStdDev(jumpHashDistribution, avg, nodeCount)
	maglevHash2039StdDev := calculateStdDev(maglevHash2039Distribution, avg, nodeCount)
	maglevHash65537StdDev := calculateStdDev(maglevHash65537Distribution, avg, nodeCount)
	anchorHashStdDev := calculateStdDev(anchorHashDistribution, avg, nodeCount)
	dxHashStdDev := calculateStdDev(dxHashDistribution, avg, nodeCount)
	slotHashStdDev := calculateStdDev(slotHashDistribution, avg, nodeCount)

	fmt.Println("=== 分布均匀性测试 ===")
	fmt.Printf("使用 %d 个节点和 %d 个键进行测试\n", nodeCount, keyCount)
	fmt.Printf("每个节点平均分配键数: %d\n", avg)
	fmt.Printf("  RingHash(40个虚拟节点)标准差: %.2f\n", ringHash40StdDev)
	fmt.Printf("  RingHash(160个虚拟节点)标准差: %.2f\n", ringHash160StdDev)
	fmt.Printf("  RendezvousHash标准差: %.2f\n", rendezvousHashStdDev)
	fmt.Printf("  JumpHash标准差: %.2f\n", jumpHashStdDev)
	fmt.Printf("  MaglevHash(2039表长)标准差: %.2f\n", maglevHash2039StdDev)
	fmt.Printf("  MaglevHash(65537表长)标准差: %.2f\n", maglevHash65537StdDev)
	fmt.Printf("  AnchorHash标准差: %.2f\n", anchorHashStdDev)
	fmt.Printf("  DxHash标准差: %.2f\n", dxHashStdDev)
	fmt.Printf("  SlotHash标准差: %.2f\n", slotHashStdDev)
}

// calculateStdDev 计算标准差
func calculateStdDev(distribution map[string]int, avg, nodeCount int) float64 {
	sum := 0.0
	for _, count := range distribution {
		diff := float64(count - avg)
		sum += diff * diff
	}
	variance := sum / float64(nodeCount)
	return math.Sqrt(variance)
}

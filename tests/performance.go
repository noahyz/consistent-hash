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
	"time"
)

func testPerformance() {
	fmt.Println("\n3. 查询性能测试:")

	nodeCount := 1000
	opCount := 100000

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
	keys := make([]string, opCount)
	for i := 0; i < opCount; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	// 测试哈希环性能(40个虚拟节点)
	start := time.Now()
	for _, key := range keys {
		ringHash40.Get(key, 1)
	}
	rh40Elapsed := time.Since(start)

	// 测试哈希环性能(160个虚拟节点)
	start = time.Now()
	for _, key := range keys {
		ringHash160.Get(key, 1)
	}
	rh160Elapsed := time.Since(start)

	// 测试Rendezvous哈希性能
	start = time.Now()
	for _, key := range keys {
		rendezvousHash.Get(key)
	}
	rhElapsed := time.Since(start)

	// 测试跳跃哈希性能
	start = time.Now()
	for _, key := range keys {
		jumpHash.Get(key)
	}
	jhElapsed := time.Since(start)

	// 测试Maglev哈希性能(2039表长)
	start = time.Now()
	for _, key := range keys {
		maglevHash2039.Get(key)
	}
	mh2039Elapsed := time.Since(start)

	// 测试Maglev哈希性能(65537表长)
	start = time.Now()
	for _, key := range keys {
		maglevHash65537.Get(key)
	}
	mhElapsed := time.Since(start)

	// 测试AnchorHash性能(2000长度)
	start = time.Now()
	for _, key := range keys {
		anchorHash2000.Get(key)
	}
	ahElapsed := time.Since(start)

	// 测试DxHash性能
	start = time.Now()
	for _, key := range keys {
		dxHash.Get(key)
	}
	dxElapsed := time.Since(start)

	// 测试SlotHash性能
	start = time.Now()
	for _, key := range keys {
		slotHash.Get(key)
	}
	shElapsed := time.Since(start)

	fmt.Println("=== 查询性能测试 ===")
	fmt.Printf("执行 %d 次查询操作:\n", opCount)
	fmt.Printf("  RingHash(40个虚拟节点): %v\n", rh40Elapsed)
	fmt.Printf("  RingHash(160个虚拟节点): %v\n", rh160Elapsed)
	fmt.Printf("  RendezvousHash: %v\n", rhElapsed)
	fmt.Printf("  JumpHash: %v\n", jhElapsed)
	fmt.Printf("  MaglevHash(2039表长): %v\n", mh2039Elapsed)
	fmt.Printf("  MaglevHash(65537表长): %v\n", mhElapsed)
	fmt.Printf("  AnchorHash: %v\n", ahElapsed)
	fmt.Printf("  DxHash: %v\n", dxElapsed)
	fmt.Printf("  SlotHash: %v\n", shElapsed)
	fmt.Println("\n测试完成!")
}

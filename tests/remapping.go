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

func testAddNodeRemapping() {
	fmt.Println("2. 添加节点时的重映射测试:")

	initialNodes := 1000
	addCount := 10
	keyCount := 100000

	// 节点
	nodeList := make([]models.HashNode, 0, initialNodes)
	nodeKeyList := make([]string, 0, initialNodes)
	for i := 0; i < initialNodes; i++ {
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
	dxHash := dx_hash.NewDxHash(nodeList, initialNodes)
	slotHash := slot_hash.NewSlotHash(nodeList, utils.GetHashCode)

	// 生成测试键
	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}
	// 新增的节点
	newNodeList := make([]models.HashNode, addCount)
	newNodeKeyList := make([]string, addCount)
	for i := 0; i < addCount; i++ {
		nodeNum := initialNodes + i
		nk := fmt.Sprintf("node_%d", nodeNum)
		newNodeKeyList[i] = nk
		newNodeList[i] = models.NewNormalHashNode(nk, 1, true)
	}

	// 测试哈希环(40个虚拟节点)
	rh40Changed, rh40Elapsed, err := remappingOfRingHash40(ringHash40, addCount, keyCount, keys, newNodeList)
	if err != nil {
		fmt.Printf("remappingOfRingHash40 err: %v", err)
		return
	}
	// 测试哈希环(160个虚拟节点)
	rh160Changed, rh160Elapsed, err := remappingOfRingHash160(ringHash160, addCount, keyCount, keys, newNodeList)
	if err != nil {
		fmt.Printf("remappingOfRingHash160 err: %v", err)
		return
	}
	// 测试Rendezvous哈希
	rhChanged, rhElapsed, err := remappingOfRendezvousHash(rendezvousHash, addCount, keyCount, keys, newNodeList)
	if err != nil {
		fmt.Printf("remappingOfRendezvousHash err: %v", err)
		return
	}
	// 测试跳跃哈希
	jhChanged, jhElapsed, err := remappingOfJumpHash(jumpHash, addCount, keyCount, keys, newNodeList)
	if err != nil {
		fmt.Printf("remappingOfJumpHash err: %v", err)
		return
	}
	// 测试Maglev哈希(2039表长)
	mh2039Changed, mh2039Elapsed, err := remappingOfMaglevHash2039(maglevHash2039, addCount, keyCount, keys, newNodeKeyList)
	if err != nil {
		fmt.Printf("remappingOfMaglevHash2039 err: %v", err)
		return
	}
	// 测试Maglev哈希(65537表长)
	mh65537Changed, mh65537Elapsed, err := remappingOfMaglevHash65537(maglevHash65537, addCount, keyCount, keys, newNodeKeyList)
	if err != nil {
		fmt.Printf("remappingOfMaglevHash65537 err: %v", err)
		return
	}
	// 测试AnchorHash
	ahChanged, ahElapsed, err := remappingOfAnchorHash(anchorHash2000, addCount, keyCount, keys, newNodeKeyList)
	if err != nil {
		fmt.Printf("remappingOfAnchorHash err: %v", err)
		return
	}
	// 测试DxHash
	dhChanged, dhElapsed, err := remappingOfDxHash(dxHash, addCount, keyCount, keys, newNodeList)
	if err != nil {
		fmt.Printf("remappingOfDxHash err: %v", err)
		return
	}
	// 测试SlotHash
	shChanged, shElapsed, err := remappingOfSlotHash(slotHash, addCount, keyCount, keys, newNodeList)
	if err != nil {
		fmt.Printf("remappingOfSlotHash err: %v", err)
		return
	}

	fmt.Println("=== 添加节点时的重映射测试 ===")
	fmt.Printf("添加 %d 个节点到 %d 个初始节点:\n", addCount, initialNodes)
	fmt.Printf("  哈希环(40个虚拟节点): 耗时 %v, 重映射键数 %d (%.2f%%)\n", rh40Elapsed, rh40Changed, float64(rh40Changed)*100/float64(keyCount))
	fmt.Printf("  哈希环(160个虚拟节点): 耗时 %v, 重映射键数 %d (%.2f%%)\n", rh160Elapsed, rh160Changed, float64(rh160Changed)*100/float64(keyCount))
	fmt.Printf("  Rendezvous哈希: 耗时 %v, 重映射键数 %d (%.2f%%)\n", rhElapsed, rhChanged, float64(rhChanged)*100/float64(keyCount))
	fmt.Printf("  跳跃哈希: 耗时 %v, 重映射键数 %d (%.2f%%)\n", jhElapsed, jhChanged, float64(jhChanged)*100/float64(keyCount))
	fmt.Printf("  Maglev哈希(2039表长): 耗时 %v, 重映射键数 %d (%.2f%%)\n", mh2039Elapsed, mh2039Changed, float64(mh2039Changed)*100/float64(keyCount))
	fmt.Printf("  Maglev哈希(65537表长): 耗时 %v, 重映射键数 %d (%.2f%%)\n", mh65537Elapsed, mh65537Changed, float64(mh65537Changed)*100/float64(keyCount))
	fmt.Printf("  AnchorHash: 耗时 %v, 重映射键数 %d (%.2f%%)\n", ahElapsed, ahChanged, float64(ahChanged)*100/float64(keyCount))
	fmt.Printf("  DxHash: 耗时 %v, 重映射键数 %d (%.2f%%)\n", dhElapsed, dhChanged, float64(dhChanged)*100/float64(keyCount))
	fmt.Printf("  SlotHash: 耗时 %v, 重映射键数 %d (%.2f%%)\n", shElapsed, shChanged, float64(shChanged)*100/float64(keyCount))
}

func remappingOfRingHash40[T models.HashNode](ringHash40 *ring_hash.RingHash[T],
	addCount, keyCount int, keys []string, newNodeList []T) (int, time.Duration, error) {
	var err error
	// 测试哈希环(40个虚拟节点)
	rh40Before := make([]T, keyCount)
	for i, key := range keys {
		var resultNodes []T
		resultNodes, err = ringHash40.Get(key, 1)
		if err != nil {
			fmt.Printf("ringHash40 get err: %v\n", err)
			return 0, 0, err
		}
		rh40Before[i] = resultNodes[0]
	}

	start := time.Now()
	for i := 0; i < addCount; i++ {
		ringHash40.AddNode(newNodeList[i])
	}
	rh40Elapsed := time.Since(start)

	rh40After := make([]T, keyCount)
	rh40Changed := 0
	for i, key := range keys {
		var resultNodes []T
		resultNodes, err = ringHash40.Get(key, 1)
		if err != nil {
			fmt.Printf("ringHash40 get err: %v\n", err)
			return 0, 0, err
		}
		rh40After[i] = resultNodes[0]
		if rh40Before[i].GetKey() != rh40After[i].GetKey() {
			rh40Changed++
		}
	}
	return rh40Changed, rh40Elapsed, nil
}

func remappingOfRingHash160[T models.HashNode](ringHash160 *ring_hash.RingHash[T],
	addCount, keyCount int, keys []string, newNodeList []T) (int, time.Duration, error) {
	var err error
	rh160Before := make([]T, keyCount)
	for i, key := range keys {
		var resultNodes []T
		resultNodes, err = ringHash160.Get(key, 1)
		if err != nil {
			fmt.Printf("ringHash160 get err: %v\n", err)
			return 0, 0, err
		}
		rh160Before[i] = resultNodes[0]
	}

	start := time.Now()
	for i := 0; i < addCount; i++ {
		ringHash160.AddNode(newNodeList[i])
	}
	rh160Elapsed := time.Since(start)

	rh160After := make([]T, keyCount)
	rh160Changed := 0
	for i, key := range keys {
		var resultNodes []T
		resultNodes, err = ringHash160.Get(key, 1)
		if err != nil {
			fmt.Printf("ringHash160 get err: %v\n", err)
			return 0, 0, err
		}
		rh160After[i] = resultNodes[0]
		if rh160Before[i].GetKey() != rh160After[i].GetKey() {
			rh160Changed++
		}
	}
	return rh160Changed, rh160Elapsed, nil
}

func remappingOfRendezvousHash[T models.HashNode](rendezvousHash *rendezvous_hash.RendezvousHash[T],
	addCount, keyCount int, keys []string, newNodeList []T) (int, time.Duration, error) {
	var err error
	rhBefore := make([]T, keyCount)
	for i, key := range keys {
		rhBefore[i], err = rendezvousHash.Get(key)
		if err != nil {
			fmt.Printf("rendezvousHash get err: %v\n", err)
			return 0, 0, err
		}
	}
	start := time.Now()
	for i := 0; i < addCount; i++ {
		rendezvousHash.AddNode(newNodeList[i])
	}
	rhElapsed := time.Since(start)

	rhAfter := make([]T, keyCount)
	rhChanged := 0
	for i, key := range keys {
		rhAfter[i], err = rendezvousHash.Get(key)
		if err != nil {
			fmt.Printf("rendezvousHash get err: %v\n", err)
			return 0, 0, err
		}
		if rhBefore[i].GetKey() != rhAfter[i].GetKey() {
			rhChanged++
		}
	}
	return rhChanged, rhElapsed, nil
}

func remappingOfJumpHash[T models.HashNode](jumpHash *jump_hash.JumpHash[T],
	addCount, keyCount int, keys []string, newNodeList []T) (int, time.Duration, error) {
	var err error
	jhBefore := make([]T, keyCount)
	for i, key := range keys {
		jhBefore[i], err = jumpHash.Get(key)
		if err != nil {
			fmt.Printf("jumpHash get err: %v\n", err)
			return 0, 0, err
		}
	}

	start := time.Now()
	for i := 0; i < addCount; i++ {
		jumpHash.AddNode(newNodeList[i])
	}
	jhElapsed := time.Since(start)

	jhAfter := make([]T, keyCount)
	jhChanged := 0
	for i, key := range keys {
		jhAfter[i], err = jumpHash.Get(key)
		if err != nil {
			fmt.Printf("jumpHash get err: %v\n", err)
			return 0, 0, err
		}
		if jhBefore[i].GetKey() != jhAfter[i].GetKey() {
			jhChanged++
		}
	}
	return jhChanged, jhElapsed, nil
}

func remappingOfMaglevHash2039(maglevHash2039 *maglev_hash.MaglevHash,
	addCount, keyCount int, keys []string, newNodeKeyList []string) (int, time.Duration, error) {
	var err error
	mh2039Before := make([]string, keyCount)
	for i, key := range keys {
		mh2039Before[i], err = maglevHash2039.Get(key)
		if err != nil {
			fmt.Printf("maglevHash2039 get err: %v\n", err)
			return 0, 0, err
		}
	}

	start := time.Now()
	for i := 0; i < addCount; i++ {
		maglevHash2039.AddNode(newNodeKeyList[i])
	}
	mh2039Elapsed := time.Since(start)

	mh2039After := make([]string, keyCount)
	mh2039Changed := 0
	for i, key := range keys {
		mh2039After[i], err = maglevHash2039.Get(key)
		if err != nil {
			fmt.Printf("maglevHash2039 get err: %v\n", err)
			return 0, 0, err
		}
		if mh2039Before[i] != mh2039After[i] {
			mh2039Changed++
		}
	}
	return mh2039Changed, mh2039Elapsed, nil
}

func remappingOfMaglevHash65537(maglevHash65537 *maglev_hash.MaglevHash,
	addCount, keyCount int, keys []string, newNodeKeyList []string) (int, time.Duration, error) {
	var err error
	mhBefore := make([]string, keyCount)
	for i, key := range keys {
		mhBefore[i], err = maglevHash65537.Get(key)
		if err != nil {
			fmt.Printf("maglevHash65537 get err: %v\n", err)
			return 0, 0, err
		}
	}

	start := time.Now()
	for i := 0; i < addCount; i++ {
		maglevHash65537.AddNode(newNodeKeyList[i])
	}
	mhElapsed := time.Since(start)

	mhAfter := make([]string, keyCount)
	mhChanged := 0
	for i, key := range keys {
		mhAfter[i], err = maglevHash65537.Get(key)
		if err != nil {
			fmt.Printf("maglevHash65537 get err: %v\n", err)
			return 0, 0, err
		}
		if mhBefore[i] != mhAfter[i] {
			mhChanged++
		}
	}
	return mhChanged, mhElapsed, nil
}

func remappingOfAnchorHash(anchorHash2000 *anchor_hash.AnchorHash,
	addCount, keyCount int, keys []string, newNodeKeyList []string) (int, time.Duration, error) {
	var err error
	ahBefore := make([]string, keyCount)
	for i, key := range keys {
		ahBefore[i], err = anchorHash2000.Get(key)
		if err != nil {
			fmt.Printf("anchorHash2000 get err: %v\n", err)
			return 0, 0, err
		}
	}

	start := time.Now()
	for i := 0; i < addCount; i++ {
		anchorHash2000.AddBucket(newNodeKeyList[i])
	}
	ahElapsed := time.Since(start)

	ahAfter := make([]string, keyCount)
	ahChanged := 0
	for i, key := range keys {
		ahAfter[i], err = anchorHash2000.Get(key)
		if err != nil {
			fmt.Printf("anchorHash2000 get err: %v\n", err)
			return 0, 0, err
		}
		if ahBefore[i] != ahAfter[i] {
			ahChanged++
		}
	}
	return ahChanged, ahElapsed, nil
}

func remappingOfDxHash[T models.HashNode](dxHash *dx_hash.DxHash[T],
	addCount, keyCount int, keys []string, newNodeList []T) (int, time.Duration, error) {
	var err error
	dxBefore := make([]T, keyCount)
	for i, key := range keys {
		dxBefore[i], err = dxHash.Get(key)
		if err != nil {
			fmt.Printf("dxHash get err: %v\n", err)
			return 0, 0, err
		}
	}

	start := time.Now()
	for i := 0; i < addCount; i++ {
		dxHash.AddNode(newNodeList[i])
	}
	dxElapsed := time.Since(start)

	dxAfter := make([]T, keyCount)
	dxChanged := 0
	for i, key := range keys {
		dxAfter[i], err = dxHash.Get(key)
		if err != nil {
			fmt.Printf("dxHash get err: %v\n", err)
			return 0, 0, err
		}
		if dxBefore[i].GetKey() != dxAfter[i].GetKey() {
			dxChanged++
		}
	}
	return dxChanged, dxElapsed, nil
}

func remappingOfSlotHash[T models.HashNode](shHash *slot_hash.SlotHash[T],
	addCount, keyCount int, keys []string, newNodeList []T) (int, time.Duration, error) {
	shBefore := make([][]models.HashNode, keyCount)
	for i, key := range keys {
		resultNodes := shHash.Get(key)
		if len(resultNodes) > 0 {
			for _, rn := range resultNodes {
				shBefore[i] = append(shBefore[i], rn)
			}
		} else {
			shBefore[i] = []models.HashNode{models.NewNormalHashNode("", 1, false)}
		}
	}

	start := time.Now()
	for i := 0; i < addCount; i++ {
		shHash.AddNode(newNodeList[i])
	}
	shElapsed := time.Since(start)

	shAfter := make([][]models.HashNode, keyCount)
	shChanged := 0
	for i, key := range keys {
		resultNodes := shHash.Get(key)
		if len(resultNodes) > 0 {
			for _, rn := range resultNodes {
				shAfter[i] = append(shAfter[i], rn)
			}
		} else {
			shAfter[i] = []models.HashNode{models.NewNormalHashNode("", 1, false)}
		}
		isEqual := false
		for _, nodeBefore := range shBefore[i] {
			for _, nodeAfter := range shAfter[i] {
				if nodeBefore.GetKey() == nodeAfter.GetKey() {
					isEqual = true
					break
				}
			}
			if isEqual {
				break
			}
		}
		if !isEqual {
			shChanged++
		}
	}
	return shChanged, shElapsed, nil
}

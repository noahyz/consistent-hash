package tests

import (
	"fmt"
)

func Run() {
	fmt.Println("一致性哈希算法对比测试")
	fmt.Println("========================")

	// 测试分布均匀性
	testDistribution()
	fmt.Println()

	// 测试添加节点时的重映射
	testAddNodeRemapping()
	fmt.Println()

	// 测试查询性能
	testPerformance()
	fmt.Println()
}

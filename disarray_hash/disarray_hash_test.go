package disarray_hash

import (
	"consistent-hash/models"
	"consistent-hash/utils"
	"strconv"
	"testing"
)

func TestDisarrayHash_NormalFunction(t *testing.T) {
	vnodeBaseNum := 10
	ringFloorLimit := 1
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 3, true),
		models.NewNormalHashNode("node_2", 5, true),
		models.NewNormalHashNode("node_3", 2, true),
	}
	hashFunc := utils.GetHashCode
	obj := NewDisarrayHash(vnodeBaseNum, ringFloorLimit, nodes, hashFunc)

	key := "photoId_1"
	results := obj.Get(key, 1, hashFunc)
	if len(results) <= 0 {
		t.Errorf("hash ring get empty result")
		return
	}
	t.Logf("result: %v", results[0])
}

func TestDisarrayHash_AllocateRatio(t *testing.T) {
	vnodeBaseNum := 1000
	ringFloorLimit := 1
	nodes := []*models.NormalHashNode{
		models.NewNormalHashNode("node_1", 3, true),
		models.NewNormalHashNode("node_2", 5, true),
		models.NewNormalHashNode("node_3", 2, true),
	}
	hashFunc := utils.GetHashCode
	obj := NewDisarrayHash(vnodeBaseNum, ringFloorLimit, nodes, hashFunc)

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

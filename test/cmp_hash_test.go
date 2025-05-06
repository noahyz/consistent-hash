package test

import (
	"consistent-hash/disarray_hash"
	"consistent-hash/ring_hash"
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

func TestCmpHash(t *testing.T) {
	vnodeBaseNum := 1000
	ringFloorLimit := 1
	nodes := []*NormalHashNode{
		NewNormalHashNode("node_1", 1),
		NewNormalHashNode("node_2", 2),
		NewNormalHashNode("node_3", 2),
		NewNormalHashNode("node_4", 3),
		NewNormalHashNode("node_5", 1),
		NewNormalHashNode("node_6", 1),
	}
	hashFunc := utils.GetHashCode
	ringHash := ring_hash.NewRingHash(vnodeBaseNum, ringFloorLimit, nodes, hashFunc)
	disarrayHash := disarray_hash.NewDisarrayHash(vnodeBaseNum, ringFloorLimit, nodes, hashFunc)

	// 测试节点的权重
	nodeResultMap1 := map[string]uint64{
		"node_1": 0,
		"node_2": 0,
		"node_3": 0,
		"node_4": 0,
		"node_5": 0,
		"node_6": 0,
	}
	nodeResultMap2 := map[string]uint64{
		"node_1": 0,
		"node_2": 0,
		"node_3": 0,
		"node_4": 0,
		"node_5": 0,
		"node_6": 0,
	}

	equalCount := 0
	notEqualCount := 0
	for i := 0; i < 100000; i++ {
		key := "key_" + strconv.Itoa(i)
		results1 := ringHash.Get(key, 1, hashFunc)
		results2 := disarrayHash.Get(key, 1, hashFunc)
		if len(results1) <= 0 || len(results2) <= 0 {
			t.Fatal("hash get empty result")
			return
		}
		if results1[0].GetKey() == results2[0].GetKey() {
			equalCount++
		} else {
			notEqualCount++
		}
		nodeResultMap1[results1[0].GetKey()]++
		nodeResultMap2[results2[0].GetKey()]++
	}
	t.Logf("equal count: %v, not equal count: %v", equalCount, notEqualCount)
	t.Logf("ring hash. node_1: %v, node_2: %v, node_3: %v, node_4: %v, node_5: %v, node_6: %v",
		nodeResultMap1["node_1"], nodeResultMap1["node_2"], nodeResultMap1["node_3"],
		nodeResultMap1["node_4"], nodeResultMap1["node_5"], nodeResultMap1["node_6"])
	t.Logf("disarray hash. node_1: %v, node_2: %v, node_3: %v, node_4: %v, node_5: %v, node_6: %v",
		nodeResultMap2["node_1"], nodeResultMap2["node_2"], nodeResultMap2["node_3"],
		nodeResultMap1["node_4"], nodeResultMap1["node_5"], nodeResultMap1["node_6"])
}

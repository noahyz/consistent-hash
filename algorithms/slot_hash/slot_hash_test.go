package slot_hash

import (
	"consistent-hash/models"
	"consistent-hash/utils"
	"fmt"
	"testing"
)

func TestSlotHash(t *testing.T) {
	// 节点
	nodeCount := 100
	nodeList := make([]*models.NormalHashNode, 0, nodeCount)
	nodeKeyList := make([]string, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeKey := fmt.Sprintf("node_%d", i)
		nodeKeyList = append(nodeKeyList, nodeKey)
		nodeList = append(nodeList, models.NewNormalHashNode(nodeKey, 1, true))
	}
	slotHash := NewSlotHash(nodeList, utils.GetHashCode)
	slotTable := slotHash.GetSlotTable()
	for slot, nodes := range slotTable {
		t.Logf("slot: %v nodes: %v", slot, nodes)
	}
}

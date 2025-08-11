package rendezvous_hash_v3

import (
	"consistent-hash/models"
	"consistent-hash/utils"
	"fmt"
	"math"
	"sort"
)

const (
	defaultNodeWeightBaseNum = 100000
	defaultSlotNum           = 1000
)

var (
	hashFunc = utils.GetHashCode
)

type nodeScore struct {
	nodeKey string
	score   float64
}

type RendezvousHashV3 struct {
	nodeInstanceMap  map[string]*models.NormalHashNode // node --> 节点实例
	nodeSlotScoreMap map[string][]float64              // node --> 每个槽位得分
	slotNodeTable    [][]string                        // 槽位 --> 节点列表
}

func NewRendezvousHashV3(nodes []*models.NormalHashNode) *RendezvousHashV3 {
	r := &RendezvousHashV3{
		nodeInstanceMap:  make(map[string]*models.NormalHashNode),
		nodeSlotScoreMap: make(map[string][]float64),
		slotNodeTable:    make([][]string, 0),
	}
	r.initRendezvousHash(nodes)
	return r
}

func (r *RendezvousHashV3) initRendezvousHash(nodes []*models.NormalHashNode) {
	for _, node := range nodes {
		nk := node.GetKey()
		r.nodeInstanceMap[nk] = node
		// 每个槽位的分数
		r.nodeSlotScoreMap[nk] = computeSlotScoreOfNode(node)
	}
	r.slotNodeTable = r.buildSlotNodeTable()
}

func (r *RendezvousHashV3) buildSlotNodeTable() [][]string {
	fixedSlotNum, extraSlotRatio := computeSlotNumRatio(r.nodeInstanceMap)
	slotNodeTable := make([][]string, defaultSlotNum)
	for slot := 0; slot < defaultSlotNum; slot++ {
		// 获取到所有节点得分
		nodeScores := make([]*nodeScore, 0)
		for nk, scores := range r.nodeSlotScoreMap {
			nodeScores = append(nodeScores, &nodeScore{
				nodeKey: nk,
				score:   scores[slot],
			})
		}
		sort.Slice(nodeScores, func(i, j int) bool {
			return nodeScores[i].score > nodeScores[j].score
		})
		// 计算槽位要归属的机房数
		nodeNum := computeNodeNumOfSlot(slot, fixedSlotNum, extraSlotRatio)
		topNodes := make([]string, 0, nodeNum)
		for i := 0; i < nodeNum && i < len(nodeScores); i++ {
			topNodes = append(topNodes, nodeScores[i].nodeKey)
		}
		slotNodeTable[slot] = topNodes
	}
	return slotNodeTable
}

// AddNode 新增节点并重建
func (r *RendezvousHashV3) AddNode(nk string, weight int) {
	// 节点实例
	node := models.NewNormalHashNode(nk, weight, true)
	r.nodeInstanceMap[nk] = node
	// 每个槽位的分数
	scores := computeSlotScoreOfNode(node)
	r.nodeSlotScoreMap[nk] = scores
	// 重置槽位归属机房
	r.slotNodeTable = r.buildSlotNodeTable()
}

// RemoveNode 删除节点并重建
func (r *RendezvousHashV3) RemoveNode(nk string) {
	delete(r.nodeInstanceMap, nk)
	delete(r.nodeSlotScoreMap, nk)
	// 重置槽位归属机房
	r.slotNodeTable = r.buildSlotNodeTable()
}

// UpdateWeight 更新节点权重并重建
func (r *RendezvousHashV3) UpdateWeight(nk string, weight int) {
	// 节点实例
	node := r.nodeInstanceMap[nk]
	node.SetWeight(weight)
	// 每个槽位的分数
	scores := computeSlotScoreOfNode(node)
	r.nodeSlotScoreMap[nk] = scores
	// 重置槽位归属机房
	r.slotNodeTable = r.buildSlotNodeTable()
}

// GetSlotNodes 获取槽位归属节点
func (r *RendezvousHashV3) GetSlotNodes(key string) []string {
	h := hashFunc([]byte(key))
	slot := int(h % uint64(defaultSlotNum))
	nodeKeys := r.slotNodeTable[slot]
	results := make([]string, 0)
	for _, nk := range nodeKeys {
		node := r.nodeInstanceMap[nk]
		if node.IsEnabled() {
			results = append(results, nk)
		}
	}
	return results
}

func computeSlotScoreOfNode(node *models.NormalHashNode) []float64 {
	nk := node.GetKey()
	weight := node.GetWeight()
	slotScores := make([]float64, defaultSlotNum)
	for slot := 0; slot < defaultSlotNum; slot++ {
		score := hashScore(nk, weight, slot)
		slotScores[slot] = score
	}
	return slotScores
}

func computeNodeNumOfSlot(slot, fixedSlotNum int, extraSlotRatio float64) int {
	u := uniformHash(fmt.Sprintf("nodes_slot_%d", slot))
	num := fixedSlotNum
	if u < extraSlotRatio {
		num++
	}
	return num
}

func computeSlotNumRatio(nodeInstanceMap map[string]*models.NormalHashNode) (int, float64) {
	var totalWeight = 0
	for _, node := range nodeInstanceMap {
		totalWeight += node.GetWeight()
	}
	Q := float64(totalWeight) / defaultNodeWeightBaseNum
	fixedSlotNum := int(math.Floor(Q))
	extraSlotRatio := Q - float64(fixedSlotNum)
	return fixedSlotNum, extraSlotRatio
}

func uniformHash(val string) float64 {
	rawHash := hashFunc([]byte(val))
	// h 的取值范围是 [0, 2^64-1]。加1后是 [1, 2^64]
	// 将哈希值规整到范围 (0, 1]
	normalizedHash := (float64(rawHash) + 1) / float64(math.MaxUint64)
	return normalizedHash
}

func hashScore(nodeKey string, nodeWeight, slot int) float64 {
	nodeSlot := fmt.Sprintf("score_node_%v_slot_%v", nodeKey, slot)
	rawHash := hashFunc([]byte(nodeSlot))
	// h 的取值范围是 [0, 2^64-1]。加1后是 [1, 2^64]
	// 将哈希值规整到范围 (0, 1]
	normalizedHash := (float64(rawHash) + 1) / float64(math.MaxUint64)
	logScore := 1.0 / -math.Log(normalizedHash)
	weightedScore := logScore * float64(nodeWeight)
	return weightedScore
}

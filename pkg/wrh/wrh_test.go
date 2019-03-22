package wrh

import (
	"testing"
)

func TestResponsibleNodes(t *testing.T) {
	nodeCount := 10
	respNodeCount := 3
	key := "test"
	nodes := make([]Node, nodeCount)
	for i := range nodes {
		node := &nodes[i]
		node.Seed = uint32(i + 1)
		node.Weight = 1.0
		node.score = node.Score([]byte(key))
	}
	respNodes := make([]Node, respNodeCount)
	ResponsibleNodes(nodes, []byte(key), respNodes)
	var lastSc float64
	for i := range respNodes {
		respNode := &respNodes[i]
		if i > 0 && lastSc < respNode.score {
			t.Errorf("key=%v\n", key)
			t.Errorf("nodes=%v\n", nodes)
			t.Errorf("respNodes=%v\n", respNodes)
			t.FailNow()
		}
		lastSc = respNode.score
	}
}

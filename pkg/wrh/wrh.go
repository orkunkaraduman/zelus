package wrh

import (
	"math"

	"github.com/spaolacci/murmur3"
)

func uint64ToFloat64(v uint64) float64 {
	ones := uint64(^uint64(0) >> (64 - 53))
	zeros := float64(1 << 53)
	return float64(v&ones) / zeros
}

func ResponsibleNodes(nodes []Node, key []byte, respNodes []Node) bool {
	count := len(respNodes)
	if count <= 0 {
		return false
	}
	for i := 0; i < count; i++ {
		respNodes[i] = Node{}
	}
	for i := 0; i < len(nodes); i++ {
		sc := nodes[i].Score(key)
		k := -1
		for j := 0; j < count; j++ {
			if sc > respNodes[j].score {
				if k < 0 || (respNodes[k].score > respNodes[j].score) {
					k = j
				}
			}
		}
		if k >= 0 {
			respNodes[k] = nodes[i]
			respNodes[k].score = sc
		}
	}
	return true
}

func FindSeed(nodes []Node, seed uint32) int {
	for i, j := 0, len(nodes); i < j; i++ {
		if nodes[i].Seed == seed {
			return i
		}
	}
	return -1
}

func MaxSeed(nodes []Node) uint32 {
	max := uint32(0)
	for i, j := 0, len(nodes); i < j; i++ {
		if nodes[i].Seed > max {
			max = nodes[i].Seed
		}
	}
	return max
}

func MergeNodes(nodes1, nodes2 []Node, mergedNodesIn []Node) (mergedNodes []Node) {
	mergedNodes = mergedNodesIn
	for i := range nodes1 {
		if FindSeed(mergedNodes, nodes1[i].Seed) >= 0 {
			continue
		}
		mergedNodes = append(mergedNodes, nodes1[i])
	}
	for i := range nodes2 {
		if FindSeed(mergedNodes, nodes2[i].Seed) >= 0 {
			continue
		}
		mergedNodes = append(mergedNodes, nodes2[i])
	}
	return
}

type Node struct {
	Seed     uint32
	Weight   float64
	UserData interface{}
	score    float64
}

func (nd *Node) Score(key []byte) float64 {
	if nd.Weight <= 0 {
		return 0
	}
	_, h2 := murmur3.Sum128WithSeed(key, nd.Seed)
	hf := uint64ToFloat64(h2)
	x := 1.0 / (-math.Log(hf))
	return nd.Weight*x + 1
}

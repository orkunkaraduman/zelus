package wrh

type nodesIfc struct {
	nodes []Node
}

func (ni *nodesIfc) Len() int {
	return len(ni.nodes)
}

func (ni *nodesIfc) Less(i, j int) bool {
	// descending order
	return ni.nodes[i].score >= ni.nodes[j].score
}

func (ni *nodesIfc) Swap(i, j int) {
	ni.nodes[i], ni.nodes[j] = ni.nodes[j], ni.nodes[i]
}

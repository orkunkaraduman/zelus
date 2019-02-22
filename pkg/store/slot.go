package store

import (
	"bytes"
	"sync"
)

type slot struct {
	Mu    sync.Mutex
	Nodes []node
}

func (sl *slot) FindNode(keyHash int, bKey []byte) int {
	bKeyLen := len(bKey)
	if bKeyLen <= 0 {
		return -1
	}
	for i := range sl.Nodes {
		nd := &sl.Nodes[i]
		if nd.KeyHash == keyHash {
			o, p := 0, bKeyLen
			for _, data := range nd.Datas {
				r := len(data)
				if r > p {
					r = p
				}
				if !bytes.Equal(bKey[o:r], data[:r]) {
					return -1
				}
				o += r
				p -= r
				if p == 0 {
					return i
				}
			}
		}
	}
	return -1
}

func (sl *slot) NewNode(slotPool MemPool) int {
	for i := range sl.Nodes {
		nd := &sl.Nodes[i]
		if nd.KeyHash < 0 {
			return i
		}
	}
	idx := len(sl.Nodes)
	ptr := allocBlock(slotPool, (idx+1)*sizeOfNode)
	if ptr == nil {
		return -1
	}
	newNodes := toSlice(ptr, len(ptr)/sizeOfNode, typeOfNode).([]node)
	for i := range newNodes {
		if i < idx {
			newNodes[i] = sl.Nodes[i]
		} else {
			newNodes[i].KeyHash = -1
			newNodes[i].Datas = nil
			newNodes[i].Size = 0
			newNodes[i].Expiry = -1
		}
	}
	if sl.Nodes != nil {
		slotPool.Free(toSlice(sl.Nodes, len(sl.Nodes)*sizeOfNode, typeOfByte).([]byte))
	}
	sl.Nodes = newNodes
	return idx
}

func (sl *slot) DelNode(idx int, slotPool MemPool) {
	nd := &sl.Nodes[idx]
	nd.KeyHash = -1
	nd.Datas = nil
	nd.Size = 0
	nd.Expiry = -1
	for i := range sl.Nodes {
		if sl.Nodes[i].KeyHash >= 0 {
			return
		}
	}
	if sl.Nodes != nil {
		slotPool.Free(toSlice(sl.Nodes, len(sl.Nodes)*sizeOfNode, typeOfByte).([]byte))
		sl.Nodes = nil
	}
}

func (sl *slot) FreeNode(idx int, slotPool, dataPool MemPool) {
	nd := &sl.Nodes[idx]
	nd.KeyHash = -1
	nd.Free(slotPool, dataPool)
	nd.Expiry = -1
	for i := range sl.Nodes {
		if sl.Nodes[i].KeyHash >= 0 {
			return
		}
	}
	if sl.Nodes != nil {
		slotPool.Free(toSlice(sl.Nodes, len(sl.Nodes)*sizeOfNode, typeOfByte).([]byte))
		sl.Nodes = nil
	}
}

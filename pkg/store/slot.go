package store

import (
	"bytes"
	"sync"
	"unsafe"

	"github.com/orkunkaraduman/zelus/pkg/malloc"
)

type slot struct {
	Mu    sync.Mutex
	Nodes []node
}

func (sl *slot) FindNode(keyHash int, bKey []byte) int {
	lbKey := len(bKey)
	if lbKey <= 0 {
		return -1
	}
	for i := range sl.Nodes {
		nd := &sl.Nodes[i]
		if nd.KeyHash == keyHash {
			o, p := 0, lbKey
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

func (sl *slot) NewNode(slotPool *malloc.Pool) int {
	for i := range sl.Nodes {
		nd := &sl.Nodes[i]
		if nd.KeyHash < 0 {
			return i
		}
	}
	var zeroNode node
	sizeOfNode := int(unsafe.Sizeof(zeroNode))
	idx := len(sl.Nodes)
	ptr := slotPool.AllocBlock((idx + 1) * sizeOfNode)
	if ptr == nil {
		return -1
	}
	newNodes := (*[^uint32(0) >> 1]node)(unsafe.Pointer(&ptr[0]))[:len(ptr)/sizeOfNode]
	for i := range newNodes {
		if i < idx {
			newNodes[i] = sl.Nodes[i]
		} else {
			newNodes[i].KeyHash = -1
			newNodes[i].Datas = nil
			newNodes[i].Size = 0
		}
	}
	if sl.Nodes != nil {
		slotPool.Free((*[^uint32(0) >> 1]byte)(unsafe.Pointer(&sl.Nodes[0]))[:len(sl.Nodes)*sizeOfNode][:])
	}
	sl.Nodes = newNodes
	return idx
}

func (sl *slot) DelNode(slotPool, dataPool *malloc.Pool, idx int) {
	nd := &sl.Nodes[idx]
	nd.KeyHash = -1
	nd.Free(slotPool, dataPool)
	var zeroNode node
	sizeOfNode := int(unsafe.Sizeof(zeroNode))
	for i := range sl.Nodes {
		if sl.Nodes[i].KeyHash >= 0 {
			return
		}
	}
	if sl.Nodes != nil {
		slotPool.Free((*[^uint32(0) >> 1]byte)(unsafe.Pointer(&sl.Nodes[0]))[:len(sl.Nodes)*sizeOfNode][:])
		sl.Nodes = nil
	}
}

type node struct {
	KeyHash int
	Datas   [][]byte
	Size    int
}

func (nd *node) Alloc(slotPool, dataPool *malloc.Pool, size int) int {
	firstSize := size
	if firstSize <= 0 {
		return len(nd.Datas)
	}
	size -= malloc.MinLength - (nd.Size-1)%malloc.MinLength - 1
	datas := [][]byte(nil)
	for size > 0 {
		var ptr []byte
		for l := 1 << uint(malloc.HighBit(size)-1); l >= malloc.MinLength && ptr == nil; l >>= 1 {
			ptr = dataPool.AllocBlock(l)
		}
		if ptr == nil {
			break
		}
		datas = append(datas, ptr)
		size -= len(ptr)
	}
	if size > 0 && size < malloc.MinLength {
		ptr := dataPool.AllocBlock(size)
		if ptr != nil {
			datas = append(datas, ptr)
			size -= len(ptr)
		}
	}
	if size > 0 {
		for _, ptr := range datas {
			dataPool.Free(ptr)
		}
		return -1
	}
	var zeroData []byte
	sizeOfData := int(unsafe.Sizeof(zeroData))
	idx := len(nd.Datas)
	for idx > 0 && nd.Datas[idx-1] == nil {
		idx--
	}
	newDatas := nd.Datas
	newDatasLen := idx + len(datas)
	if newDatasLen > len(nd.Datas) {
		ptr := slotPool.AllocBlock(newDatasLen * sizeOfData)
		if ptr == nil {
			for _, ptr := range datas {
				dataPool.Free(ptr)
			}
			return -1
		}
		newDatas = (*[^uint32(0) >> 1][]byte)(unsafe.Pointer(&ptr[0]))[:len(ptr)/sizeOfData]
		newDatasLen = len(newDatas)
		for i := 0; i < idx; i++ {
			newDatas[i] = nd.Datas[i]
		}
	}
	for i, k, l := idx, 0, len(datas); i < newDatasLen; i++ {
		if k < l {
			newDatas[i] = datas[k]
			k++
		} else {
			if nd.Datas != nil && &nd.Datas[0] == &newDatas[0] {
				break
			}
			newDatas[i] = nil
		}
	}
	if nd.Datas != nil && &nd.Datas[0] != &newDatas[0] {
		slotPool.Free((*[^uint32(0) >> 1]byte)(unsafe.Pointer(&nd.Datas[0]))[:len(nd.Datas)*sizeOfData][:])
	}
	nd.Datas = newDatas
	nd.Size += firstSize
	return idx
}

func (nd *node) Free(slotPool, dataPool *malloc.Pool) {
	var zeroData []byte
	sizeOfData := int(unsafe.Sizeof(zeroData))
	for _, data := range nd.Datas {
		dataPool.Free(data)
	}
	if nd.Datas != nil {
		slotPool.Free((*[^uint32(0) >> 1]byte)(unsafe.Pointer(&nd.Datas[0]))[:len(nd.Datas)*sizeOfData][:])
	}
	nd.Datas = nil
}

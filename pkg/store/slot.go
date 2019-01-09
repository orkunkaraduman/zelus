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
	for i, j := 0, len(sl.Nodes); i < j; i++ {
		nd := &sl.Nodes[i]
		if nd.KeyHash == keyHash {
			var buf [256]byte
			for k, l, m := 0, len(nd.Datas), 0; k < l && m < len(buf); k++ {
				m += copy(buf[m:], nd.Datas[k])
			}
			if bytes.Equal(bKey, buf[:len(bKey)]) {
				return i
			}
		}
	}
	return -1
}

func (sl *slot) NewNode(slotPool *malloc.Pool) int {
	for i, j := 0, len(sl.Nodes); i < j; i++ {
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
	for i, j := 0, len(newNodes); i < j; i++ {
		if i < idx {
			newNodes[i] = sl.Nodes[i]
		} else {
			newNodes[i].KeyHash = -1
			newNodes[i].Datas = nil
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
	//! free node array
}

type node struct {
	KeyHash int
	Datas   [][]byte
}

func (nd *node) Alloc(slotPool, dataPool *malloc.Pool, size int) int {
	datas := [][]byte(nil)
	for size > 0 {
		var ptr []byte
		for l := 1 << uint(malloc.HighBit(size)-1); l > 0 && ptr == nil; l >>= 1 {
			ptr = dataPool.Alloc(l)
		}
		if ptr == nil {
			break
		}
		datas = append(datas, ptr)
		size -= len(ptr)
	}
	if size != 0 {
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
	ptr := slotPool.AllocBlock((len(nd.Datas) - idx + len(datas)) * sizeOfData)
	if ptr == nil {
		return -1
	}
	newDatas := (*[^uint32(0) >> 1][]byte)(unsafe.Pointer(&ptr[0]))[:len(ptr)/sizeOfData]
	for i, j, k := 0, len(newDatas), 0; i < j; i++ {
		if i < idx {
			newDatas[i] = nd.Datas[i]
		} else {
			if k < len(datas) {
				newDatas[i] = datas[k]
				k++
			} else {
				newDatas[i] = nil
			}
		}
	}
	if nd.Datas != nil {
		slotPool.Free((*[^uint32(0) >> 1]byte)(unsafe.Pointer(&nd.Datas[0]))[:len(nd.Datas)*sizeOfData][:])
	}
	nd.Datas = newDatas
	return idx
}

func (nd *node) Free(slotPool, dataPool *malloc.Pool) {
	var zeroData []byte
	sizeOfData := int(unsafe.Sizeof(zeroData))
	for i, j := 0, len(nd.Datas); i < j; i++ {
		dataPool.Free(nd.Datas[i])
	}
	if nd.Datas != nil {
		slotPool.Free((*[^uint32(0) >> 1]byte)(unsafe.Pointer(&nd.Datas[0]))[:len(nd.Datas)*sizeOfData][:])
	}
	nd.Datas = nil
}

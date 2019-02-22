package store

import (
	"bytes"
	"sync"
	"unsafe"
)

type slot struct {
	Mu    sync.Mutex
	Nodes []node
}

var zeroSlot slot
var sizeOfSlot = int(unsafe.Sizeof(zeroSlot))

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
			newNodes[i].Expiry = -1
		}
	}
	if sl.Nodes != nil {
		slotPool.Free((*[^uint32(0) >> 1]byte)(unsafe.Pointer(&sl.Nodes[0]))[:len(sl.Nodes)*sizeOfNode][:])
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
		slotPool.Free((*[^uint32(0) >> 1]byte)(unsafe.Pointer(&sl.Nodes[0]))[:len(sl.Nodes)*sizeOfNode][:])
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
		slotPool.Free((*[^uint32(0) >> 1]byte)(unsafe.Pointer(&sl.Nodes[0]))[:len(sl.Nodes)*sizeOfNode][:])
		sl.Nodes = nil
	}
}

type node struct {
	KeyHash int
	Datas   [][]byte
	Size    int
	Expiry  int
}

var zeroNode node
var sizeOfNode = int(unsafe.Sizeof(zeroNode))

func (nd *node) Alloc(slotPool, dataPool MemPool, size int) bool {
	dataBlockSize := dataPool.BlockSize()
	sz := size
	if nd.Size > 0 {
		sz -= dataBlockSize - (nd.Size-1)%dataBlockSize - 1
	}
	var d [256][]byte
	datas := d[:0]
	for sz > 0 {
		var ptr []byte
		x, l := sz, 1
		for x != 0 {
			x >>= 1
			l <<= 1
		}
		l >>= 1
		for ; l >= dataBlockSize && ptr == nil; l >>= 1 {
			ptr = dataPool.AllocBlock(l)
		}
		if ptr == nil {
			break
		}
		datas = append(datas, ptr)
		sz -= len(ptr)
	}
	if sz > 0 && sz < dataBlockSize {
		ptr := dataPool.AllocBlock(sz)
		if ptr != nil {
			datas = append(datas, ptr)
			sz -= len(ptr)
		}
	}
	if sz > 0 {
		for _, ptr := range datas {
			dataPool.Free(ptr)
		}
		return false
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
			return false
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
	nd.Size += size
	return true
}

func (nd *node) Set(slotPool, dataPool MemPool, size int) bool {
	if nd.Size == size {
		return true
	}
	if size > nd.Size {
		return nd.Alloc(slotPool, dataPool, size-nd.Size)
	}
	sz := 0
	for i := range nd.Datas {
		if sz >= size {
			dataPool.Free(nd.Datas[i])
			nd.Datas[i] = nil
			continue
		}
		sz += len(nd.Datas[i])
	}
	nd.Size = size
	return true
}

func (nd *node) Free(slotPool, dataPool MemPool) {
	var zeroData []byte
	sizeOfData := int(unsafe.Sizeof(zeroData))
	for i := range nd.Datas {
		dataPool.Free(nd.Datas[i])
		nd.Datas[i] = nil
	}
	if nd.Datas != nil {
		slotPool.Free((*[^uint32(0) >> 1]byte)(unsafe.Pointer(&nd.Datas[0]))[:len(nd.Datas)*sizeOfData][:])
	}
	nd.Datas = nil
	nd.Size = 0
}

func (nd *node) Last() (index, offset int) {
	index = -1
	offset = nd.Size
	for i, data := range nd.Datas {
		dl := len(data)
		if offset <= dl {
			index = i
			return
		}
		offset -= dl
	}
	return
}

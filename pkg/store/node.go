package store

import "github.com/orkunkaraduman/zelus/pkg/utils"

type node struct {
	KeyHash int
	Datas   [][]byte
	Size    int
	Expiry  int
}

func (nd *node) Alloc(slotPool, dataPool MemPool, size int) bool {
	sz := size
	if nd.Size > 0 {
		sz -= blockSize - (nd.Size-1)%blockSize - 1
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
		for ; l >= blockSize && ptr == nil; l >>= 1 {
			if !NativeAlloc {
				ptr = allocBlock(dataPool, l)
			} else {
				ptr = make([]byte, l)
			}
		}
		if ptr == nil {
			break
		}
		datas = append(datas, ptr)
		sz -= len(ptr)
	}
	if sz > 0 && sz < blockSize {
		var ptr []byte
		if !NativeAlloc {
			ptr = allocBlock(dataPool, sz)
		} else {
			ptr = make([]byte, sz)
		}
		if ptr != nil {
			datas = append(datas, ptr)
			sz -= len(ptr)
		}
	}
	if sz > 0 {
		if !NativeAlloc {
			for _, ptr := range datas {
				dataPool.Free(ptr)
			}
		}
		return false
	}
	idx := len(nd.Datas)
	for idx > 0 && nd.Datas[idx-1] == nil {
		idx--
	}
	newDatas := nd.Datas
	newDatasLen := idx + len(datas)
	if newDatasLen > len(nd.Datas) {
		if !NativeAlloc {
			ptr := allocBlock(slotPool, newDatasLen*sizeOfData)
			if ptr == nil {
				for _, ptr := range datas {
					dataPool.Free(ptr)
				}
				return false
			}
			newDatas = utils.ChangeSliceType(ptr, len(ptr)/sizeOfData, typeOfData).([][]byte)
			newDatasLen = len(newDatas)
		} else {
			newDatas = make([][]byte, newDatasLen)
		}
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
	if nd.Datas != nil && &nd.Datas[0] != &newDatas[0] && !NativeAlloc {
		slotPool.Free(utils.ChangeSliceType(nd.Datas, len(nd.Datas)*sizeOfData, typeOfByte).([]byte))
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
			if !NativeAlloc {
				dataPool.Free(nd.Datas[i])
			}
			nd.Datas[i] = nil
			continue
		}
		sz += len(nd.Datas[i])
	}
	nd.Size = size
	return true
}

func (nd *node) Free(slotPool, dataPool MemPool) {
	for i := range nd.Datas {
		if !NativeAlloc {
			dataPool.Free(nd.Datas[i])
		}
		nd.Datas[i] = nil
	}
	if nd.Datas != nil && !NativeAlloc {
		slotPool.Free(utils.ChangeSliceType(nd.Datas, len(nd.Datas)*sizeOfData, typeOfByte).([]byte))
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

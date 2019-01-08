package malloc

import (
	"sync"
	"unsafe"
)

type Arena struct {
	mu       sync.Mutex
	buf      []byte
	freeList map[int]int
	stats    ArenaStats
}

type ArenaStats struct {
	TotalSize     int
	AllocatedSize int
	RequestedSize int
}

func NewArena(buf []byte) *Arena {
	totalSize := len(buf)
	if totalSize <= 0 {
		return nil
	}
	if totalSize != 1<<uint(HighBit(totalSize)-1) {
		panic(ErrSizeMustBePowerOfTwo)
	}
	a := &Arena{
		buf:      buf,
		freeList: make(map[int]int),
	}
	a.freeList[0] = totalSize
	a.stats.TotalSize = totalSize
	return a
}

func AllocArena(totalSize int) *Arena {
	if totalSize <= 0 {
		return nil
	}
	if totalSize != 1<<uint(HighBit(totalSize)-1) {
		panic(ErrSizeMustBePowerOfTwo)
	}
	buf := make([]byte, totalSize)
	return NewArena(buf)
}

func (a *Arena) alloc(size int, block bool) []byte {
	if size <= 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	offset, length := -1, int(^uint(0)>>1)
	for k, v := range a.freeList {
		if v >= size && v < length && k > offset {
			offset = k
			length = v
		}
	}
	if offset < 0 {
		return nil
	}
	foundOffset := offset
	ending := offset + length
	lengthHigh := HighBit(length - 1)
	sizeHigh := HighBit(size - 1)
	for offset < ending {
		if lengthHigh > sizeHigh {
			lengthHigh--
			length = 1 << uint(lengthHigh)
		}
		l := ending - offset
		if l < length {
			length = l
		}
		a.freeList[offset] = length
		if length >= size {
			foundOffset = offset
		}
		offset += length
	}
	offset = foundOffset
	length = a.freeList[offset]
	delete(a.freeList, offset)
	a.stats.AllocatedSize += length
	if block {
		a.stats.RequestedSize += length
		return a.buf[offset : offset+length]
	}
	a.stats.RequestedSize += size
	return a.buf[offset : offset+size]
}

func (a *Arena) Alloc(size int) []byte {
	return a.alloc(size, false)
}

func (a *Arena) AllocBlock(size int) []byte {
	return a.alloc(size, true)
}

func (a *Arena) Free(ptr []byte) {
	ptrSize := len(ptr)
	if ptrSize <= 0 {
		return
	}
	ptrOffset := int(uintptr(unsafe.Pointer(&ptr[0])) - uintptr(unsafe.Pointer(&a.buf[0])))
	if ptrOffset < 0 || ptrOffset >= len(a.buf) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	ptrHigh := HighBit(ptrSize - 1)
	ptrLength := 1 << uint(ptrHigh)
	offset := ptrOffset
	length := ptrLength
	a.freeList[offset] = length
	b := true
	for b {
		b = false
		if (offset/length)%2 == 0 {
			n := offset + length
			if l, ok := a.freeList[n]; ok && l == length {
				delete(a.freeList, n)
				length *= 2
				a.freeList[offset] = length
				b = true
			}
		} else {
			n := offset - length
			if l, ok := a.freeList[n]; ok && l == length {
				delete(a.freeList, offset)
				offset = n
				length *= 2
				a.freeList[offset] = length
				b = true
			}
		}
	}
	a.stats.AllocatedSize -= ptrLength
	a.stats.RequestedSize -= ptrSize
}

func (a *Arena) Stats() ArenaStats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stats
}

package malloc

import (
	"sync"
	"unsafe"
)

type Arena struct {
	mu    sync.Mutex
	buf   []byte
	fl    *freeList
	stats ArenaStats
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
	h := HighBit(totalSize - 1)
	if totalSize != 1<<uint(h) {
		panic(ErrSizeMustBePowerOfTwo)
	}
	a := &Arena{
		buf: buf,
		fl:  newFreeList(totalSize),
	}
	a.fl.setFree(0, h)
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
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
	return NewArena(buf)
}

func (a *Arena) alloc(size int, block bool) []byte {
	if size <= 0 {
		return nil
	}
	a.mu.Lock()
	sizeHigh := HighBit(size - 1)
	foundOffset, foundLength, foundHigh := -1, 0, -1
	offset, length, high := 0, 1<<minHigh, minHigh
	for offset < len(a.buf) {
		high = a.fl.get(offset)
		if high < 0 {
			panic("aa")
		}
		allocated := high&freeListAlloc != 0
		high &= 0x3f
		length = 1 << uint(high)
		if !allocated && high >= sizeHigh {
			foundOffset = offset
			foundLength = length
			foundHigh = high
		}
		offset += length
	}
	if foundOffset < 0 {
		a.mu.Unlock()
		return nil
	}
	a.fl.del(foundOffset)
	offset = foundOffset
	length = foundLength
	high = foundHigh
	ending := offset + length
	for offset < ending {
		if high > sizeHigh && high > minHigh {
			high--
			length >>= 1
		}
		foundOffset = offset
		foundLength = length
		foundHigh = high
		a.fl.setFree(foundOffset, foundHigh)
		offset += length
	}
	a.fl.setAlloc(foundOffset, foundHigh)
	offset = foundOffset
	length = foundLength
	a.stats.AllocatedSize += length
	if block {
		a.stats.RequestedSize += length
		a.mu.Unlock()
		return a.buf[offset : offset+length]
	}
	a.stats.RequestedSize += size
	a.mu.Unlock()
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
	ptrHigh := HighBit(ptrSize - 1)
	ptrLength := 1 << uint(ptrHigh)
	offset := ptrOffset
	length := ptrLength
	high := ptrHigh
	a.fl.setFree(offset, high)
	b := true
	for b {
		b = false
		if (offset/length)%2 == 0 {
			n := offset + length
			l := a.fl.getFree(n)
			if l >= 0 && 1<<uint(l) == length {
				a.fl.del(n)
				length <<= 1
				high++
				a.fl.setFree(offset, high)
				b = true
			}
		} else {
			n := offset - length
			l := a.fl.getFree(n)
			if l >= 0 && 1<<uint(l) == length {
				a.fl.del(offset)
				offset = n
				length <<= 1
				high++
				a.fl.setFree(offset, high)
				b = true
			}
		}
	}
	a.stats.AllocatedSize -= ptrLength
	a.stats.RequestedSize -= ptrSize
	a.mu.Unlock()
}

func (a *Arena) Stats() (stats ArenaStats) {
	a.mu.Lock()
	stats = a.stats
	a.mu.Unlock()
	return
}

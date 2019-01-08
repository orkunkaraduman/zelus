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
		fl:  newFreeList(),
	}
	a.fl.add(h, 0)
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
	offset, length, high, idx := -1, int(^uint(0)>>1), -1, -1
	for h := HighBit(length - 1); h >= sizeHigh; h-- {
		i := a.fl.first(h)
		if i >= 0 {
			offset = a.fl.get(h, i)
			length = 1 << uint(h)
			high = h
			idx = i
		}
	}
	if offset < 0 {
		a.mu.Unlock()
		return nil
	}
	foundOffset := offset
	foundLength := length
	foundHigh := high
	foundIdx := idx
	a.fl.del(foundHigh, foundIdx)
	foundIdx = -1
	ending := offset + length
	for offset < ending {
		if high > sizeHigh && high > 8 {
			high--
			length >>= 1
		}
		if length < size {
			break
		}
		foundOffset = offset
		foundLength = length
		foundHigh = high
		foundIdx = a.fl.add(high, offset)
		offset += length
	}
	if foundIdx >= 0 {
		a.fl.del(foundHigh, foundIdx)
	}
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
	a.fl.add(high, offset)
	b := true
	for b {
		b = false
		if (offset/length)%2 == 0 {
			n := offset + length
			idx := a.fl.find(high, n)
			if idx >= 0 {
				a.fl.del(high, idx)
				idx = a.fl.find(high, offset)
				a.fl.del(high, idx)
				length <<= 1
				high++
				a.fl.add(high, offset)
				b = true
			}
		} else {
			n := offset - length
			idx := a.fl.find(high, n)
			if idx >= 0 {
				a.fl.del(high, idx)
				idx = a.fl.find(high, offset)
				a.fl.del(high, idx)
				offset = n
				length <<= 1
				high++
				a.fl.add(high, offset)
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

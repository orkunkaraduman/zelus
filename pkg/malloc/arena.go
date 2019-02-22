package malloc

import (
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

type Arena struct {
	buf        []byte
	fl         *freeList
	flMu       []sync.Mutex
	flMuHigh   int
	flMuLength int
	stats      ArenaStats
}

type ArenaStats struct {
	TotalSize     int64
	AllocatedSize int64
	RequestedSize int64
}

func NewArena(buf []byte) *Arena {
	totalSize := len(buf)
	if totalSize <= 0 {
		return nil
	}
	h := highBit(totalSize - 1)
	if totalSize != 1<<uint(h) {
		panic(ErrSizeMustBePowerOfTwo)
	}
	if h < minHigh {
		panic(ErrSizeMustBeGEMinLength)
	}
	flMuHigh := h - 6
	if flMuHigh < 26 {
		flMuHigh = 26
	}
	flMuLength := 1 << uint(flMuHigh)
	a := &Arena{
		buf:        buf,
		fl:         newFreeList(totalSize),
		flMu:       make([]sync.Mutex, (totalSize-1)/flMuLength+1),
		flMuHigh:   flMuHigh,
		flMuLength: flMuLength,
	}
	a.fl.setFree(0, h)
	a.stats.TotalSize = int64(totalSize)
	return a
}

func AllocArena(totalSize int) *Arena {
	if totalSize <= 0 {
		panic(ErrSizeMustBePositive)
	}
	h := highBit(totalSize) - 1
	if totalSize != 1<<uint(h) {
		panic(ErrSizeMustBePowerOfTwo)
	}
	if h < minHigh {
		panic(ErrSizeMustBeGEMinLength)
	}
	pagesize := os.Getpagesize()
	buf := make([]byte, totalSize)
	for i, j := 0, len(buf); i < j; i += pagesize {
		buf[i] = 0
	}
	return NewArena(buf)
}

func (a *Arena) Close() {
	atomic.StoreInt32(&a.fl.done, 1)
	select {
	case a.fl.dispatcherCh <- struct{}{}:
	default:
	}
}

func (a *Arena) BlockSize() int {
	return minLength
}

func (a *Arena) flLock(offset, length int) {
	for i, j := offset/a.flMuLength, (offset+length-1)/a.flMuLength; i <= j; i++ {
		a.flMu[i].Lock()
	}
}

func (a *Arena) flUnlock(offset, length int) {
	for i, j := offset/a.flMuLength, (offset+length-1)/a.flMuLength; i <= j; i++ {
		a.flMu[i].Unlock()
	}
}

func (a *Arena) alloc(size int, block bool) []byte {
	if size <= 0 {
		return nil
	}
	sizeHigh := highBit(size - 1)
	foundOffset, foundLength, foundHigh := -1, 0, -1
	offset, length, high := -1, 1<<minHigh, minHigh
	if sizeHigh > high {
		length = 1 << uint(sizeHigh)
		high = sizeHigh
	}
	flLockOffset, flLockLength := -1, -1
	for foundOffset < 0 && high <= maxHigh {
		select {
		case offset = <-a.fl.queue[high-minHigh]:
			h := a.fl.getFree(offset)
			if h >= high {
				l := 1 << uint(h)
				flLockOffset, flLockLength = offset, l
				a.flLock(flLockOffset, flLockLength)
				if a.fl.getFree(offset) != h {
					a.flUnlock(flLockOffset, flLockLength)
					continue
				}
				foundOffset = offset
				foundLength = l
				foundHigh = h
			}
		default:
			length <<= 1
			high++
		}
	}
	if foundOffset < 0 {
		return nil
	}
	a.fl.del(foundOffset)
	offset = foundOffset + foundLength
	length = foundLength
	high = foundHigh
	starting := foundOffset
	for offset > starting {
		if high > sizeHigh && high > minHigh {
			high--
			length >>= 1
		}
		offset -= length
		foundOffset = offset
		foundLength = length
		foundHigh = high
		a.fl.setFree(foundOffset, foundHigh)
	}
	a.fl.setAlloc(foundOffset, foundHigh)
	offset = foundOffset
	length = foundLength
	a.flUnlock(flLockOffset, flLockLength)
	atomic.AddInt64(&a.stats.AllocatedSize, int64(length))
	if block {
		atomic.AddInt64(&a.stats.RequestedSize, int64(length))
		return a.buf[offset : offset+length]
	}
	atomic.AddInt64(&a.stats.RequestedSize, int64(size))
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
	ptrHigh := highBit(ptrSize - 1)
	if ptrHigh < minHigh {
		ptrHigh = minHigh
	}
	ptrLength := 1 << uint(ptrHigh)
	offset := ptrOffset
	length := ptrLength
	high := ptrHigh
	flLockOffset, flLockLength := offset, length
	a.flLock(flLockOffset, flLockLength)
	if a.fl.getAlloc(offset) != high {
		a.flUnlock(flLockOffset, flLockLength)
		panic(ErrInvalidPointer)
	}
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
	a.flUnlock(flLockOffset, flLockLength)
	atomic.AddInt64(&a.stats.AllocatedSize, -int64(ptrLength))
	atomic.AddInt64(&a.stats.RequestedSize, -int64(ptrSize))
}

func (a *Arena) Stats() (stats ArenaStats) {
	stats = a.stats
	return
}

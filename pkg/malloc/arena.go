package malloc

import (
	"runtime"
	"sync"
	"time"
	"unsafe"
)

type Arena struct {
	buf   []byte
	fl    *freeList
	flMu  []sync.Mutex
	stats ArenaStats
}

const (
	flMuHigh   = 30
	flMuLength = 1 << flMuHigh
)

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
	if h < minHigh {
		panic(ErrSizeMustBeGEMinLength)
	}
	a := &Arena{
		buf:  buf,
		fl:   newFreeList(totalSize),
		flMu: make([]sync.Mutex, (totalSize-1)/flMuLength+1),
	}
	a.fl.setFree(0, h)
	a.stats.TotalSize = totalSize
	go a.dispatch(0, totalSize)
	runtime.Gosched()
	return a
}

func AllocArena(totalSize int) *Arena {
	if totalSize <= 0 {
		panic(ErrSizeMustBePositive)
	}
	h := HighBit(totalSize) - 1
	if totalSize != 1<<uint(h) {
		panic(ErrSizeMustBePowerOfTwo)
	}
	if h < minHigh {
		panic(ErrSizeMustBeGEMinLength)
	}
	buf := make([]byte, totalSize)
	for i, j := 0, len(buf); i < j; i += 1024 {
		buf[i] = 0
	}
	return NewArena(buf)
}

func (a *Arena) dispatch(start, end int) {
	for {
		offset, length, high := start, 1<<minHigh, minHigh
		for offset < end {
			high = a.fl.get(offset)
			if high < 0 {
				offset += length
				continue
			}
			allocated := high&freeListAlloc != 0
			high &= 0x3f
			length = 1 << uint(high)
			c := a.fl.queue[high-minHigh]
			if !allocated {
				select {
				case c <- offset:
				default:
				}
			}
			offset += length
			if len(c) >= cap(c) {
				time.Sleep(5 * time.Millisecond)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (a *Arena) flLock(offset, length int) {
	for i, j := offset/flMuLength, (offset+length-1)/flMuLength; i <= j; i++ {
		a.flMu[i].Lock()
	}
}

func (a *Arena) flUnlock(offset, length int) {
	for i, j := offset/flMuLength, (offset+length-1)/flMuLength; i <= j; i++ {
		a.flMu[i].Unlock()
	}
}

func (a *Arena) alloc(size int, block bool) []byte {
	if size <= 0 {
		return nil
	}
	sizeHigh := HighBit(size - 1)
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
				h2 := a.fl.getFree(offset)
				if h != h2 {
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
	a.stats.AllocatedSize += length
	if block {
		a.stats.RequestedSize += length
		a.flUnlock(flLockOffset, flLockLength)
		return a.buf[offset : offset+length]
	}
	a.stats.RequestedSize += size
	a.flUnlock(flLockOffset, flLockLength)
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
	ptrHigh := HighBit(ptrSize - 1)
	if ptrHigh < minHigh {
		ptrHigh = minHigh
	}
	ptrLength := 1 << uint(ptrHigh)
	offset := ptrOffset
	length := ptrLength
	high := ptrHigh
	flLockOffset, flLockLength := offset, length
	a.flLock(flLockOffset, flLockLength)
	h := a.fl.getAlloc(offset)
	if h != high {
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
	a.stats.AllocatedSize -= ptrLength
	a.stats.RequestedSize -= ptrSize
}

func (a *Arena) Stats() (stats ArenaStats) {
	//a.flMu.Lock()
	stats = a.stats
	//a.flMu.Unlock()
	return
}

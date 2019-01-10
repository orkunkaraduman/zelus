package malloc

import (
	"runtime"
	"unsafe"
)

type Arena struct {
	//mu    sync.Mutex
	mu    chan struct{}
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
		mu:  make(chan struct{}, 1),
		buf: buf,
		fl:  newFreeList(totalSize),
	}
	a.fl.setFree(0, h)
	a.stats.TotalSize = totalSize
	go a.dispatch()
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

func (a *Arena) dispatch() {
	for {
		offset, length, high := 0, 1<<minHigh, minHigh
		for offset < len(a.buf) {
			high = a.fl.get(offset)
			if high < 0 {
				offset += length
				continue
			}
			allocated := high&freeListAlloc != 0
			high &= 0x3f
			length = 1 << uint(high)
			if !allocated {
				select {
				case a.fl.queue[high-minHigh] <- offset:
				default:
				}
			}
			offset += length
		}
		//time.Sleep(50 * time.Millisecond)
		runtime.Gosched()
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
	//a.mu.Lock()
	for foundOffset < 0 && high <= maxHigh {
		select {
		case offset = <-a.fl.queue[high-minHigh]:
			h := a.fl.getFree(offset)
			if h >= high {
				//a.mu.Lock()
				a.mu <- struct{}{}
				h2 := a.fl.getFree(offset)
				if h != h2 {
					<-a.mu
					continue
				}
				foundOffset = offset
				foundLength = 1 << uint(h)
				foundHigh = h
			}
		default:
			length <<= 1
			high++
		}
	}
	if foundOffset < 0 {
		//a.mu.Unlock()
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
		<-a.mu
		return a.buf[offset : offset+length]
	}
	a.stats.RequestedSize += size
	<-a.mu
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
	//a.mu.Lock()
	a.mu <- struct{}{}
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
	//a.mu.Unlock()
	<-a.mu
	a.stats.AllocatedSize -= ptrLength
	a.stats.RequestedSize -= ptrSize
}

func (a *Arena) Stats() (stats ArenaStats) {
	//a.mu.Lock()
	a.mu <- struct{}{}
	stats = a.stats
	//a.mu.Unlock()
	<-a.mu
	return
}

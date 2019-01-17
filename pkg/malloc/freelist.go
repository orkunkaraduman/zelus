package malloc

import (
	"sync/atomic"
	"unsafe"
)

type freeList struct {
	size       int
	list       []uint8
	queue      []chan int
	dispatchCh chan struct{}
}

const (
	freeListEmpty = 1 << 7
	freeListAlloc = 1 << 6
)

func newFreeList(size int) *freeList {
	count := (size-1)/MinLength + 1
	f := &freeList{
		size:       size,
		list:       make([]uint8, count, count+4),
		queue:      make([]chan int, MaxHigh-MinHigh+1),
		dispatchCh: make(chan struct{}),
	}
	for i := range f.list {
		f.list[i] = 0xff
	}
	for i := range f.queue {
		f.queue[i] = make(chan int, 4*1024)
	}
	go f.dispatch()
	return f
}

func (f *freeList) filledCount() int {
	r := 0
	for _, v := range f.list {
		if v&freeListEmpty == 0 {
			r++
		}
	}
	return r
}

func (f *freeList) filledSize() int {
	r := 0
	for _, v := range f.list {
		if v&freeListEmpty == 0 {
			r += 1 << (v & 0x3f)
		}
	}
	return r
}

func (f *freeList) dispatch() {
	for {
		<-f.dispatchCh
		offset, length, high := 0, 1<<MinHigh, MinHigh
		for offset < f.size {
			high = f.get(offset)
			if high < 0 {
				offset += length
				continue
			}
			allocated := high&freeListAlloc != 0
			high &= 0x3f
			length = 1 << uint(high)
			c := f.queue[high-MinHigh]
			if !allocated {
				select {
				case c <- offset:
				default:
				}
			}
			offset += length
			/*if len(c) >= cap(c) {
				time.Sleep(5 * time.Millisecond)
			}*/
		}
		//time.Sleep(5 * time.Millisecond)
	}
}

func (f *freeList) getFree(offset int) int {
	i := offset / MinLength
	if i >= len(f.list) {
		return -1
	}
	v := uint8(atomic.LoadUint32((*uint32)(unsafe.Pointer(&f.list[i]))))
	if v&freeListEmpty != 0 || v&freeListAlloc != 0 {
		return -1
	}
	return int(v & 0x3f)
}

func (f *freeList) setFree(offset int, high int) {
	i := offset / MinLength
	v := uint8(high & 0x3f)
	f.list[i] = v
	select {
	case f.queue[v-MinHigh] <- offset:
	default:
		select {
		case f.dispatchCh <- struct{}{}:
		default:
		}
	}
}

func (f *freeList) getAlloc(offset int) int {
	i := offset / MinLength
	if i >= len(f.list) {
		return -1
	}
	v := uint8(atomic.LoadUint32((*uint32)(unsafe.Pointer(&f.list[i]))))
	if v&freeListEmpty != 0 || v&freeListAlloc == 0 {
		return -1
	}
	return int(v & 0x3f)
}

func (f *freeList) setAlloc(offset int, high int) {
	i := offset / MinLength
	v := uint8(high & 0x3f)
	f.list[i] = v | freeListAlloc
}

func (f *freeList) get(offset int) int {
	i := offset / MinLength
	if i >= len(f.list) {
		return -1
	}
	v := uint8(atomic.LoadUint32((*uint32)(unsafe.Pointer(&f.list[i]))))
	if v&freeListEmpty != 0 {
		return -1
	}
	return int(v & 0x7f)
}

func (f *freeList) del(offset int) {
	i := offset / MinLength
	f.list[i] = 0xff
}

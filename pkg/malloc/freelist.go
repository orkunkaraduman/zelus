package malloc

import (
	"sync/atomic"
	"unsafe"
)

type freeList struct {
	size  int
	list  []uint8
	queue []chan int
}

const (
	freeListEmpty = 1 << 7
	freeListAlloc = 1 << 6
)

func newFreeList(size int) *freeList {
	count := size / minLength
	f := &freeList{
		size:  size,
		list:  make([]uint8, count, count+4),
		queue: make([]chan int, maxHigh-minHigh+1),
	}
	for i := range f.list {
		f.list[i] = 0xff
	}
	for i := range f.queue {
		f.queue[i] = make(chan int, 16*1024)
	}
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

func (f *freeList) getFree(offset int) int {
	i := offset / minLength
	if i >= len(f.list) {
		return -1
	}
	v := f.list[i]
	if v&freeListEmpty != 0 || v&freeListAlloc != 0 {
		return -1
	}
	return int(v & 0x3f)
}

func (f *freeList) setFree(offset int, high int) {
	i := offset / minLength
	v := uint8(high & 0x3f)
	f.list[i] = v
	select {
	case f.queue[v-minHigh] <- offset:
	default:
	}
}

func (f *freeList) getAlloc(offset int) int {
	i := offset / minLength
	if i >= len(f.list) {
		return -1
	}
	v := f.list[i]
	if v&freeListEmpty != 0 || v&freeListAlloc == 0 {
		return -1
	}
	return int(v & 0x3f)
}

func (f *freeList) setAlloc(offset int, high int) {
	i := offset / minLength
	v := uint8(high & 0x3f)
	f.list[i] = v | freeListAlloc
}

func (f *freeList) get(offset int) int {
	i := offset / minLength
	if i >= len(f.list) {
		return -1
	}
	//v := f.list[i]
	v := uint8(atomic.LoadUint32((*uint32)(unsafe.Pointer(&f.list[i]))))
	if v&freeListEmpty != 0 {
		return -1
	}
	return int(v & 0x7f)
}

func (f *freeList) del(offset int) {
	i := offset / minLength
	f.list[i] = 0xff
}

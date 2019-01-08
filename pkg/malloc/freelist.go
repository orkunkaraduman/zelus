package malloc

import (
	"unsafe"
)

type freeList struct {
	list [][]int
}

func newFreeList() *freeList {
	var zeroInt int
	return &freeList{
		list: make([][]int, unsafe.Sizeof(zeroInt)*8),
	}
}

func (f *freeList) find(offset, lengthHigh int) int {
	for i, v := range f.list[lengthHigh] {
		if v == offset {
			return i
		}
	}
	return -1
}

func (f *freeList) first(lengthHigh int) int {
	for i, v := range f.list[lengthHigh] {
		if v >= 0 {
			return i
		}
	}
	return -1
}

func (f *freeList) get(idx int, lengthHigh int) int {
	return f.list[lengthHigh][idx]
}

func (f *freeList) set(idx int, offset int, lengthHigh int) {
	f.list[lengthHigh][idx] = offset
}

func (f *freeList) del(idx int, lengthHigh int) {
	f.list[lengthHigh][idx] = -1
	for i := len(f.list[lengthHigh]) - 1; i >= 0 && f.list[lengthHigh][i] < 0; i-- {
		f.list[lengthHigh] = f.list[lengthHigh][:i]
	}
}

func (f *freeList) append(offset int, lengthHigh int) int {
	f.list[lengthHigh] = append(f.list[lengthHigh], offset)
	return len(f.list[lengthHigh]) - 1
}

func (f *freeList) add(offset int, lengthHigh int) int {
	idx := f.find(-1, lengthHigh)
	if idx < 0 {
		return f.append(offset, lengthHigh)
	}
	f.list[lengthHigh][idx] = offset
	return idx
}

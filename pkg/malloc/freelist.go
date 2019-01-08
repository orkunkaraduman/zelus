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

func (f *freeList) size() int {
	r := 0
	for _, l := range f.list {
		for _, o := range l {
			if o >= 0 {
				r++
			}
		}
	}
	return r
}

func (f *freeList) find(h, v int) int {
	for i, u := range f.list[h] {
		if u == v {
			return i
		}
	}
	return -1
}

func (f *freeList) first(h int) int {
	for i, u := range f.list[h] {
		if u >= 0 {
			return i
		}
	}
	return -1
}

func (f *freeList) get(h, i int) int {
	return f.list[h][i]
}

func (f *freeList) set(h, i int, v int) {
	f.list[h][i] = v
}

func (f *freeList) del(h, i int) {
	f.list[h][i] = -1
	m := -1
	for j := len(f.list[h]) - 1; j >= 0 && f.list[h][j] < 0; j-- {
		m = j
	}
	if m >= 0 {
		f.list[h] = f.list[h][:m]
	}
}

func (f *freeList) append(h int, v int) int {
	f.list[h] = append(f.list[h], v)
	return len(f.list[h]) - 1
}

func (f *freeList) add(h int, v int) int {
	i := f.find(h, -1)
	if i < 0 {
		return f.append(h, v)
	}
	f.list[h][i] = v
	return i
}

package malloc

type freeList struct {
	size int
	list []uint8
}

const (
	freeListEmpty = 1 << 7
	freeListAlloc = 1 << 6
)

func newFreeList(size int) *freeList {
	count := size / minLength
	f := &freeList{
		size: size,
		list: make([]uint8, count),
	}
	for i := range f.list {
		f.list[i] = 0xff
	}
	return f
}

func (f *freeList) filledCount() int {
	r := 0
	for _, v := range f.list {
		if v != 0xff {
			r++
		}
	}
	return r
}

func (f *freeList) filledSize() int {
	r := 0
	for _, v := range f.list {
		if v != 0xff {
			r += 1 << v
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
	f.list[i] = uint8(high & 0x3f)
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
	f.list[i] = uint8((high & 0x3f) | freeListAlloc)
}

func (f *freeList) get(offset int) int {
	i := offset / minLength
	if i >= len(f.list) {
		return -1
	}
	v := f.list[i]
	if v&freeListEmpty != 0 {
		return -1
	}
	return int(v & 0x7f)
}

func (f *freeList) del(offset int) {
	i := offset / minLength
	f.list[i] = 0xff
}

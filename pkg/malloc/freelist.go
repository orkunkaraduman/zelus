package malloc

type freeList struct {
	list []uint8
}

const minHigh = 8
const minLength = 1 << minHigh

func newFreeList(size int) *freeList {
	count := size / minLength
	f := &freeList{
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

func (f *freeList) get(offset int) int {
	i := offset / minLength
	if i >= len(f.list) {
		return -1
	}
	v := f.list[i]
	if v == 0xff {
		return -1
	}
	return int(v)
}

func (f *freeList) set(offset int, high int) {
	i := offset / minLength
	f.list[i] = uint8(high)
}

func (f *freeList) del(offset int) {
	i := offset / minLength
	f.list[i] = 0xff
}

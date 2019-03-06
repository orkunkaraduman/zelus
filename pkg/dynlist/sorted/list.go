package sorted

import (
	"sort"

	"github.com/orkunkaraduman/zelus/pkg/dynlist"
)

type List struct {
	lf dynlist.LessFunc
	dl *dynlist.DynList
}

func NewList(lf dynlist.LessFunc) (sl *List) {
	if lf == nil {
		return
	}
	sl = &List{
		lf: lf,
		dl: dynlist.New(16, lf),
	}
	return
}

func (sl *List) Len() int {
	return sl.dl.Len()
}

func (sl *List) Push(x ...interface{}) (count int) {
	for _, x := range x {
		if x != nil {
			sl.dl.Push(x)
			count++
		}
	}
	sort.Sort(sl.dl)
	return
}

func (sl *List) Pop() interface{} {
	if 0 >= sl.dl.Len() {
		return nil
	}
	return sl.dl.Pop()
}

func (sl *List) Get(i int) interface{} {
	if !(i >= 0 && i < sl.dl.Len()) {
		return nil
	}
	return sl.dl.Get(i)
}

func (sl *List) Remove(i ...int) (count int) {
	l := sl.dl.Len()
	for _, i := range i {
		if i >= 0 && i < l && sl.dl.Get(i) != nil {
			sl.dl.Set(i, nil)
			count++
		}
	}
	sort.Sort(sl.dl)
	for i := 0; i < count; i++ {
		sl.dl.Pop()
	}
	return
}

func (sl *List) Search(start int, x interface{}) int {
	l := sl.dl.Len()
	if !(start >= 0 && start < l) || x == nil {
		return -1
	}
	n := l - start
	i := sort.Search(n, func(i int) bool {
		return !sl.lf(sl.Get(start+i), x)
	})
	if !(i >= 0 && i < n) {
		return -1
	}
	i += start
	if sl.Get(i) != x {
		return -1
	}
	return i
}

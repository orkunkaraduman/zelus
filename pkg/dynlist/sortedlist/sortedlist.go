package sortedlist

import (
	"sort"

	"github.com/orkunkaraduman/zelus/pkg/dynlist"
)

type SortedList struct {
	lf dynlist.LessFunc
	dl *dynlist.DynList
}

func New(lf dynlist.LessFunc) (sl *SortedList) {
	if lf == nil {
		return
	}
	sl = &SortedList{
		lf: lf,
		dl: dynlist.New(16, lf),
	}
	return
}

func (sl *SortedList) Len() int {
	return sl.dl.Len()
}

func (sl *SortedList) Push(x ...interface{}) (count int) {
	for _, x := range x {
		if x != nil {
			sl.dl.Push(x)
			count++
		}
	}
	sort.Sort(sl.dl)
	return
}

func (sl *SortedList) Pop() interface{} {
	if 0 >= sl.dl.Len() {
		return nil
	}
	return sl.dl.Pop()
}

func (sl *SortedList) Get(i int) interface{} {
	if !(i >= 0 && i < sl.dl.Len()) {
		return nil
	}
	return sl.dl.Get(i)
}

func (sl *SortedList) Del(i ...int) (count int) {
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

func (sl *SortedList) Search(start int, x interface{}) int {
	l := sl.dl.Len()
	if !(start >= 0 && start < l) {
		return -1
	}
	n := l - start
	i := sort.Search(n, func(i int) bool {
		return !sl.lf(sl.Get(start+i), x)
	})
	if !(i >= 0 && i < n) {
		return -1
	}
	return start + i
}

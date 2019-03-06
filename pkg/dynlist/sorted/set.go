package sorted

import "github.com/orkunkaraduman/zelus/pkg/dynlist"

type Set struct {
	*List
}

func NewSet(lf dynlist.LessFunc) (ss *Set) {
	sl := NewList(lf)
	if sl == nil {
		return
	}
	ss = &Set{
		List: sl,
	}
	return
}

func (ss *Set) Push(x ...interface{}) (count int) {
	isIn := func(v []interface{}, x interface{}) bool {
		for _, y := range v {
			if x == y {
				return true
			}
		}
		return false
	}
	for i := range x {
		if isIn(x[:i], x[i]) || ss.Search(0, x[i]) >= 0 {
			x[i] = nil
		}
	}
	return ss.List.Push(x...)
}

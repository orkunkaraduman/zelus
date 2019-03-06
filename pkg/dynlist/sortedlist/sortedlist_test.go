package sortedlist

import (
	"math/rand"
	"testing"

	"github.com/orkunkaraduman/zelus/pkg/dynlist"
)

func TestSortedList1(t *testing.T) {
	valueCount := 100
	rnd := rand.New(rand.NewSource(1))
	rMin, rMax := -100, +100
	sl := New(dynlist.LessFuncInt)
	for i := 0; i < valueCount; i++ {
		sl.Push(rMin + rnd.Intn(rMax-rMin+1))
	}
	if sl.Len() != valueCount {
		t.FailNow()
	}

	idxs := make([]int, 0, 10)
	for i := 0; i < cap(idxs); i++ {
		idxs = append(idxs, rnd.Intn(valueCount))
	}
	valueCount -= sl.Del(idxs...)
	if sl.Len() != valueCount {
		t.FailNow()
	}

	var lastValue int
	var first bool

	first = true
	for i := 0; i < valueCount; i++ {
		x := sl.Get(i).(int)
		if !first && x < lastValue {
			t.FailNow()
		}
		lastValue = x
		first = false
	}

	var idx int
	i, x := 5, -85
	idx = sl.Search(0, x)
	if idx != i {
		t.FailNow()
	}
	idx = sl.Search(2, x)
	if idx != i {
		t.FailNow()
	}
	idx = sl.Search(i, x)
	if idx != i {
		t.FailNow()
	}
	idx = sl.Search(i+1, x)
	if idx != i+1 {
		t.FailNow()
	}
	idx = sl.Search(50, 200)
	if idx != -1 {
		t.FailNow()
	}

	first = true
	for i := 0; i < valueCount; i++ {
		x := sl.Pop().(int)
		if !first && x > lastValue {
			t.FailNow()
		}
		lastValue = x
		first = false
	}
	if sl.Len() != 0 {
		t.FailNow()
	}
}

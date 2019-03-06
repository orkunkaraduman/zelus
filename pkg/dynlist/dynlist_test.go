package dynlist

import (
	"container/heap"
	"math/rand"
	"testing"
)

func TestDynListWithHeap(t *testing.T) {
	dl := New(1, LessFuncInt)
	heap.Init(dl)

	rnd := rand.New(rand.NewSource(1))

	valueCount := 100000
	for i := 0; i < valueCount; i++ {
		heap.Push(dl, rnd.Int())
	}
	if dl.Len() != valueCount {
		t.FailNow()
	}

	var lastValue int
	var first bool

	first = true
	for i := 0; i < valueCount; i++ {
		x := heap.Pop(dl).(int)
		if !first && x < lastValue {
			t.FailNow()
		}
		lastValue = x
		first = false
	}

	if dl.Len() != 0 {
		t.FailNow()
	}
}

package sorted

import (
	"math/rand"
	"testing"

	"github.com/orkunkaraduman/zelus/pkg/dynlist"
)

func TestList1(t *testing.T) {
	valueCount := 100
	rnd := rand.New(rand.NewSource(1))
	rMin, rMax := -100, +100
	sl := NewList(dynlist.LessFuncInt)
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
	valueCount -= sl.Remove(idxs...)
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
	idx = sl.Search(0, -200)
	if idx != -1 {
		t.FailNow()
	}
	idx = sl.Search(0, 200)
	if idx != -1 {
		t.FailNow()
	}
	idx = sl.Search(50, -200)
	if idx != -1 {
		t.FailNow()
	}
	idx = sl.Search(50, 200)
	if idx != -1 {
		t.FailNow()
	}
	idx = sl.Search(50, nil)
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

func benchmarkListPush(b *testing.B, n int) {
	rnd := rand.New(rand.NewSource(1))
	sl := NewList(dynlist.LessFuncInt)
	rnds := make([]interface{}, 0, b.N)
	for i := 0; i < b.N; i++ {
		rnds = append(rnds, rnd.Intn(1000))
	}
	b.ResetTimer()
	for i := 0; i < b.N; {
		l := b.N
		if l-i > n {
			l = i + n
		}
		sl.Push(rnds[i:l]...)
		i = l
	}
	if sl.Len() != b.N {
		b.FailNow()
	}
}

func BenchmarkListPush(b *testing.B) {
	benchmarkListPush(b, b.N)
}

func BenchmarkListPush1(b *testing.B) {
	benchmarkListPush(b, 1)
}

func BenchmarkListPush16(b *testing.B) {
	benchmarkListPush(b, 16)
}

func BenchmarkListPush256(b *testing.B) {
	benchmarkListPush(b, 256)
}

package sorted

import (
	"math/rand"
	"testing"

	"github.com/orkunkaraduman/zelus/pkg/dynlist"
)

func TestSet1(t *testing.T) {
	ss := NewSet(dynlist.LessFuncInt)
	ss.Push(6, 6)
	ss.Push(5)
	ss.Push(5, 5)
	ss.Push(7)
	if ss.Len() != 3 || ss.Get(0) != 5 || ss.Get(1) != 6 {
		t.FailNow()
	}
}

func benchmarkSetPush(b *testing.B, n int) {
	rnd := rand.New(rand.NewSource(1))
	ss := NewSet(dynlist.LessFuncInt)
	rnds := make([]interface{}, 0, b.N)
	for i := 0; i < b.N; i++ {
		rnds = append(rnds, rnd.Intn(1000))
	}
	b.ResetTimer()
	count := 0
	for i := 0; i < b.N; {
		l := b.N
		if l-i > n {
			l = i + n
		}
		count += ss.Push(rnds[i:l]...)
		i = l
	}
	if ss.Len() != count {
		b.FailNow()
	}
}

func BenchmarkSetPush(b *testing.B) {
	benchmarkSetPush(b, 1)
}

func BenchmarkSetPush16(b *testing.B) {
	benchmarkSetPush(b, 16)
}

func BenchmarkSetPush256(b *testing.B) {
	benchmarkSetPush(b, 256)
}

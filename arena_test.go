package malloc

import (
	"math/rand"
	"testing"
	"time"
)

func TestArena(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	size := 256 * 1024 * 1024
	maxChunkSize := 500 * 1024
	a := AllocArena(size)
	ptrList := make(map[int][]byte)
	requested := 0
	i := 0
	for requested < size*60/100 {
		l := r.Intn(maxChunkSize)
		p := a.Alloc(l)
		if p == nil {
			t.Errorf("Cannot allocate %d", i)
			t.FailNow()
		}
		ptrList[i] = p
		requested += l
		i++
	}
	t.Logf("Allocated: count=%d size=%d freelist=%d", i, requested, len(a.freeList))
	if a.Stats().RequestedSize != requested {
		t.Error("Stats error")
		t.FailNow()
	}
	for _, p := range ptrList {
		a.Free(p)
	}
	if len(a.freeList) != 1 {
		t.Error("Freelist length error")
		t.FailNow()
	}
	if a.Stats().RequestedSize != 0 || a.Stats().AllocatedSize != 0 {
		t.Error("Stats error")
		t.FailNow()
	}
	t.Log("Arena OK")
}

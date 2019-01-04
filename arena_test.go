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
	a := NewArena(size)
	ptrList := make(map[int][]byte)
	allocated := 0
	i := 0
	for allocated < size*60/100 {
		l := r.Intn(maxChunkSize)
		p := a.Alloc(l)
		if p == nil {
			t.Errorf("Cannot allocate %d", i)
			t.FailNow()
		}
		ptrList[i] = p
		allocated += l
		//t.Logf("Allocated %d with len %d", i, l)
		i++
	}
	t.Logf("count=%d size=%d freelist=%d", i, allocated, len(a.freeList))
	for _, p := range ptrList {
		a.Free(p)
		//t.Logf("Deallocated %d with len %d", i, len(p))
	}
	if len(a.freeList) != 1 {
		t.Error("Cannot deallocate")
		t.FailNow()
	}
	t.Log("Arena OK")
}

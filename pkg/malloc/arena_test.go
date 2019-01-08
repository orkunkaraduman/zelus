package malloc

import (
	"math/rand"
	"testing"
	"time"
)

func TestArena(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	size := 256 * 1024 * 1024
	a := AllocArena(size)
	ptrList := make(map[int][]byte)
	requested := 0
	i := 0
	for requested < size*60/100 {
		l := r.Intn(500 * 1024)
		ptr := a.Alloc(l)
		if ptr == nil {
			t.Errorf("Cannot allocate %d", i)
			t.FailNow()
		}
		ptrList[i] = ptr
		requested += l
		i++
	}
	t.Logf("Allocated: count=%d size=%d freelist=%d", i, requested, a.fl.size())
	if a.Stats().RequestedSize != requested {
		t.Error("Stats error")
		t.FailNow()
	}
	for _, ptr := range ptrList {
		a.Free(ptr)
	}
	if a.fl.size() != 1 {
		t.Errorf("Freelist size error %d", a.fl.size())
		t.FailNow()
	}
	if a.Stats().RequestedSize != 0 || a.Stats().AllocatedSize != 0 {
		t.Error("Stats error")
		t.FailNow()
	}
	t.Log("Arena OK")
}

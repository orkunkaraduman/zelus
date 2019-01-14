package malloc

import (
	"math/rand"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	size := 500 * 1024 * 1024
	p := AllocPool(size)
	ptrList := make(map[int][]byte)
	requested := 0
	i := 0
	for requested < size*60/100 {
		l := r.Intn(500 * 1024)
		ptr := p.Alloc(l)
		if ptr == nil {
			t.Errorf("Cannot allocate %d", i)
			t.FailNow()
		}
		ptrList[i] = ptr
		requested += l
		i++
	}
	t.Logf("Allocated: count=%d size=%d", i, requested)
	if int(p.Stats().RequestedSize) != requested {
		t.Error("Stats error")
		t.FailNow()
	}
	for _, ptr := range ptrList {
		p.Free(ptr)
	}
	if p.Stats().RequestedSize != 0 || p.Stats().AllocatedSize != 0 {
		t.Error("Stats error")
		t.FailNow()
	}
	t.Log("Pool OK")
}

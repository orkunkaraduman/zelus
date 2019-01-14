package malloc

import (
	"math/rand"
	"testing"
	"time"
)

func TestArena(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	size := 256 * 1024 * 1024
	a := AllocArena(size)
	ptrList := make(map[int][]byte)
	requested := 0
	i := 0
	for requested < size*50/100 {
		l := 1 + r.Intn(500*1024)
		ptr := a.Alloc(l)
		if ptr == nil {
			t.Errorf("Cannot allocate %d %d %d", i, requested, l)
			t.FailNow()
		}
		ptrList[i] = ptr
		requested += l
		i++
	}
	t.Logf("Allocated: count=%d size=%d freelistCount=%d freelistSize=%d %d", i, requested, a.fl.filledCount(), a.fl.filledSize(), a.Stats().AllocatedSize)
	if int(a.Stats().RequestedSize) != requested {
		t.Error("Stats error")
		t.FailNow()
	}
	for i, ptr := range ptrList {
		a.Free(ptr)
		delete(ptrList, i)
	}
	t.Logf("Freelist %d %d", a.fl.filledCount(), a.fl.filledSize())
	if a.fl.filledCount() != 1 {
		t.Errorf("Freelist count error %d %d", a.fl.filledCount(), a.fl.filledSize())
		t.FailNow()
	}
	if a.Stats().RequestedSize != 0 || a.Stats().AllocatedSize != 0 {
		t.Error("Stats error")
		t.FailNow()
	}
	t.Log("Arena OK")
}

/*func TestArena2(t *testing.T) {
	a := AllocArena(512 * 1024 * 1024)
	ptr1 := a.Alloc(500 * 1024)
	ptr2 := a.Alloc(512)
	ptr3 := a.Alloc(256)
	a.Free(ptr1)
	a.Free(ptr2)
	a.Free(ptr3)
}*/

package sorted

import (
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

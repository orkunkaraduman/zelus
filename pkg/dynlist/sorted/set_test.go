package sorted

import (
	"testing"

	"github.com/orkunkaraduman/zelus/pkg/dynlist"
)

func TestSet1(t *testing.T) {
	ss := NewSet(dynlist.LessFuncInt)
	ss.Push(5)
	ss.Push(5, 5)
	if ss.Len() != 1 || ss.Get(0) != 5 {
		t.FailNow()
	}
}

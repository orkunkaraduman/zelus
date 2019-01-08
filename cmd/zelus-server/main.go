package main

import (
	"fmt"

	"github.com/orkunkaraduman/zelus/pkg/store"
)

func main() {
	st := store.New(1000, 1*1024*1024)
	key := "aaa"
	val := []byte("bbb")
	if !st.Set(key, val, false) {
		panic("1")
	}
	if !st.Append(key, []byte("12345")) {
		panic("1")
	}
	val = st.Get(key)
	if val == nil {
		panic("2")
	}

	if !st.Set(key, []byte("0987676"), true) {
		//panic("1")
	}
	val = st.Get(key)
	if val == nil {
		panic("2")
	}

	fmt.Println(string(val))
}

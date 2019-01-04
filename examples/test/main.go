package main

import (
	"fmt"

	malloc "github.com/orkunkaraduman/go-malloc"
)

func main() {
	p := malloc.NewPool(16)
	p.Grow(5)
	ptr1 := p.Alloc(16)
	ptr2 := p.Alloc(4)
	p.Free(ptr1)
	p.Free(ptr2)
	fmt.Println(p.Stats())
}

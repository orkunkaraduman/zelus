package main

import (
	"fmt"

	malloc "github.com/orkunkaraduman/go-malloc"
)

func main() {
	a := malloc.NewArena(16)

	p1 := a.Alloc(15)
	p2 := a.Alloc(2)
	fmt.Println(p2)
	//p3 := a.Alloc(2)

	//a.Free(p3)
	a.Free(p2)
	a.Free(p1)
}

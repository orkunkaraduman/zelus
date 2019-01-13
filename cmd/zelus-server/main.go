package main

import (
	"github.com/orkunkaraduman/zelus/pkg/server"
	"github.com/orkunkaraduman/zelus/pkg/store"
)

func main() {
	st := store.New(1000, 1*1024*1024)
	srv := server.New(st)
	srv.ListenAndServe("0.0.0.0:1234")
}

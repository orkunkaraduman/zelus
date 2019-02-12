package store

import "errors"

var (
	ErrConsistency = errors.New("consistency error")
)

func panicConsistency() {
	panic(ErrConsistency)
}

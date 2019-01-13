package malloc

import "errors"

var (
	ErrSizeMustBePowerOfTwo  = errors.New("the size must be power of two")
	ErrSizeMustBePositive    = errors.New("the size must be greater than zero")
	ErrSizeMustBeGEMinLength = errors.New("the size must be equal or greater than 256")
	ErrInvalidPointer        = errors.New("invalid pointer")
)

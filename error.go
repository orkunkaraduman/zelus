package malloc

import "errors"

var (
	ErrSizeMustBePowerOfTwo = errors.New("the size must be power of two")
	ErrSizeMustBePositive   = errors.New("the size must be greater than zero")
)

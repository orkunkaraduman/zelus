package malloc

import "errors"

var (
	ErrPowerOfTwo = errors.New("the size must be power of two")
)

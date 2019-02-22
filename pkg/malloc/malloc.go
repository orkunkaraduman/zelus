package malloc

const (
	minHigh   = 4
	minLength = 1 << minHigh
	maxHigh   = 62
	maxLength = 1 << maxHigh
)

const (
	BlockSize = minLength
)

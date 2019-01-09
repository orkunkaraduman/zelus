package malloc

const minHigh = 8
const minLength = 1 << minHigh

func HighBit(x int) int {
	r := 0
	for x != 0 {
		x >>= 1
		r++
	}
	return r
}

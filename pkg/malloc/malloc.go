package malloc

const MinHigh = 4
const MinLength = 1 << MinHigh
const MaxHigh = 62
const MaxLength = 1 << MaxHigh

func HighBit(x int) int {
	r := 0
	for x != 0 {
		x >>= 1
		r++
	}
	return r
}

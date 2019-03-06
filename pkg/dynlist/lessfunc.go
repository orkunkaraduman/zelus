package dynlist

type LessFunc func(x, y interface{}) bool

func LessFuncInt(x, y interface{}) bool {
	return x.(int) < y.(int)
}

func LessFuncString(x, y interface{}) bool {
	return x.(string) < y.(string)
}

package dynlist

import "reflect"

type LessFunc func(x, y interface{}) bool

func LessFuncInt(x, y interface{}) bool {
	return x.(int) < y.(int)
}

func LessFuncString(x, y interface{}) bool {
	return x.(string) < y.(string)
}

func LessFuncGeneric(x, y interface{}) bool {
	if reflect.TypeOf(x) != reflect.TypeOf(y) {
		return false
	}
	switch x.(type) {
	case int:
		return LessFuncInt(x, y)
	case string:
		return LessFuncString(x, y)
	}
	return false
}

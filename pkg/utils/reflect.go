package utils

import (
	"reflect"
	"unsafe"
)

var (
	zeroByte   byte
	typeOfByte = reflect.TypeOf(zeroByte)
)

func ChangeSliceType(src interface{}, count int, typ reflect.Type) interface{} {
	var srcVal, dstVal reflect.Value
	srcVal = reflect.ValueOf(src)
	dstVal = reflect.NewAt(reflect.ArrayOf(count, typ), unsafe.Pointer(srcVal.Pointer())).Elem()
	return dstVal.Slice(0, count).Interface()
}

func StringToByteSlice(s string) []byte {
	n := (*reflect.StringHeader)(unsafe.Pointer(&s)).Len
	bVal := reflect.NewAt(reflect.ArrayOf(n, typeOfByte), unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&s)).Data)).Elem()
	return bVal.Slice(0, n).Interface().([]byte)
}

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
	sh := *(*reflect.StringHeader)(unsafe.Pointer(&s))
	bVal := reflect.NewAt(reflect.ArrayOf(sh.Len, typeOfByte), unsafe.Pointer(sh.Data)).Elem()
	return bVal.Slice(0, sh.Len).Interface().([]byte)
}

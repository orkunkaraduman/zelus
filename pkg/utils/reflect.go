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

func StringToByteSlice(src string) (dst []byte) {
	// correct implementation with native.
	// 'p := unsafe.Pointer(&src)' is key point.
	// Also dh.Cap assignment is must.
	// (*reflect.StringHeader)(p) can be used instead of sh.
	p := unsafe.Pointer(&src)
	sh := (*reflect.StringHeader)(p)
	dh := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	dh.Data = uintptr(unsafe.Pointer(sh.Data))
	dh.Len = sh.Len
	dh.Cap = sh.Len
	return

	// correct implementation with reflect
	/*p := unsafe.Pointer(&src)
	sh := (*reflect.StringHeader)(p)
	bVal := reflect.NewAt(reflect.ArrayOf(sh.Len, typeOfByte), unsafe.Pointer(sh.Data)).Elem()
	return bVal.Slice(0, sh.Len).Interface().([]byte)*/

	// wrong implementation with reflect
	/*sh := (*reflect.StringHeader)(unsafe.Pointer(&src))
	bVal := reflect.NewAt(reflect.ArrayOf(sh.Len, typeOfByte), unsafe.Pointer(sh.Data)).Elem()
	return bVal.Slice(0, sh.Len).Interface().([]byte)*/
}

package server

type KeyVal struct {
	Key      string
	Val      []byte
	Expires  int
	CallBack chan interface{}
}

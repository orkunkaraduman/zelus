package server

type keyVal struct {
	Key      string
	Val      []byte
	Expires  int
	CallBack chan interface{}
	UserData interface{}
}

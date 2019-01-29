package protocol

type Receiver interface {
	OnReadCmd(cmd Cmd) (count int)
	OnReadData(count int, index int, data []byte, expiry int)
	OnQuit(e error)
}

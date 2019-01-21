package protocol

type State interface {
	OnReadCmd(cmd Cmd) (count int)
	OnReadData(count int, index int, data []byte)
	OnQuit(e error)
}

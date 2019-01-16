package protocol

type State interface {
	OnReadCmd(cmd Cmd) (count int)
	OnReadData(data []byte)
	OnQuit(e error)
}

package protocol

type State interface {
	OnReadLine(line string) (dataCount int)
	OnReadData(data []byte)
	OnQuit(e error)
}

package protocol

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"sync"
)

const (
	MaxLineLen = 4096
)

var (
	ErrProtocol = errors.New("protocol error")
	ErrIO       = errors.New("IO error")
)

type Protocol struct {
	rd        *bufio.Reader
	wr        *bufio.Writer
	wrMu      sync.Mutex
	closeCh   chan struct{}
	cmdParser *CmdParser
}

func New(r io.Reader, w io.Writer) (prt *Protocol) {
	prt = &Protocol{
		rd:        bufio.NewReaderSize(r, 128*1024),
		wr:        bufio.NewWriterSize(w, 128*1024),
		closeCh:   make(chan struct{}, 1),
		cmdParser: NewCmdParser(),
	}
	return
}

func (prt *Protocol) Close() {
	select {
	case prt.closeCh <- struct{}{}:
	default:
	}
}

func (prt *Protocol) SendCmd(cmd Cmd) (err error) {
	prt.wrMu.Lock()
	var b []byte
	b, err = prt.cmdParser.Serialize(cmd)
	if err != nil {
		err = ErrProtocol
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write(b)
	if err != nil {
		err = ErrIO
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write([]byte("\r\n"))
	if err != nil {
		err = ErrIO
		prt.wrMu.Unlock()
		return
	}
	prt.wrMu.Unlock()
	return
}

func (prt *Protocol) SendData(data []byte) (err error) {
	prt.wrMu.Lock()
	_, err = prt.wr.Write([]byte("\r\n"))
	if err != nil {
		err = ErrIO
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write([]byte(strconv.Itoa(len(data))))
	if err != nil {
		err = ErrIO
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write([]byte("\r\n"))
	if err != nil {
		err = ErrIO
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write(data)
	if err != nil {
		err = ErrIO
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write([]byte("\r\n"))
	if err != nil {
		err = ErrIO
		prt.wrMu.Unlock()
		return
	}
	prt.wrMu.Unlock()
	return
}

func (prt *Protocol) Flush() (err error) {
	prt.wrMu.Lock()
	err = prt.wr.Flush()
	if err != nil {
		err = ErrIO
		prt.wrMu.Unlock()
		return
	}
	prt.wrMu.Unlock()
	return
}

func (prt *Protocol) Serve(state State, closeCh <-chan struct{}) {
	defer func() {
		e, _ := recover().(error)
		state.OnQuit(e)
	}()
	for {
		closed := false
		select {
		case <-closeCh:
			closed = true
		case <-prt.closeCh:
			closed = true
		default:
		}
		if closed {
			break
		}

		var line []byte
		var err error

		// read line
		line, err = readBytesLimit(prt.rd, '\n', MaxLineLen)
		if err != nil {
			if err == errBufferLimitExceeded {
				err = ErrProtocol
			} else {
				err = ErrIO
			}
			panic(err)
		}
		line = trimCrLf(line)
		if len(line) == 0 {
			panic(ErrProtocol)
		}
		var cmd Cmd
		cmd, err = prt.cmdParser.Parse(line)
		if err != nil {
			panic(ErrProtocol)
		}
		var count int
		count = state.OnReadCmd(cmd)

		// read datas
		for i := 0; i < count; i++ {
			// read data header
			line, err = readBytesLimit(prt.rd, '\n', MaxLineLen)
			if err != nil {
				if err == errBufferLimitExceeded {
					err = ErrProtocol
				} else {
					err = ErrIO
				}
				panic(err)
			}
			line = trimCrLf(line)
			if len(line) != 0 {
				state.OnReadData(line)
				continue
			}

			// read data
			line, err = readBytesLimit(prt.rd, '\n', MaxLineLen)
			if err != nil {
				if err == errBufferLimitExceeded {
					err = ErrProtocol
				} else {
					err = ErrIO
				}
				panic(err)
			}
			line = trimCrLf(line)
			var size int
			size, err = strconv.Atoi(string(line))
			if err != nil || size < 0 {
				panic(ErrProtocol)
			}
			data := make([]byte, size)
			_, err = io.ReadFull(prt.rd, data)
			if err != nil {
				panic(ErrIO)
			}
			line, err = readBytesLimit(prt.rd, '\n', 2)
			if err != nil {
				if err == errBufferLimitExceeded {
					err = ErrProtocol
				} else {
					err = ErrIO
				}
				panic(err)
			}
			line = trimCrLf(line)
			if len(line) != 0 {
				panic(ErrProtocol)
			}
			state.OnReadData(data)
		}
		err = prt.Flush()
		if err != nil {
			panic(err)
		}
	}
}

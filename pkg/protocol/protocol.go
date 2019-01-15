package protocol

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

const (
	MaxLineLen = 4096
)

var (
	ErrProtocol = errors.New("protocol error")
	ErrIO       = errors.New("IO error")
)

type Protocol struct {
	rd      *bufio.Reader
	wr      *bufio.Writer
	closeCh chan struct{}
}

func New(r io.Reader, w io.Writer) (prt *Protocol) {
	prt = &Protocol{
		rd:      bufio.NewReader(r),
		wr:      bufio.NewWriter(w),
		closeCh: make(chan struct{}, 1),
	}
	return
}

func (prt *Protocol) Close() {
	prt.closeCh <- struct{}{}
}

func (prt *Protocol) SendLine(line string) {
	var err error
	_, err = prt.wr.Write([]byte(line))
	if err != nil {
		panic(ErrIO)
	}
	_, err = prt.wr.Write([]byte("\r\n"))
	if err != nil {
		panic(ErrIO)
	}
	err = prt.wr.Flush()
	if err != nil {
		panic(ErrIO)
	}
	return
}

func (prt *Protocol) SendData(data []byte) {
	var err error
	_, err = prt.wr.Write([]byte("\r\n"))
	if err != nil {
		panic(ErrIO)
	}
	_, err = prt.wr.Write([]byte(strconv.Itoa(len(data))))
	if err != nil {
		panic(ErrIO)
	}
	_, err = prt.wr.Write([]byte("\r\n"))
	if err != nil {
		panic(ErrIO)
	}
	_, err = prt.wr.Write(data)
	if err != nil {
		panic(ErrIO)
	}
	_, err = prt.wr.Write([]byte("\r\n"))
	if err != nil {
		panic(ErrIO)
	}
	err = prt.wr.Flush()
	if err != nil {
		panic(ErrIO)
	}
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
		var dataCount int
		dataCount = state.OnReadLine(string(line))

		// read datas
		for i := 0; i < dataCount; i++ {
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
	}
}

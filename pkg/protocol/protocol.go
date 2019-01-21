package protocol

import (
	"bufio"
	"encoding/csv"
	"io"
	"strconv"
	"sync"

	"github.com/orkunkaraduman/zelus/pkg/buffer"
)

type Protocol struct {
	rd        *bufio.Reader
	wr        *bufio.Writer
	wrMu      sync.Mutex
	cmdParser *CmdParser
}

var (
	MaxLineLen = 64 * 1024
)

func New(r io.Reader, w io.Writer) (prt *Protocol) {
	prt = &Protocol{
		rd:        bufio.NewReaderSize(r, MaxLineLen*2),
		wr:        bufio.NewWriterSize(w, MaxLineLen*2),
		cmdParser: NewCmdParser(),
	}
	return
}

func (prt *Protocol) SendCmd(cmd Cmd) (err error) {
	prt.wrMu.Lock()
	var b []byte
	b, err = prt.cmdParser.Serialize(cmd)
	if err != nil {
		if _, ok := err.(*csv.ParseError); ok {
			err = &Error{Err: ErrProtocol}
		}
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write(b)
	if err != nil {
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write([]byte("\r\n"))
	if err != nil {
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
		prt.wrMu.Unlock()
		return
	}
	if data == nil {
		_, err = prt.wr.Write([]byte("-1\r\n"))
		if err != nil {
			prt.wrMu.Unlock()
			return
		}
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write([]byte(strconv.Itoa(len(data))))
	if err != nil {
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write([]byte("\r\n"))
	if err != nil {
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write(data)
	if err != nil {
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write([]byte("\r\n"))
	if err != nil {
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
		prt.wrMu.Unlock()
		return
	}
	prt.wrMu.Unlock()
	return
}

func (prt *Protocol) Receive(rc Receiver, bf *buffer.Buffer) bool {
	var line []byte
	var err error

	// read line
	line, err = readBytesLimit(prt.rd, '\n', MaxLineLen)
	if err != nil {
		if err == errBufferLimitExceeded {
			err = &Error{Err: ErrProtocol}
		}
		panic(err)
	}
	line = trimCrLf(line)
	if len(line) == 0 {
		panic(&Error{Err: ErrProtocol})
	}
	var cmd Cmd
	cmd, err = prt.cmdParser.Parse(line)
	if err != nil {
		panic(&Error{Err: ErrProtocol})
	}
	var count int
	count = rc.OnReadCmd(cmd)
	if count < 0 {
		err = prt.Flush()
		if err != nil {
			panic(err)
		}
		return false
	}

	// read datas
	for i := 0; i < count; i++ {
		// read data header
		line, err = readBytesLimit(prt.rd, '\n', MaxLineLen)
		if err != nil {
			if err == errBufferLimitExceeded {
				err = &Error{Err: ErrProtocol}
			}
			panic(err)
		}
		line = trimCrLf(line)
		if len(line) != 0 {
			rc.OnReadData(count, i, line)
			continue
		}

		// read data length
		line, err = readBytesLimit(prt.rd, '\n', MaxLineLen)
		if err != nil {
			if err == errBufferLimitExceeded {
				err = &Error{Err: ErrProtocol}
			}
			panic(err)
		}
		line = trimCrLf(line)
		var size int
		size, err = strconv.Atoi(string(line))
		if err != nil {
			panic(&Error{Err: ErrProtocol})
		}

		// read null data
		if size < 0 {
			rc.OnReadData(count, i, nil)
			continue
		}

		// read data
		data := bf.Want(size)
		_, err = io.ReadFull(prt.rd, data)
		if err != nil {
			panic(err)
		}
		line, err = readBytesLimit(prt.rd, '\n', 2)
		if err != nil {
			if err == errBufferLimitExceeded {
				err = &Error{Err: ErrProtocol}
			}
			panic(err)
		}
		line = trimCrLf(line)
		if len(line) != 0 {
			panic(&Error{Err: ErrProtocol})
		}
		rc.OnReadData(count, i, data)
	}
	err = prt.Flush()
	if err != nil {
		panic(err)
	}
	return true
}

func (prt *Protocol) Serve(rc Receiver, closeCh <-chan struct{}) {
	bf := buffer.New()
	defer func() {
		bf.Close()
		e, _ := recover().(error)
		rc.OnQuit(e)
	}()
	for {
		closed := false
		select {
		case <-closeCh:
			closed = true
		default:
		}
		if closed {
			break
		}
		if !prt.Receive(rc, bf) {
			break
		}
	}
}

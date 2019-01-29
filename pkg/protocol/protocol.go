package protocol

import (
	"bufio"
	"bytes"
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
	BufferSize = MaxLineLen * 2
)

func New(r io.Reader, w io.Writer) (prt *Protocol) {
	prt = &Protocol{
		rd:        bufio.NewReaderSize(r, BufferSize),
		wr:        bufio.NewWriterSize(w, BufferSize),
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

func (prt *Protocol) SendData(data []byte, expiry int) (err error) {
	prt.wrMu.Lock()
	_, err = prt.wr.Write([]byte("\r\n"))
	if err != nil {
		prt.wrMu.Unlock()
		return
	}
	size := len(data)
	if data == nil {
		size = -1
	}
	_, err = prt.wr.Write([]byte(strconv.Itoa(size)))
	if err != nil {
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write([]byte(" "))
	if err != nil {
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write([]byte(strconv.Itoa(expiry)))
	if err != nil {
		prt.wrMu.Unlock()
		return
	}
	_, err = prt.wr.Write([]byte("\r\n"))
	if err != nil {
		prt.wrMu.Unlock()
		return
	}
	if data == nil {
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
			rc.OnReadData(count, i, line, -1)
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
		var bSize, bExpiry []byte
		idx := bytes.IndexByte(line, ' ')
		if idx < 0 {
			bSize = line
		} else {
			bSize = line[:idx]
			bExpiry = line[idx+1:]
		}
		var size int
		size, err = strconv.Atoi(string(bSize))
		if err != nil {
			panic(&Error{Err: ErrProtocol})
		}
		var expiry = int(-1)
		if bExpiry != nil {
			expiry, err = strconv.Atoi(string(bExpiry))
			if err != nil {
				panic(&Error{Err: ErrProtocol})
			}
		}

		// read null data
		if size < 0 {
			rc.OnReadData(count, i, nil, expiry)
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
		rc.OnReadData(count, i, data, expiry)
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

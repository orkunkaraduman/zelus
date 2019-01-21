package protocol

import (
	"bufio"
	"encoding/csv"
	"io"
	"strconv"
	"sync"
	"time"
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

func (prt *Protocol) Serve(state State, closeCh <-chan struct{}) {
	var data []byte
	var dataMaxSize int
	var dataFreeCancelCh = make(chan struct{}, 1)
	var dataMu sync.Mutex
	defer func() {
		data = nil
		close(dataFreeCancelCh)
		e, _ := recover().(error)
		state.OnQuit(e)
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
		count = state.OnReadCmd(cmd)
		if count < 0 {
			break
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
				state.OnReadData(line)
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
			if err != nil || size < 0 {
				panic(&Error{Err: ErrProtocol})
			}

			// read data
			func() {
				dataMu.Lock()
				defer dataMu.Unlock()
				if cap(data) >= size {
					data = data[:size]
				} else {
					data = make([]byte, size, size*2)
					//dataFreeCancelCh <- struct{}{}
					close(dataFreeCancelCh)
					dataFreeCancelCh = make(chan struct{}, 1)
					go func(c chan struct{}) {
						tk := time.NewTicker(60 * time.Second)
						for {
							done := false
							select {
							case <-tk.C:
								dataMu.Lock()
								if cap(data)/4 >= dataMaxSize {
									if dataMaxSize > 0 {
										data = make([]byte, 0, dataMaxSize*2)
									} else {
										data = nil
									}
								}
								dataMaxSize = 0
								dataMu.Unlock()
							case <-c:
								done = true
							}
							if done {
								break
							}
						}
						tk.Stop()
					}(dataFreeCancelCh)
				}
				if size > dataMaxSize {
					dataMaxSize = size
				}
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
				state.OnReadData(data)
			}()
		}
		err = prt.Flush()
		if err != nil {
			panic(err)
		}
	}
}

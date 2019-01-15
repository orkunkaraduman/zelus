package server

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"net"
	"strings"

	"github.com/orkunkaraduman/zelus/pkg/protocol"
	"github.com/orkunkaraduman/zelus/pkg/store"
)

type clientConn struct {
	*protocol.Protocol
	cmdBuffer *bytes.Buffer
	csvReader *csv.Reader
	st        *store.Store
	args      []string
}

func newClientConn(conn net.Conn) (cn *clientConn) {
	cn = &clientConn{
		Protocol: protocol.New(conn, conn),
	}
	cn.cmdBuffer = bytes.NewBuffer(nil)
	cn.csvReader = csv.NewReader(cn.cmdBuffer)
	cn.csvReader.Comma = ' '
	//cn.csvReader.LazyQuotes = true
	//cn.csvReader.ReuseRecord = true
	return
}

func (cn *clientConn) OnReadLine(line string) (dataCount int) {
	var err error
	_, err = cn.cmdBuffer.WriteString(line)
	if err != nil {
		panic(protocol.ErrProtocol)
	}
	var args []string
	args, err = cn.csvReader.Read()
	if err != nil {
		panic(protocol.ErrProtocol)
	}
	switch strings.ToUpper(args[0]) {
	case "QUIT":
		cn.Close()
	case "GET":
		if len(args) < 2 {
			panic(protocol.ErrProtocol)
		}
		var buf [4096]byte
		val := cn.st.Get(args[1], buf[:])
		if val != nil {
			cn.SendLine("OK")
			cn.SendData(val)
		} else {
			cn.SendLine("ERROR")
		}
	case "SET":
		if len(args) < 2 {
			panic(protocol.ErrProtocol)
		}
		cn.args = args
		dataCount = 1
	}

	return
}

func (cn *clientConn) OnReadData(data []byte) {
	args := cn.args
	switch strings.ToUpper(args[0]) {
	case "SET":
		if cn.st.Set(args[1], data, true) {
			cn.SendLine("OK")
		} else {
			cn.SendLine("ERROR")
		}
	}
	return
}

func (cn *clientConn) OnQuit(e error) {
	if e != nil {
		if e != protocol.ErrIO {
			cn.SendLine(fmt.Sprintf("FATAL %q", e.Error()))
		}
		return
	}
	cn.SendLine("QUIT")
}

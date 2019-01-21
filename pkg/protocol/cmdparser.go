package protocol

import (
	"bytes"
	"encoding/csv"
	"strings"
)

type CmdParser struct {
	csvReaderBuffer *bytes.Buffer
	csvReader       *csv.Reader
	csvWriterBuffer *bytes.Buffer
	csvWriter       *csv.Writer
}

func NewCmdParser() (cp *CmdParser) {
	cp = &CmdParser{}
	cp.csvReaderBuffer = bytes.NewBuffer(nil)
	cp.csvReaderBuffer.Grow(MaxLineLen)
	cp.csvReader = csv.NewReader(cp.csvReaderBuffer)
	cp.csvReader.Comma = ' '
	cp.csvReader.FieldsPerRecord = -1
	cp.csvReader.ReuseRecord = true
	cp.csvWriterBuffer = bytes.NewBuffer(nil)
	cp.csvWriterBuffer.Grow(MaxLineLen)
	cp.csvWriter = csv.NewWriter(cp.csvWriterBuffer)
	cp.csvWriter.Comma = ' '
	return
}

func (cp *CmdParser) Parse(b []byte) (cmd Cmd, err error) {
	_, err = cp.csvReaderBuffer.Write(b)
	if err != nil {
		return
	}
	var args []string
	args, err = cp.csvReader.Read()
	if err != nil {
		return
	}
	if len(args) <= 0 {
		return
	}
	cmd.Name = strings.ToUpper(args[0])
	cmd.Args = args[1:]
	return
}

func (cp *CmdParser) Serialize(cmd Cmd) (b []byte, err error) {
	args := make([]string, 1+len(cmd.Args))
	args[0] = strings.ToUpper(cmd.Name)
	copy(args[1:], cmd.Args)
	err = cp.csvWriter.Write(args)
	if err != nil {
		return
	}
	cp.csvWriter.Flush()
	buf := cp.csvWriterBuffer.Bytes()
	b = make([]byte, len(buf))
	copy(b, buf)
	cp.csvWriterBuffer.Truncate(0)
	b = trimCrLf(b)
	return
}

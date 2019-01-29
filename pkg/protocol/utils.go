package protocol

import (
	"bufio"
	"errors"
)

var (
	errBufferLimitExceeded = errors.New("buffer limit exceeded")
)

func readBytesLimit(b *bufio.Reader, delim byte, lim int) (line []byte, err error) {
	line = []byte(nil)
	for len(line) <= lim {
		var buf []byte
		buf, err = b.ReadSlice(delim)
		line = append(line, buf...)
		if err != bufio.ErrBufferFull {
			break
		}
	}
	if err == nil && len(line) > lim {
		err = errBufferLimitExceeded
	}
	return
}

func trimCrLf(buf []byte) []byte {
	l := len(buf)
	if l == 0 {
		return buf
	}

	l--
	if buf[l] != '\n' {
		return buf
	}
	buf = buf[0:l]
	if l == 0 {
		return buf
	}

	l--
	if buf[l] != '\r' {
		return buf
	}
	buf = buf[0:l]

	return buf
}

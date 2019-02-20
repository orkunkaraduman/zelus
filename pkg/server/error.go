package server

import "errors"

var (
	ErrProtocolUnexpectedCommand = errors.New("unexpected command")
)

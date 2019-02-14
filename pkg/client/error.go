package client

import "errors"

var (
	ErrClosed                    = errors.New("closed")
	ErrProtocolUnexpectedCommand = errors.New("unexpected command")
)

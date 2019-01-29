package protocol

import "errors"

type Error struct {
	Err error
	Cmd Cmd
}

var (
	ErrProtocol = errors.New("protocol error")
)

func (e *Error) Error() string {
	return e.Err.Error()
}

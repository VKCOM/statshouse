package sqlite

import "errors"

var (
	errAlreadyClosed = errors.New("sqlite-engine: already closed")
	errUnsafe        = errors.New("sqlite-engine: unsafe SQL")
	ErrReadOnly      = errors.New("sqlite-engine: engine is readonly")
	ErrEngineBroken  = errors.New("sqlite-engine: engine is broken")
)

func IsEngineBrokenError(err error) bool {
	return err == ErrEngineBroken || errors.Is(err, ErrEngineBroken)
}

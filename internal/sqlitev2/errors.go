package sqlitev2

import "errors"

var (
	errAlreadyClosed = errors.New("sqlite-engine: already closed")
	ErrReadOnly      = errors.New("sqlite-engine: engine is readonly")
	ErrEngineBroken  = errors.New("sqlite-engine: engine is broken")
	errEnginePanic   = errors.New("sqlite-engine: engine in panic")
)

func IsEngineBrokenError(err error) bool {
	return err == ErrEngineBroken || errors.Is(err, ErrEngineBroken)
}

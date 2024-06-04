package sqlite

import "errors"

var (
	errAlreadyClosed = errors.New("sqlite-engine: already closed")
	errReadOnly      = errors.New("sqlite-engine: engine is readonly")
	errEngineBroken  = errors.New("sqlite-engine: engine is broken")
)

func IsEngineBrokenError(err error) bool {
	return err == errEngineBroken || errors.Is(err, errEngineBroken)
}

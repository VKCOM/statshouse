package sqlitev2

import "errors"

var (
	AvoidUnsafe      = false // In the case of some bags
	errAlreadyClosed = errors.New("sqlite-engine: already closed")
	errReadOnly      = errors.New("sqlite-engine: engine is readonly")
	errEngineBroken  = errors.New("sqlite-engine: engine is broken")
	errEnginePanic   = errors.New("sqlite-engine: engine in panic")
)

func IsEngineBrokenError(err error) bool {
	return err == errEngineBroken || errors.Is(err, errEngineBroken)
}

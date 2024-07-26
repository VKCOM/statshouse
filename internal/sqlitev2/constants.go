package sqlitev2

import (
	"errors"

	"github.com/vkcom/statshouse/internal/sqlitev2/sqlite0"
	"go.uber.org/multierr"
)

var (
	AvoidUnsafe      = false // In the case of some bags
	ErrAlreadyClosed = errors.New("sqlite-engine: already closed")
	errReadOnly      = errors.New("sqlite-engine: engine is readonly")
	errEngineBroken  = errors.New("sqlite-engine: engine is broken")
	errEnginePanic   = errors.New("sqlite-engine: engine in panic")
)

var (
	ErrConstraintUnique     = errors.New("unique_constraint_error")
	ErrConstraintDatatype   = errors.New("datatype_constraint_error")
	ErrConstraintPrimarykey = errors.New("primarykey_constraint_error")
	ErrDoWithoutEvent       = errors.New("do without binlog event")
	codeToError             = map[int]error{
		2067: ErrConstraintUnique,
		3091: ErrConstraintDatatype,
		1555: ErrConstraintPrimarykey,
	}
)

func IsEngineBrokenError(err error) bool {
	return err == errEngineBroken || errors.Is(err, errEngineBroken)
}

func mapSqliteErr(err error) error {
	if err == nil {
		return err
	}
	var sqliteErr sqlite0.Error
	if errors.As(err, &sqliteErr) {
		err1 := codeToError[sqliteErr.Code()]
		if err1 == nil {
			return err
		}
		return multierr.Append(err, err1)
	}
	return err
}

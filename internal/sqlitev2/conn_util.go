package sqlitev2

import (
	"fmt"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/sqlitev2/sqlite0"
)

const (
	busyTimeout = 5 * time.Second
	cacheKB     = 65536 // 64MB
)

func openRW(open func(path string, pageSize int, flags int) (*sqlite0.Conn, error), path string, appID uint32, pageSize int) (*sqlite0.Conn, error) {
	conn, err := open(path, pageSize, sqlite0.OpenReadWrite|sqlite0.OpenCreate)
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA cache_size=-%d", cacheKB))
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to change DB cache size to %dKB: %w", cacheKB, err)
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA application_id=%d", appID)) // make DB ready to use snapshots
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to set DB app ID %d: %w", appID, err)
	}

	return conn, nil
}

func open(path string, flags int) (*sqlite0.Conn, error) {
	conn, err := sqlite0.Open(path, flags)
	if err != nil {
		return nil, err
	}

	err = conn.SetBusyTimeout(busyTimeout)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to set DB busy timeout to %v: %w", busyTimeout, err)
	}
	return conn, nil
}

func openWAL(path string, pageSize int, flags int) (*sqlite0.Conn, error) {
	conn, err := open(path, flags)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite conn: %w", err)
	}

	if pageSize > 0 {
		err = conn.Exec(fmt.Sprintf("PRAGMA page_size=%d", pageSize))
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to set page_size: %w", err)
		}
	}

	err = conn.Exec("PRAGMA journal_mode=WAL2")
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to enable DB WAL mode: %w", err)
	}
	return conn, nil
}

func openROWAL(path string) (*sqlite0.Conn, error) {
	return openWAL(path, 0, sqlite0.OpenReadonly)
}

func checkSliceParamName(s string) bool {
	return strings.HasPrefix(s, "$") && strings.HasSuffix(s, "$")
}

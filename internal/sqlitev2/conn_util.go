package sqlitev2

import (
	"fmt"
	"time"
)
import "github.com/vkcom/statshouse/internal/sqlite/internal/sqlite0"

const (
	busyTimeout = 5 * time.Second
	cacheKB     = 65536                         // 64MB
	mmapSize    = 8 * 1024 * 1024 * 1024 * 1024 // 8TB
)

func openRW(open func(path string, flags int) (*sqlite0.Conn, error), path string, appID int32, schemas ...string) (*sqlite0.Conn, error) {
	conn, err := open(path, sqlite0.OpenReadWrite|sqlite0.OpenCreate)
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA cache_size=-%d", cacheKB))
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to change DB cache size to %dKB: %w", cacheKB, err)
	}

	if false {
		err = conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d", mmapSize))
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to set DB mmap size to %d: %w", mmapSize, err)
		}
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA application_id=%d", appID)) // make DB ready to use snapshots
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to set DB app ID %d: %w", appID, err)
	}

	for _, schema := range schemas {
		err = conn.Exec(schema)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to setup DB schema: %w", err)
		}
	}

	return conn, nil
}

func openWAL(path string, flags int) (*sqlite0.Conn, error) {
	conn, err := sqlite0.Open(path, flags)
	if err != nil {
		return nil, err
	}

	// todo make checkpoint manually
	if false {
		err = conn.SetAutoCheckpoint(0)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to disable DB auto-checkpoints: %w", err)
		}
	}

	err = conn.SetBusyTimeout(busyTimeout)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to set DB busy timeout to %v: %w", busyTimeout, err)
	}

	err = conn.Exec("PRAGMA journal_mode=WAL2")
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to enable DB WAL mode: %w", err)
	}

	return conn, nil
}

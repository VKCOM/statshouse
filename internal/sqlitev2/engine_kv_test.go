package sqlitev2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	binlog2 "github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"
	"go.uber.org/multierr"
)

type eng struct {
	engine *Engine
	bl     binlog2.Binlog
}

const schemeKV = "CREATE TABLE IF NOT EXISTS test_db (k INTEGER PRIMARY KEY, v INTEGER);"

func (e *eng) put(ctx context.Context, k, v int64) error {
	return e.engine.Do(ctx, "put", func(c Conn, cache []byte) ([]byte, error) {
		return putConn(c, cache, k, v)
	})
}

func putConn(c Conn, cache []byte, k, v int64) ([]byte, error) {
	rows := c.Query("select", "SELECT v FROM test_db WHERE k = $k", Int64("$k", k))
	var err = rows.Error()
	if rows.Next() {
		_, err = c.Exec("update", "UPDATE test_db SET v = $v WHERE k = $k", Int64("$k", k), Int64("$v", v))
	} else {
		_, err = c.Exec("update", "INSERT INTO test_db (k, v) VALUES ($k, $v)", Int64("$k", k), Int64("$v", v))
	}
	err = multierr.Append(err, rows.Error())
	event := tl.VectorLong{k, v}
	cache, errWrite := event.WriteBoxed(cache)
	return cache, multierr.Append(err, errWrite)
}

func (e *eng) get(ctx context.Context, k int64) (v int64, ok bool, err error) {
	err = e.engine.View(ctx, "put", func(c Conn) error {
		rows := c.Query("select", "SELECT v FROM test_db WHERE k = $k", Int64("$k", k))
		if ok = rows.Next(); ok {
			v = rows.ColumnInt64(0)
		}
		return rows.Error()
	})
	return v, ok, err
}

func (e *eng) mustCloseGoodEngine(t *testing.T) {
	require.NoError(t, e.engine.rw.connError)
	require.NoError(t, e.engine.Close())
	require.ErrorIs(t, e.engine.rw.connError, errAlreadyClosed)
}

func (e *eng) mustCloseErrorEngine(t *testing.T, err error) {
	require.ErrorIs(t, e.engine.rw.connError, err)
	require.ErrorIs(t, e.engine.Close(), err)
	require.ErrorIs(t, e.engine.rw.connError, errAlreadyClosed)
}

func applyKV(conn Conn, bytes []byte) (read int, err error) {
	event := tl.VectorLong{}
	for len(bytes) > 0 {
		if len(bytes) < 4 {
			return fsbinlog.AddPadding(read), binlog2.ErrorNotEnoughData
		}
		newBytes, err := event.ReadBoxed(bytes)
		if err != nil {
			return read, err
		}
		read += len(bytes) - len(newBytes)
		bytes = newBytes
		_, err = putConn(conn, nil, event[0], event[1])
		if err != nil {
			return fsbinlog.AddPadding(read), err
		}
	}
	return fsbinlog.AddPadding(read), nil
}

func createEngMaster(t *testing.T, opt testEngineOptions) *eng {
	engine, bl := openEngine1(t, testEngineOptions{
		prefix:      opt.prefix,
		dbFile:      opt.dbFile,
		scheme:      schemeKV,
		create:      true,
		replica:     false,
		readAndExit: false,
		applyF:      applyKV,
		testOptions: nil,
		maxRoConn:   opt.maxRoConn,
	})
	return &eng{
		engine: engine,
		bl:     bl,
	}
}

/*
	func openEngMaster(t *testing.T, tempDir string) *eng {
		engine, bl := openEngine1(t, testEngineOptions{
			prefix:      tempDir,
			dbFile:      "db",
			scheme:      schemeKV,
			create:      false,
			replica:     false,
			readAndExit: false,
			applyF:      applyKV,
			testOptions: nil,
		})
		return &eng{
			engine: engine,
			bl:     bl,
		}
	}
*/

func openEngReplica(t *testing.T, tempDir string) *eng {
	engine, bl := openEngine1(t, testEngineOptions{
		prefix:      tempDir,
		dbFile:      "db",
		scheme:      schemeKV,
		create:      false,
		replica:     true,
		readAndExit: false,
		applyF:      applyKV,
		testOptions: nil,
	})
	return &eng{
		engine: engine,
		bl:     bl,
	}
}
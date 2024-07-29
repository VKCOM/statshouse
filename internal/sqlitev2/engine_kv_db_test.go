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

func (e *eng) insertOrReplace(ctx context.Context, k, v int64) error {
	_, err := e.engine.DoTx(ctx, "Put", func(c Conn, cache []byte) ([]byte, error) {
		return putConn(c, cache, k, v)
	})
	return err
}

func (e *eng) insert(ctx context.Context, k, v int64) error {
	_, err := e.engine.DoTx(ctx, "Insert", func(c Conn, cache []byte) ([]byte, error) {
		err := c.Exec("update", "INSERT INTO test_db (k, v) VALUES ($k, $v)", Integer("$k", k), Integer("$v", v))
		if err != nil {
			return cache, err
		}
		event := tl.VectorLong{k, v}
		cache = event.WriteBoxed(cache)
		return cache, err
	})
	return err
}

func putConn(c Conn, cache []byte, k, v int64) ([]byte, error) {
	rows := c.Query("select", "SELECT v FROM test_db WHERE k = $k", Integer("$k", k))
	var err = rows.Error()
	if rows.Next() {
		err = c.Exec("update", "UPDATE test_db SET v = $v WHERE k = $k", Integer("$k", k), Integer("$v", v))
	} else {
		err = c.Exec("update", "INSERT INTO test_db (k, v) VALUES ($k, $v)", Integer("$k", k), Integer("$v", v))
	}
	err = multierr.Append(err, rows.Error())
	event := tl.VectorLong{k, v}
	cache = event.WriteBoxed(cache)
	return cache, err
}

func (e *eng) get(ctx context.Context, k int64) (v int64, ok bool, err error) {
	_, err = e.engine.ViewTx(ctx, "Put", func(c Conn) error {
		rows := c.Query("select", "SELECT v FROM test_db WHERE k = $k", Integer("$k", k))
		if ok = rows.Next(); ok {
			v = rows.ColumnInteger(0)
		}
		return rows.Error()
	})
	return v, ok, err
}

func (e *eng) mustCloseGoodEngine(t *testing.T) {
	require.NoError(t, e.engine.rw.conn.connError)
	require.NoError(t, e.engine.Close())
	require.ErrorIs(t, e.engine.rw.conn.connError, ErrAlreadyClosed)
}

func (e *eng) mustCloseErrorEngine(t *testing.T, err error) {
	require.ErrorIs(t, e.engine.rw.conn.connError, err)
	require.ErrorIs(t, e.engine.Close(), err)
	require.ErrorIs(t, e.engine.rw.conn.connError, ErrAlreadyClosed)
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
*/

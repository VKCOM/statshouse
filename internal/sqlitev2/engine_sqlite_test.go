package sqlitev2

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vkcom/statshouse/internal/sqlitev2/restart"
)

// sqlite специфичные тесты

func saveCommitOffset(pathDb string, offset int64) error {
	rf, err := restart.OpenAndLock(pathDb)
	if err != nil {
		return err
	}
	err = rf.SetCommitOffsetAndSync(offset)
	if err != nil {
		return err
	}
	return rf.Close()
}

// Все изменения должны быть откачены
func Test_MustRevert(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	dbPath := dir + "/db"
	engine, err := OpenEngine(Options{
		Path:                         dbPath,
		APPID:                        32,
		Scheme:                       schema,
		CacheApproxMaxSizePerConnect: 1,
		ShowLastInsertID:             true,
	})
	require.NoError(t, err)
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("__update_binlog_pos", "UPDATE __binlog_offset set offset = 1")
		return cache, err
	})
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
			_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", strconv.FormatInt(int64(i), 10)))
			return cache, err
		})
		require.NoError(t, err)
	}

	require.NoError(t, engine.Close())
	engine, err = OpenEngine(Options{
		Path:                         dbPath,
		APPID:                        32,
		Scheme:                       schema,
		CacheApproxMaxSizePerConnect: 1,
		ShowLastInsertID:             true,
	})
	require.NoError(t, err)
	var count int64
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT count(data) from test_db")
		require.NoError(t, rows.Error())
		for rows.Next() {
			count = rows.ColumnInt64(0)
		}
		return cache, err
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
	require.NoError(t, engine.Close())
}

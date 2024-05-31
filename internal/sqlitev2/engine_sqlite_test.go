package sqlitev2

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// sqlite специфичные тесты

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
	})
	require.NoError(t, err)
	_, err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		err = conn.Exec("__update_binlog_pos", "UPDATE __binlog_offset set offset = 1")
		return cache, err
	})
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		_, err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
			err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", strconv.FormatInt(int64(i), 10)))
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
	})
	require.NoError(t, err)
	var count int64
	_, err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT count(data) from test_db")
		require.NoError(t, rows.Error())
		for rows.Next() {
			count = rows.ColumnInteger(0)
		}
		return cache, err
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
	require.NoError(t, engine.Close())
}

package sqlitev2

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rand"
)

const schemaNumber = "CREATE TABLE IF NOT EXISTS test_db (k INTEGER PRIMARY KEY, v INTEGER);"

type testAggregation1 struct {
	n  int64
	mx sync.Mutex
}

func genBinlogNumberEvent(v int64, cache []byte) []byte {
	cache = binary.LittleEndian.AppendUint32(cache, magic)
	return binary.LittleEndian.AppendUint64(cache, uint64(v))
}

func incNumber(e *Engine, n int64, failAfterExec bool) error {
	return e.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("select", "SELECT * FROM test_db")
		if rows.Error() != nil {
			return cache, rows.Error()
		}
		var err error
		if !rows.Next() {
			_, err = conn.Exec("test", "INSERT INTO test_db(k, v) VALUES (0, $v)", Int64("$v", n))
		} else {
			_, err = conn.Exec("update", "UPDATE test_db SET v = v + $v", Int64("$v", n))
		}
		if err != nil {
			return cache, err
		}
		if failAfterExec {
			return genBinlogNumberEvent(n, cache), errTest
		}
		return genBinlogNumberEvent(n, cache), err
	})
}

func Test_Engine_Do(t *testing.T) {
	dir := t.TempDir()
	engine, _ := openEngine1(t, testEngineOptions{
		prefix: dir,
		dbFile: "db",
		scheme: schemaNumber,
		create: true,
		testOptions: &testOptions{sleep: func() {
			if rand.Int()%2 == 0 {
				time.Sleep(time.Nanosecond + time.Duration(rand.Int63n(int64(time.Millisecond))))
			}
		}},
	})
	agg := &testAggregation1{}
	n := 32
	iters := 1000
	wg := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				n := rand.Int63n(99999)
				err := incNumber(engine, n, j%10 == 0)
				if errors.Is(err, errTest) {
					continue
				}
				require.NoError(t, err)
				agg.mx.Lock()
				agg.n += n
				agg.mx.Unlock()
			}
		}(i)
	}
	wg.Wait()
	require.NoError(t, engine.Close())
	engine, _ = openEngine(t, dir, "db", schemaNumber, false, false, false, func(s string) {
		t.Fatal("mustn't apply music")
	})
	var actualN int64
	err := engine.Do(context.Background(), "test", func(conn Conn, bytes []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT v from test_db")
		if rows.err != nil {
			return bytes, rows.err
		}
		for rows.Next() {
			actualN = rows.ColumnInt64(0)
		}
		return bytes, nil
	})
	require.NoError(t, err)
	require.Equal(t, agg.n, actualN)
	require.NoError(t, engine.Close())
}

func Test_Engine_View(t *testing.T) {
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, nil)
	agg := &testAggregation{}
	iters := 1000
	for j := 0; j < iters; j++ {
		data := make([]byte, 20)
		_, err := rand.Read(data)
		require.NoError(t, err)
		data = strconv.AppendInt(data, int64(j), 10)

		str := string(data)
		err = insertText(engine, str, false)
		if err == errTest {
			continue
		}
		require.NoError(t, err)
		agg.mx.Lock()
		agg.writeHistory = append(agg.writeHistory, str)
		agg.mx.Unlock()
	}
	require.NoError(t, engine.Close())
	engine, _ = openEngine(t, dir, "db", schema, false, false, false, func(s string) {
		t.Fatal("mustn't apply music")
	})
	expectedMap := map[string]struct{}{}
	for _, t := range agg.writeHistory {
		expectedMap[t] = struct{}{}
	}
	wg := &sync.WaitGroup{}
	n := 32
	iters = 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				err := engine.View(context.Background(), "test", func(conn Conn) error {
					actualDb := map[string]struct{}{}
					rows := conn.Query("test", "SELECT t from test_db")
					if rows.err != nil {
						return rows.err
					}
					for rows.Next() {
						t, err := rows.ColumnBlobString(0)
						if err != nil {
							return err
						}
						actualDb[t] = struct{}{}
					}
					require.True(t, reflect.DeepEqual(expectedMap, actualDb))
					return nil
				})
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()
	require.NoError(t, engine.Close())
}

func Test_ReadAndExit(t *testing.T) {
	const n = 1000
	dir := t.TempDir()
	engineMaster, _ := openEngine(t, dir, "db", schema, true, false, false, nil)
	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := insertText(engineMaster, strconv.Itoa(i), false)
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()
	require.NoError(t, engineMaster.Close())
	engineMaster, _ = openEngine(t, dir, "db1", schema, false, false, true, nil)
	c := 0
	err := engineMaster.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT t from test_db")
		for rows.Next() {
			c++
		}
		return cache, nil
	})
	require.NoError(t, err)
	require.Greater(t, c, 0, "no data in replica")
	require.Equal(t, c, n)
	require.NoError(t, engineMaster.Close())
}

package sqlitev2

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rand"
)

func Test_Engine_Do(t *testing.T) {
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, nil)
	agg := &testAggregation{}
	n := 32
	iters := 1000
	wg := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				data := make([]byte, 20)
				_, err := rand.Read(data)
				require.NoError(t, err)
				data = strconv.AppendInt(data, int64(i), 10)
				data = strconv.AppendInt(data, int64(j), 10)

				str := string(data)
				err = insertText(engine, str, j%10 == 0)
				if errors.Is(err, errTest) {
					continue
				}
				require.NoError(t, err)
				agg.mx.Lock()
				agg.writeHistory = append(agg.writeHistory, str)
				agg.mx.Unlock()
			}
		}(i)
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))
	engine, _ = openEngine(t, dir, "db", schema, false, false, false, func(s string) {
		t.Fatal("mustn't apply music")
	})
	expectedMap := map[string]struct{}{}
	for _, t := range agg.writeHistory {
		expectedMap[t] = struct{}{}
	}
	actualDb := map[string]struct{}{}
	err := engine.Do(context.Background(), "test", func(conn Conn, bytes []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT t from test_db")
		if rows.err != nil {
			return bytes, rows.err
		}
		for rows.Next() {
			t, err := rows.ColumnBlobString(0)
			if err != nil {
				return bytes, err
			}
			actualDb[t] = struct{}{}
		}
		return bytes, nil
	})
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(expectedMap, actualDb))
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))
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
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engineMaster.Close(ctx))
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
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engineMaster.Close(ctx))
}

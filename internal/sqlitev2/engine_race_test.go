package sqlitev2

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	binlog2 "github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"
	"go.uber.org/atomic"
	"pgregory.net/rand"
)

type testAggregation1 struct {
	n  int64
	mx sync.Mutex
}

type incResult struct {
	prevOffset int64
	newOffset  int64
	//number     int64
}

func genBinlogNumberEvent(v int64, cache []byte) []byte {
	cache = binary.LittleEndian.AppendUint32(cache, magic)
	return binary.LittleEndian.AppendUint64(cache, uint64(v))
}

func selectNumber(conn Conn) (int64, bool, error) {
	rows := conn.Query("select", "SELECT v FROM test_db")
	if rows.Error() != nil {
		return 0, false, rows.Error()
	}
	if !rows.Next() {
		return 0, false, nil
	}
	number := rows.ColumnInteger(0)
	return number, true, nil
}

func incNumberExec(conn Conn, cache []byte, n int64, failAfterExec bool) ([]byte, error) {
	rows := conn.Query("select", "SELECT * FROM test_db")
	if rows.Error() != nil {
		return cache, rows.Error()
	}
	var err error
	if !rows.Next() {
		err = conn.Exec("test", "INSERT INTO test_db(k, v) VALUES (0, $v)", Integer("$v", n))
	} else {
		err = conn.Exec("update", "UPDATE test_db SET v = v + $v", Integer("$v", n))
	}
	if err != nil {
		return cache, err
	}
	if failAfterExec {
		return genBinlogNumberEvent(n, cache), errTest
	}
	return genBinlogNumberEvent(n, cache), err
}

func incNumber(ctx context.Context, e *Engine, n int64, failAfterExec bool, m map[int64]int64, mx *sync.RWMutex) (result incResult, _ error) {
	res, err := e.DoTx(ctx, "test", func(conn Conn, cache []byte) ([]byte, error) {
		cache, err := incNumberExec(conn, cache, n, failAfterExec)
		if err != nil {
			return cache, fmt.Errorf("failed to inc number: %w", err)
		}
		var isExist bool
		result.prevOffset, isExist, err = binlogLoadPosition(internalFromUser(conn))
		if err != nil {
			return cache, fmt.Errorf("failed to get binlog offset: %w", err)
		}
		if !isExist {
			return cache, fmt.Errorf("expect to get binlog offset")
		}

		if mx != nil {
			newN, _, err := selectNumber(conn)
			if err != nil {
				return cache, fmt.Errorf("failed to select number: %w", err)
			}
			mx.Lock()
			defer mx.Unlock()
			m[result.prevOffset] = newN - n
		}
		return cache, err
	})
	result.newOffset = res.DBOffset
	return result, err
}

func applyNumberInc(t *testing.T, conn Conn, payload []byte) (int, error) {
	read := 0
	for len(payload) > 0 {
		if len(payload) < 4 {
			return fsbinlog.AddPadding(read), binlog2.ErrorNotEnoughData
		}
		var mark uint32
		mark, _, err := basictl.NatReadTag(payload)
		if err != nil {
			return fsbinlog.AddPadding(read), err
		}
		if mark != magic {
			return fsbinlog.AddPadding(read), binlog2.ErrorUnknownMagic
		}
		if len(payload) < 12 {
			return fsbinlog.AddPadding(read), binlog2.ErrorNotEnoughData
		}
		n := binary.LittleEndian.Uint64(payload[4:12])
		_, err = incNumberExec(conn, nil, int64(n), false)
		require.NoError(t, err)

		offset := fsbinlog.AddPadding(12)
		read += offset
		payload = payload[offset:]
	}
	return fsbinlog.AddPadding(read), nil
}

func Test_Engine_Race_Do(t *testing.T) {
	dir := t.TempDir()

	engine, _ := openEngine1(t, testEngineOptions{
		prefix: dir,
		dbFile: "db",
		scheme: schemeKV,
		create: true,
		applyF: func(conn Conn, payload []byte) (int, error) {
			return applyNumberInc(t, conn, payload)
		},
	})
	agg := &testAggregation1{}
	n := 4
	iters := 1000
	wg := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				n := rand.Int63n(99999)
				shouldTimeout := rand.Int()%2 == 0
				ctx := context.Background()
				if shouldTimeout {
					var cancel func()
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				_, err := incNumber(ctx, engine, n, j%10 == 0, nil, nil)
				if errors.Is(err, errTest) {
					continue
				}
				if shouldTimeout || errors.Is(err, context.Canceled) {
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
	engine, _ = openEngine1(t, testEngineOptions{
		prefix: dir,
		dbFile: "db",
		scheme: schemeKV,
		create: false,
		applyF: func(conn Conn, payload []byte) (int, error) {
			return applyNumberInc(t, conn, payload)
		},
	})
	var actualN int64
	_, err := engine.ViewTx(context.Background(), "test", func(conn Conn) error {
		rows := conn.Query("test", "SELECT v from test_db")
		if rows.err != nil {
			return rows.err
		}
		for rows.Next() {
			actualN = rows.ColumnInteger(0)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, agg.n, actualN)
	require.NoError(t, engine.Close())
}

func Test_Engine_Race_View(t *testing.T) {
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
	engine, _ = openEngine(t, dir, "db", schema, false, true, false, nil)
	expectedMap := map[string]struct{}{}
	for _, t := range agg.writeHistory {
		expectedMap[t] = struct{}{}
	}
	wg := &sync.WaitGroup{}
	n := 4
	iters = 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				_, err := engine.ViewTx(context.Background(), "test", func(conn Conn) error {
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
	_, err := engineMaster.ViewTx(context.Background(), "test", func(conn Conn) error {
		rows := conn.Query("test", "SELECT t from test_db")
		for rows.Next() {
			c++
		}
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, c, 0, "no data in replica")
	require.Equal(t, c, n)
	require.NoError(t, engineMaster.Close())
}

func Test_Race_Engine_Replica_View(t *testing.T) {
	dir := t.TempDir()
	engineM, _ := openEngine1(t, testEngineOptions{
		prefix: dir,
		dbFile: "db",
		scheme: schemeKV,
		create: true,
	})
	m := map[int64]int64{}
	mx := &sync.RWMutex{}
	insert := func() {
		n := rand.Int63n(99999)
		ctx := context.Background()
		_, err := incNumber(ctx, engineM, n, false, m, mx)
		require.NoError(t, err)
	}
	insert()
	go func() {
		for {
			insert()
			time.Sleep(time.Millisecond)
		}
	}()
	engineR, _ := openEngine1(t, testEngineOptions{
		prefix:  dir,
		dbFile:  "dbreplica",
		scheme:  schemeKV,
		create:  false,
		replica: true,
		applyF: func(conn Conn, payload []byte) (int, error) {
			return applyNumberInc(t, conn, payload)
		},
	})
	wg := &sync.WaitGroup{}
	n := 4
	iters := 1000
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_, err := engineR.ViewTx(context.Background(), "test", func(conn Conn) error {
					n, ok, err := selectNumber(conn)
					require.NoError(t, err)
					if !ok {
						return nil
					}
					offs, ok, err := binlogLoadPosition(internalFromUser(conn))
					require.NoError(t, err)
					if !ok {
						t.Error("expect get binlogpos")
						return nil
					}
					mx.RLock()
					expectedN, ok := m[offs]
					mx.RUnlock()
					if !ok {
						return nil
					}
					require.Equal(t, expectedN, n)
					return nil
				})
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()
}

func Test_Race_Engine_View_WaitOffset(t *testing.T) {
	dir := t.TempDir()
	engineM, _ := openEngine1(t, testEngineOptions{
		prefix: dir,
		dbFile: "db",
		scheme: schemeKV,
		create: true,
	})
	m := map[int64]int64{}
	mx := &sync.RWMutex{}
	var res atomic.Int64

	insert := func() {
		n := rand.Int63n(99999)
		ctx := context.Background()
		offset, err := incNumber(ctx, engineM, n, false, m, mx)
		require.NoError(t, err)
		res.Store(offset.newOffset)
	}
	insert()
	go func() {
		for {
			insert()
		}
	}()
	engineR, _ := openEngine1(t, testEngineOptions{
		prefix:  dir,
		dbFile:  "dbreplica",
		scheme:  schemeKV,
		create:  false,
		replica: true,
		applyF: func(conn Conn, payload []byte) (int, error) {
			time.Sleep(time.Millisecond) // to force replica delay
			return applyNumberInc(t, conn, payload)
		},
	})
	wg := &sync.WaitGroup{}
	n := 4
	iters := 10
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_, err := engineR.ViewTxOpts(context.Background(), ViewTxOptions{
					QueryName:  "test",
					WaitOffset: res.Load(),
				}, func(conn Conn) error {
					n, ok, err := selectNumber(conn)
					require.NoError(t, err)
					if !ok {
						return nil
					}
					offs, ok, err := binlogLoadPosition(internalFromUser(conn))
					require.NoError(t, err)
					if !ok {
						t.Error("expect get binlogpos")
						return nil
					}
					mx.RLock()
					expectedN, ok := m[offs]
					mx.RUnlock()
					if !ok {
						return nil
					}
					require.Equal(t, expectedN, n)
					return nil
				})
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()
}

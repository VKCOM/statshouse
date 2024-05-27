// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
package sqlitev2

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vkcom/statshouse/internal/sqlitev2/checkpoint"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"pgregory.net/rand"

	binlog2 "github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"

	"github.com/stretchr/testify/require"
)

type Logger struct{}

func (*Logger) Tracef(format string, args ...interface{}) {
	fmt.Printf("Trace: "+format+"\n", args...)
}
func (*Logger) Debugf(format string, args ...interface{}) {
	fmt.Printf("Debug: "+format+"\n", args...)
}
func (*Logger) Infof(format string, args ...interface{}) {
	fmt.Printf("Info: "+format+"\n", args...)
}
func (*Logger) Warnf(format string, args ...interface{}) {
	fmt.Printf("Warn: "+format+"\n", args...)
}
func (*Logger) Errorf(format string, args ...interface{}) {
	fmt.Printf("Error: "+format+"\n", args...)
}

type testAggregation struct {
	writeHistory []string
	mx           sync.Mutex
}

const schema = "CREATE TABLE IF NOT EXISTS test_db (t TEXT PRIMARY KEY);"

var magic uint32 = 0xf00

func genBinlogEvent(s string, cache []byte) []byte {
	cache = append(cache, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(cache, magic)
	binary.LittleEndian.PutUint32(cache[4:], uint32(len(s)))

	return append(cache, []byte(s)...)
}

var errTest = fmt.Errorf("test error")

func insertText(e *Engine, s string, failAfterExec bool) error {
	return e.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err := conn.Exec("test", "INSERT INTO test_db(t) VALUES ($t)", BlobString("$t", s))
		if failAfterExec {
			return genBinlogEvent(s, cache), errTest
		}
		return genBinlogEvent(s, cache), err
	})
}

func apply(t *testing.T, applyF func(string2 string)) func(conn Conn, bytes []byte) (int, error) {
	return func(conn Conn, bytes []byte) (int, error) {
		read := 0
		for len(bytes) > 0 {
			if len(bytes) < 4 {
				return fsbinlog.AddPadding(read), binlog2.ErrorNotEnoughData
			}
			var mark uint32
			mark, _, err := basictl.NatReadTag(bytes)
			if err != nil {
				return fsbinlog.AddPadding(read), err
			}
			if mark != magic {
				return fsbinlog.AddPadding(read), binlog2.ErrorUnknownMagic
			}
			if len(bytes) < 8 {
				return fsbinlog.AddPadding(read), binlog2.ErrorNotEnoughData
			}
			n := binary.LittleEndian.Uint32(bytes[4:8])
			if len(bytes) < int(n)+8 {
				return fsbinlog.AddPadding(read), binlog2.ErrorNotEnoughData
			}
			str := bytes[8:][:n]
			_, err = conn.Exec("test", "INSERT INTO test_db(t) VALUES ($t)", BlobString("$t", string(str)))
			require.NoError(t, err)
			if applyF != nil {
				applyF(string(str))
			}

			offset := fsbinlog.AddPadding(4 + 4 + int(n))
			read += offset
			bytes = bytes[offset:]
		}
		return fsbinlog.AddPadding(read), nil
	}

}

type testEngineOptions struct {
	prefix      string
	dbFile      string
	scheme      string
	create      bool
	replica     bool
	readAndExit bool
	applyF      ApplyEventFunction
	maxRoConn   int
}

func defaultTestEngineOptions(prefix string) testEngineOptions {
	return testEngineOptions{
		prefix:    prefix,
		dbFile:    "db",
		scheme:    "",
		applyF:    nil,
		maxRoConn: 0,
	}
}

func openEngine1(t *testing.T, opt testEngineOptions) (*Engine, binlog2.Binlog) {
	options := binlog2.Options{
		PrefixPath:  opt.prefix + "/test",
		Magic:       3456,
		ReplicaMode: opt.replica,
		ReadAndExit: opt.readAndExit,
	}
	if opt.create {
		_, err := fsbinlog.CreateEmptyFsBinlog(options)
		require.NoError(t, err)
	}
	bl, err := fsbinlog.NewFsBinlog(&Logger{}, options)
	require.NoError(t, err)
	engine, err := OpenEngine(Options{
		Path:   opt.prefix + "/" + opt.dbFile,
		APPID:  32,
		Scheme: opt.scheme,
		BinlogOptions: BinlogOptions{
			// Replica:     opt.replica,
			ReadAndExit: opt.readAndExit,
		},
		CacheApproxMaxSizePerConnect: 1,
		ShowLastInsertID:             true,
		MaxROConn:                    opt.maxRoConn,
	})
	//engine.testOptions = opt.testOptions
	require.NoError(t, err)
	go func() {
		require.NoError(t, engine.Run(bl, opt.applyF))
	}()
	require.NoError(t, engine.WaitReady())
	return engine, bl
}

func openEngine(t *testing.T, prefix string, dbfile, schema string, create, replica, readAndExit bool, applyF func(string2 string)) (*Engine, binlog2.Binlog) {
	options := binlog2.Options{
		PrefixPath:  prefix + "/test",
		Magic:       3456,
		ReplicaMode: replica,
		ReadAndExit: readAndExit,
	}
	if create {
		_, err := fsbinlog.CreateEmptyFsBinlog(options)
		require.NoError(t, err)
	}
	bl, err := fsbinlog.NewFsBinlog(&Logger{}, options)
	require.NoError(t, err)
	engine, err := OpenEngine(Options{
		Path:   prefix + "/" + dbfile,
		APPID:  32,
		Scheme: schema,
		BinlogOptions: BinlogOptions{
			// Replica:     replica,
			ReadAndExit: readAndExit,
		},
		CacheApproxMaxSizePerConnect: 100,
		ShowLastInsertID:             true,
	})
	require.NoError(t, err)
	go func() {
		require.NoError(t, engine.Run(bl, apply(t, applyF)))
	}()
	require.NoError(t, engine.WaitReady())
	return engine, bl
}

func isEquals(a, b []string) error {
	if len(a) != len(b) {
		return fmt.Errorf("len(a) != len(b) %d != %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			return fmt.Errorf(strings.Join(a, ",") + "\n" + strings.Join(b, ","))
		}
	}
	return nil
}

func Test_Engine_Reread_From_Begin(t *testing.T) {
	t.Run("no wait commit", func(t *testing.T) {
		test_Engine_Reread_From_Begin(t)
	})
}

func test_Engine_Reread_From_Begin(t *testing.T) {
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, nil)
	agg := &testAggregation{}
	n := 300
	for i := 0; i < n; i++ {
		data := make([]byte, 64)
		_, err := rand.Read(data)
		require.NoError(t, err)
		data = strconv.AppendInt(data, int64(i), 10)
		str := string(data)
		err = insertText(engine, str, false)
		require.NoError(t, err)
		agg.writeHistory = append(agg.writeHistory, str)
	}
	require.NoError(t, engine.Close())
	history := []string{}
	engine, _ = openEngine(t, dir, "db1", schema, false, false, false, func(s string) {
		history = append(history, s)
	})
	require.NoError(t, isEquals(agg.writeHistory, history))
	require.NoError(t, engine.Close())
}

func Test_Engine_Reread_From_Random_Place(t *testing.T) {
	// падает потому что при graceful shutdown я не делаю чекпоинт всего. Если делать то все будет зорошо
	t.SkipNow()
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, nil)
	agg := &testAggregation{}
	n := 5
	for i := 0; i < n; i++ {
		data := make([]byte, 20)
		_, err := rand.Read(data)
		require.NoError(t, err)
		str := strconv.FormatInt(int64(i), 10) + string(data)
		err = insertText(engine, str, false)
		require.NoError(t, err)
		agg.writeHistory = append(agg.writeHistory, str)
	}
	require.NoError(t, engine.Close())
	binlogHistory := []string{}
	engine, bl := openEngine(t, dir, "db2", schema, false, false, false, nil)
	binlogOffset := engine.rw.getDBOffsetLocked()
	textInDb := map[string]struct{}{}
	for _, s := range agg.writeHistory {
		textInDb[s] = struct{}{}
	}
	n = 5

	for i := 0; i < n; i++ {
		data := make([]byte, 1+rand.Int()%30)
		_, err := rand.Read(data)
		require.NoError(t, err)
		str := string(data)
		if _, ok := textInDb[str]; !ok {
			binlogOffset, err = bl.AppendASAP(binlogOffset, genBinlogEvent(str, nil))
			require.NoError(t, err)
			textInDb[str] = struct{}{}
			agg.writeHistory = append(agg.writeHistory, str)
			binlogHistory = append(binlogHistory, str)
		}
	}
	require.NoError(t, engine.Close())

	history := []string{}
	engine, _ = openEngine(t, dir, "db", schema, false, false, false, func(s string) {
		history = append(history, s)
	})
	expectedMap := map[string]struct{}{}
	for _, t := range agg.writeHistory {
		expectedMap[t] = struct{}{}
	}
	actualDb := map[string]struct{}{}
	err := engine.Do(context.Background(), "test", func(conn Conn, bytes []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT t from test_db")
		for rows.Next() {
			t, err := rows.ColumnBlobString(0)
			if err != nil {
				return bytes, err
			}
			actualDb[t] = struct{}{}
		}
		if rows.err != nil {
			return bytes, rows.err
		}
		return bytes, nil
	})
	require.NoError(t, err)
	require.NoError(t, isEquals(binlogHistory, history))
	require.True(t, reflect.DeepEqual(expectedMap, actualDb))
	require.NoError(t, engine.Close())
}

func Test_Engine_Read_Empty_Raw(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (id INTEGER PRIMARY KEY AUTOINCREMENT, oid INT, data TEXT);"
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, nil)
	var rowID int64
	var blob []byte
	var err error
	var isNull bool

	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		buf := make([]byte, 12)
		rowID, err = conn.Exec("test", "INSERT INTO test_db(oid) VALUES ($oid)", Int64("$oid", 1))
		binary.LittleEndian.PutUint32(buf, magic)
		binary.LittleEndian.PutUint64(buf[4:], uint64(1))
		return buf, err
	})
	require.NoError(t, err)

	_, err = engine.View(context.Background(), "test", func(conn Conn) error {
		rows := conn.Query("test", "SELECT data FROM test_db WHERE id=$id", Int64("$id", rowID))

		for rows.Next() {
			isNull = rows.ColumnIsNull(0)
			blob, err = rows.ColumnBlob(0, nil)
			if err != nil {
				return err
			}
		}
		return err
	})
	require.NoError(t, err)
	require.True(t, isNull)
	require.Nil(t, blob)
}

func Test_Engine_Put_Empty_String(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, nil)
	var err error
	var data = "abc"

	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", ""))
		return append(cache, 1), err
	})
	require.NoError(t, err)
	_, err = engine.View(context.Background(), "test", func(conn Conn) error {
		rows := conn.Query("test", "SELECT data from test_db")
		c := 0
		for rows.Next() {
			c++
		}
		require.Equal(t, 1, c)
		data, err = rows.ColumnBlobString(0)
		return rows.Error()
	})
	require.NoError(t, err)
	require.Equal(t, "", data)
	require.NoError(t, engine.Close())
}

func Test_Engine_NoBinlog(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	engine, err := OpenEngine(Options{
		Path:                         dir + "/db",
		APPID:                        32,
		Scheme:                       schema,
		CacheApproxMaxSizePerConnect: 1,
		ShowLastInsertID:             true,
	})
	require.NoError(t, err)
	var data = ""

	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", "abc"))
		return cache, err
	})
	require.NoError(t, err)
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT data from test_db")
		for rows.Next() {
			data, err = rows.ColumnBlobString(0)
		}

		return cache, rows.Error()
	})
	require.NoError(t, err)
	require.Equal(t, "abc", data)
	require.NoError(t, engine.Close())
}

func Test_Engine_NoBinlog_Close(t *testing.T) {
	// Падает потому что происходит откат вала так как нет бинлога вообще. Надо чекпоинт делать при gracefull shutdown или
	t.SkipNow()
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	engine, err := OpenEngine(Options{
		Path:                         dir + "/db",
		APPID:                        32,
		Scheme:                       schema,
		CacheApproxMaxSizePerConnect: 1,
		ShowLastInsertID:             true,
	})
	require.NoError(t, err)
	var data = ""

	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", "abc"))
		return cache, err
	})
	require.NoError(t, err)
	require.NoError(t, engine.Close())
	engine, err = OpenEngine(Options{
		Path:                         dir + "/db",
		APPID:                        32,
		Scheme:                       schema,
		CacheApproxMaxSizePerConnect: 1,
		ShowLastInsertID:             true,
	})
	require.NoError(t, err)
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT data from test_db")
		require.NoError(t, rows.Error())
		for rows.Next() {
			data, err = rows.ColumnBlobString(0)
		}
		return cache, err
	})
	require.NoError(t, err)
	require.Equal(t, "abc", data)
	require.NoError(t, engine.Close())
}

func Test_ReplicaMode(t *testing.T) {
	t.Skip("TODO: fix this test")
	const n = 10000
	dir := t.TempDir()
	engineMaster, _ := openEngine(t, dir, "db1", schema, true, false, false, nil)
	engineRepl, _ := openEngine(t, dir, "db", schema, false, true, false, nil)
	for i := 0; i < n; i++ {
		err := insertText(engineMaster, strconv.Itoa(i), false)
		require.NoError(t, err)
	}
	time.Sleep(5 * time.Second)
	c := 0
	err := engineRepl.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
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
	require.NoError(t, engineRepl.Close())
}

func Test_Engine_Put_And_Read_RO(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, nil)
	var err error
	var data = ""
	var read func(int) []string
	read = func(rec int) []string {
		var s []string
		_, err = engine.View(context.Background(), "test", func(conn Conn) error {
			if rec > 0 {
				s = append(s, read(rec-1)...)
			}
			rows := conn.Query("test", "SELECT data from test_db")
			for rows.Next() {
				data, err = rows.ColumnBlobString(0)
				s = append(s, data)
			}
			require.NoError(t, rows.Error())
			return nil
		})
		require.NoError(t, err)
		return s
	}

	t.Run("RO unshared can see committed data", func(t *testing.T) {
		err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
			_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", "abc"))
			return append(cache, 1), err
		})
		require.NoError(t, err)
		s := read(0)
		require.Len(t, s, 1)
		require.Contains(t, s, "abc")
	})

	t.Run("RO unshared can't see uncommitted data", func(t *testing.T) {
		data = ""
		err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
			s := read(0)
			require.Len(t, s, 1)
			require.Contains(t, s, "abc")
			_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", "def"))
			require.NoError(t, err)
			s = read(0)
			require.Len(t, s, 1)
			require.Contains(t, s, "abc")
			return append(cache, 1), err
		})
		require.NoError(t, err)
		s := read(0)
		require.Len(t, s, 2)
		require.Contains(t, s, "abc")
		require.Contains(t, s, "def")
	})

	t.Run("RO unshared can work concurrently", func(t *testing.T) {
		s := read(100 - 1)
		require.Len(t, s, 2*100)
		require.Contains(t, s, "abc")
		require.Contains(t, s, "def")
	})

	require.NoError(t, engine.Close())
}

func Test_Engine_Float64(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (val REAL);"
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, nil)
	var err error
	testValues := []float64{1.0, 6.0, math.MaxFloat64}

	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		for i := 0; i < len(testValues); i++ {
			_, err = conn.Exec("test", "INSERT INTO test_db(val) VALUES ($value)", Float64("$value", testValues[i]))
			require.NoError(t, err)
		}

		return append(cache, 1), err
	})
	require.NoError(t, err)
	count := 0
	result := make([]float64, 0, len(testValues))
	value := 0.0
	_, err = engine.View(context.Background(), "test", func(conn Conn) error {
		rows := conn.Query("test", "SELECT val FROM test_db WHERE val > $num", Float64("$num", 0.0))

		for rows.Next() {
			count++

			value = rows.ColumnFloat64(0)
			require.NoError(t, err)

			result = append(result, value)
		}
		return err
	})
	require.Equal(t, len(testValues), len(result))
	require.Equal(t, testValues, result)
}

func Test_Engine_Backup(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (id INTEGER);"
	var id int64
	dir := t.TempDir()
	dbPath := path.Join(dir, "db")
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, nil)
	var err error
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		buf := make([]byte, 12)
		_, err = conn.Exec("test", "INSERT INTO test_db(id) VALUES ($id)", Int64("$id", 1))
		binary.LittleEndian.PutUint32(buf, magic)
		binary.LittleEndian.PutUint64(buf[4:], uint64(1))
		return buf, err
	})
	require.NoError(t, err)
	backupPath, _, err := engine.Backup(context.Background(), path.Join(dir, "db1"))
	require.NoError(t, err)
	require.NoError(t, engine.Close())
	_ = os.Rename(checkpoint.CommitFileName(dbPath), checkpoint.CommitFileName(backupPath))

	dir, db := path.Split(backupPath)
	engine, _ = openEngine(t, dir, db, schema, false, false, false, nil)
	err = engine.Do(context.Background(), "test", func(conn Conn, b []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT id FROM test_db")
		for rows.Next() {
			id = rows.ColumnInt64(0)
			if err != nil {
				return nil, err
			}
		}
		return nil, rows.Error()
	})
	require.Equal(t, int64(1), id)
}

func Test_Engine_RO(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (id INTEGER);"
	var id int64
	dir := t.TempDir()
	dbfile := "db"
	engine, _ := openEngine(t, dir, dbfile, schema, true, false, false, nil)
	var err error
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		buf := make([]byte, 12)
		_, err = conn.Exec("test", "INSERT INTO test_db(id) VALUES ($id)", Int64("$id", 1))
		binary.LittleEndian.PutUint32(buf, magic)
		binary.LittleEndian.PutUint64(buf[4:], uint64(1))
		return buf, err
	})
	require.NoError(t, err)
	engineRO, err := OpenEngine(Options{
		Path:                         dir + "/" + dbfile,
		APPID:                        32,
		Scheme:                       schema,
		CacheApproxMaxSizePerConnect: 999,
		ReadOnly:                     true,
	})
	require.NoError(t, err)
	_, err = engineRO.View(context.Background(), "test", func(conn Conn) error {
		rows := conn.Query("test", "SELECT id FROM test_db")
		for rows.Next() {
			id = rows.ColumnInt64(0)
			if err != nil {
				return err
			}
		}
		return rows.Error()
	})
	require.Equal(t, int64(1), id)
	require.NoError(t, engine.Close())
	require.NoError(t, engineRO.Close())
}

func TestBrokenEngineCantWrite(t *testing.T) {
	d := t.TempDir()
	eng := createEngMaster(t, defaultTestEngineOptions(d))
	expectedErr := fmt.Errorf("some bad action")
	err := eng.engine.internalDo("__do", func(c internalConn) error {
		return expectedErr
	})
	require.Error(t, err)
	err = eng.put(context.Background(), 0, 0)
	require.ErrorIs(t, err, expectedErr)
	err = eng.engine.internalDo("__check", func(c internalConn) error {
		return nil
	})
	require.ErrorIs(t, err, expectedErr)
	require.ErrorIs(t, eng.engine.Close(), expectedErr)
}

func TestCanWriteAfterPanic(t *testing.T) {
	d := t.TempDir()
	eng := createEngMaster(t, defaultTestEngineOptions(d))
	defer eng.mustCloseGoodEngine(t)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	var k int64 = 1
	var v int64 = 2
	go func() {
		defer wg.Done()
		defer func() {
			require.NotNil(t, recover())
		}()
		_ = eng.engine.Do(context.Background(), "panic", func(c Conn, cache []byte) ([]byte, error) {
			_, _ = putConn(c, cache, k, 1)
			panic("oops")
		})
	}()
	wg.Wait()
	_, ok, err := eng.get(context.Background(), k)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, eng.put(context.Background(), k, v))
	actualV, ok, err := eng.get(context.Background(), k)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, v, actualV)
}

func TestCanReadAfterPanic(t *testing.T) {
	d := t.TempDir()
	opt := defaultTestEngineOptions(d)
	opt.maxRoConn = 1
	eng := createEngMaster(t, opt)
	defer eng.mustCloseGoodEngine(t)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			require.NotNil(t, recover())
		}()
		_, _ = eng.engine.View(context.Background(), "panic", func(c Conn) error {
			panic("oops")
		})
	}()
	wg.Wait()
	_, ok, err := eng.get(context.Background(), 0)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestCanWriteAfterInternalPanic(t *testing.T) {
	d := t.TempDir()
	eng := createEngMaster(t, defaultTestEngineOptions(d))
	defer eng.mustCloseErrorEngine(t, errEnginePanic)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	var k int64 = 1
	go func() {
		defer wg.Done()
		defer func() {
			require.NotNil(t, recover())
		}()
		_ = eng.engine.internalDo("__panic", func(c internalConn) error {
			_, _ = putConn(c.Conn, nil, k, 1)
			panic("oops")
		})
	}()
	wg.Wait()
	_, ok, err := eng.get(context.Background(), k)
	require.NoError(t, err)
	require.False(t, ok)
	err = eng.put(context.Background(), 0, 0)
	require.Error(t, err)
}

func TestInternalDoMustErrorWithBadName(t *testing.T) {
	d := t.TempDir()
	eng := createEngMaster(t, defaultTestEngineOptions(d))
	defer eng.mustCloseGoodEngine(t)
	err := eng.engine.internalDo("a", func(c internalConn) error {
		return nil
	})
	require.Error(t, err)
}

func TestDoMustErrorWithBadName(t *testing.T) {
	d := t.TempDir()
	eng := createEngMaster(t, defaultTestEngineOptions(d))
	defer eng.mustCloseGoodEngine(t)
	err := eng.engine.Do(context.Background(), "__a", func(c Conn, cache []byte) ([]byte, error) {
		return cache, nil
	})
	require.Error(t, err)
}

func Test_Engine_Slice_Params(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (id INTEGER PRIMARY KEY, oid INT);"
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, nil)
	var err error
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("test", "INSERT INTO test_db(oid) VALUES ($oid)", Int64("$oid", 1))
		require.NoError(t, err)
		_, err = conn.Exec("test", "INSERT INTO test_db(oid) VALUES ($oid)", Int64("$oid", 2))
		require.NoError(t, err)
		_, err = conn.Exec("test", "INSERT INTO test_db(oid) VALUES ($oid)", Int64("$oid", 3))
		require.NoError(t, err)
		return append(cache, 1), err
	})
	require.NoError(t, err)
	count := 0
	_, err = engine.View(context.Background(), "test", func(conn Conn) error {
		rows := conn.Query("test", "SELECT oid FROM test_db WHERE oid in($ids$) or oid in($ids1$)",
			Int64Slice("$ids$", []int64{1, 2}),
			Int64Slice("$ids1$", []int64{3}))

		for rows.Next() {
			count++
		}
		rows = conn.Query("test", "SELECT oid FROM test_db WHERE oid in($ids$) or oid in($ids1$)",
			Int64Slice("$ids$", []int64{1, 2, 3}),
			Int64Slice("$ids1$", []int64{3}))

		for rows.Next() {
			count++
		}
		return err
	})
	require.Equal(t, 3*2, count)
	require.NoError(t, err)
}

func Test_Engine_WaitCommit(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (id INTEGER PRIMARY KEY);"
	var engine *Engine
	binlogData := make([]byte, 1024)
	waitOffsetView := func(ctx context.Context, offset int64) (id int64, offsetView int64, _ error) {
		res, err := engine.ViewOpts(ctx, ViewTxOptions{
			QueryName:  "blocked",
			WaitOffset: offset,
		}, func(conn Conn) error {
			rows := conn.Query("select", "SELECT id FROM test_db")
			if rows.Next() {
				id = rows.ColumnInt64(0)
			} else {
				return fmt.Errorf("table is empty")
			}
			return rows.Error()
		})
		return id, res.DBOffset, err
	}
	t.Run("wait_commit_should_timeout", func(t *testing.T) {
		engine, _ = openEngine(t, t.TempDir(), "db", schema, true, false, false, nil)
		ch := make(chan error)
		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*50)
		go func() {
			_, _, err := waitOffsetView(ctx, 1024)
			ch <- err
			close(ch)
		}()
		err := <-ch
		require.ErrorIs(t, err, ctx.Err())
	})
	t.Run("wait_commit_should_finish_1", func(t *testing.T) {
		engine, _ = openEngine(t, t.TempDir(), "db", schema, true, false, false, nil)
		var id int64
		var offsetView int64
		ch := make(chan error)
		go func() {
			var err error
			id, offsetView, err = waitOffsetView(context.Background(), 1024)
			ch <- err
			close(ch)
		}()
		time.Sleep(time.Millisecond)
		res, err := engine.Dov2(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
			_, err := conn.Exec("test", "INSERT INTO test_db(oid) VALUES ($oid)", Int64("$oid", 1))
			return append(cache, binlogData...), err
		})
		offsetAfterWrite := res.DBOffset
		require.NoError(t, err)
		err = <-ch
		require.NoError(t, err)
		require.Equal(t, offsetAfterWrite, offsetView)
		require.Equal(t, int64(1), id)

	})
	t.Run("wait_commit_should_finish_2", func(t *testing.T) {
		engine, _ = openEngine(t, t.TempDir(), "db", schema, true, false, false, nil)
		var id int64
		var offsetView int64
		ch := make(chan error)
		res, err := engine.Dov2(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
			var err error
			id, err = conn.Exec("test", "INSERT INTO test_db(oid) VALUES ($oid)", Int64("$oid", 1))
			return append(cache, binlogData...), err
		})
		offsetAfterWrite := res.DBOffset
		require.NoError(t, err)
		go func() {
			var err error
			id, offsetView, err = waitOffsetView(context.Background(), 1024)
			ch <- err
			close(ch)
		}()
		err = <-ch
		require.NoError(t, err)
		require.Equal(t, offsetAfterWrite, offsetView)
		require.Equal(t, int64(1), id)
	})
}

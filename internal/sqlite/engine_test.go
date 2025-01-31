// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	binlog2 "github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"
	"pgregory.net/rand"
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

func apply(t *testing.T, scanOnly bool, applyF func(string2 string)) func(conn Conn, offset int64, bytes []byte) (int, error) {
	return func(conn Conn, offset int64, bytes []byte) (int, error) {
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
			if !scanOnly {
				_, err = conn.Exec("test", "INSERT INTO test_db(t) VALUES ($t)", BlobString("$t", string(str)))
				require.NoError(t, err)
			}
			if applyF != nil && !scanOnly {
				applyF(string(str))
			}

			offset := fsbinlog.AddPadding(4 + 4 + int(n))
			read += offset
			bytes = bytes[offset:]
		}
		return fsbinlog.AddPadding(read), nil
	}

}

func openEngine(t *testing.T, prefix string, dbfile, schema string, create, replica, readAndExit, commitOnEachWrite bool, mode DurabilityMode, applyF func(string2 string)) (*Engine, binlog2.Binlog) {
	options := fsbinlog.Options{
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
		Path:                   prefix + "/" + dbfile,
		APPID:                  32,
		Scheme:                 schema,
		DurabilityMode:         mode,
		Replica:                replica,
		ReadAndExit:            readAndExit,
		CommitOnEachWrite:      commitOnEachWrite,
		CacheMaxSizePerConnect: 1,
	}, bl, apply(t, false, applyF), apply(t, true, applyF))
	require.NoError(t, err)
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
		test_Engine_Reread_From_Begin(t, false, false)
	})
	t.Run("no wait commit and commit on each tx", func(t *testing.T) {
		test_Engine_Reread_From_Begin(t, false, true)
	})
	t.Run("wait commit", func(t *testing.T) {
		test_Engine_Reread_From_Begin(t, true, false)
	})

}

func test_Engine_Reread_From_Begin(t *testing.T, waitCommit, commitOnEachWrite bool) {
	dir := t.TempDir()
	mode := NoWaitCommit
	if waitCommit {
		mode = WaitCommit
	}
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, commitOnEachWrite, mode, nil)
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

	require.NoError(t, engine.Close(context.Background()))
	history := []string{}
	engine, _ = openEngine(t, dir, "db1", schema, false, false, false, commitOnEachWrite, mode, func(s string) {
		history = append(history, s)
	})
	require.NoError(t, isEquals(agg.writeHistory, history))
	require.NoError(t, engine.Close(context.Background()))
}

func Test_Engine_Reread_From_Random_Place(t *testing.T) {
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, false, NoWaitCommit, nil)
	agg := &testAggregation{}
	n := 500
	for i := 0; i < n; i++ {
		data := make([]byte, 20)
		_, err := rand.Read(data)
		require.NoError(t, err)
		str := strconv.FormatInt(int64(i), 10) + string(data)
		err = insertText(engine, str, false)
		require.NoError(t, err)
		agg.writeHistory = append(agg.writeHistory, str)
	}
	require.NoError(t, engine.Close(context.Background()))
	binlogHistory := []string{}
	engine, bl := openEngine(t, dir, "db2", schema, false, false, false, false, NoWaitCommit, nil)
	binlogOffset := engine.dbOffset
	textInDb := map[string]struct{}{}
	for _, s := range agg.writeHistory {
		textInDb[s] = struct{}{}
	}
	n = 5000

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
	require.NoError(t, engine.Close(context.Background()))

	history := []string{}
	engine, _ = openEngine(t, dir, "db", schema, false, false, false, false, WaitCommit, func(s string) {
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
	require.NoError(t, engine.Close(context.Background()))
}

func Test_Engine_Read_Empty_Raw(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (id INTEGER PRIMARY KEY AUTOINCREMENT, oid INT, data TEXT);"
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, false, WaitCommit, nil)
	var rowID int64
	var blob []byte
	var err error
	var isNull bool

	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		buf := make([]byte, 12)
		rowID, err = conn.Exec("test", "INSERT INTO test_db(oid) VALUES ($oid)", Int64("$oid", 1))
		binary.LittleEndian.PutUint32(buf, magic)
		binary.LittleEndian.PutUint64(buf[4:], uint64(1))
		return cache, err
	})
	require.NoError(t, err)

	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT data FROM test_db WHERE id=$id or id=$id1", Int64("$id", rowID))

		for rows.Next() {
			isNull = rows.ColumnIsNull(0)
			blob, err = rows.ColumnBlob(0, nil)
			if err != nil {
				return cache, err
			}
		}
		return cache, err
	})
	require.NoError(t, err)
	require.True(t, isNull)
	require.Nil(t, blob)
}

func Test_Engine_Put_Empty_String(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, false, WaitCommit, nil)
	var err error
	var data = "abc"

	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", ""))
		return cache, err
	})
	require.NoError(t, err)
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT data from test_db")
		require.NoError(t, rows.Error())
		require.True(t, rows.Next())
		data, err = rows.ColumnBlobString(0)
		return cache, err
	})
	require.NoError(t, err)
	require.Equal(t, "", data)
	require.NoError(t, engine.Close(context.Background()))
}

func Test_Engine_NoBinlog(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	engine, err := OpenEngine(Options{
		Path:                   dir + "/db",
		APPID:                  32,
		Scheme:                 schema,
		DurabilityMode:         NoBinlog,
		CacheMaxSizePerConnect: 1,
	}, nil, nil, nil)
	require.NoError(t, err)
	var data = ""

	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", "abc"))
		return cache, err
	})
	require.NoError(t, err)
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT data from test_db")
		require.NoError(t, rows.Error())
		require.True(t, rows.Next())
		data, err = rows.ColumnBlobString(0)
		return cache, err
	})
	require.NoError(t, err)
	require.Equal(t, "abc", data)
	require.NoError(t, engine.Close(context.Background()))
}

func Test_Engine_NoBinlog_Close(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	engine, err := OpenEngine(Options{
		Path:                   dir + "/db",
		APPID:                  32,
		Scheme:                 schema,
		DurabilityMode:         NoBinlog,
		CacheMaxSizePerConnect: 1,
	}, nil, nil, nil)
	require.NoError(t, err)
	var data = ""

	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", "abc"))
		return cache, err
	})
	require.NoError(t, err)
	require.NoError(t, engine.Close(context.Background()))
	engine, err = OpenEngine(Options{
		Path:                   dir + "/db",
		APPID:                  32,
		Scheme:                 schema,
		DurabilityMode:         NoBinlog,
		CacheMaxSizePerConnect: 1,
	}, nil, nil, nil)
	require.NoError(t, err)
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT data from test_db")
		require.NoError(t, rows.Error())
		require.True(t, rows.Next())
		data, err = rows.ColumnBlobString(0)
		return cache, err
	})
	require.NoError(t, err)
	require.Equal(t, "abc", data)
	require.NoError(t, engine.Close(context.Background()))
}

func Test_ReplicaMode(t *testing.T) {
	t.Skip("TODO: fix this test")
	const n = 10000
	dir := t.TempDir()
	engineMaster, _ := openEngine(t, dir, "db1", schema, true, false, false, false, NoWaitCommit, nil)
	engineRepl, _ := openEngine(t, dir, "db", schema, false, true, false, false, NoWaitCommit, nil)
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
	require.NoError(t, engineMaster.Close(context.Background()))
	require.NoError(t, engineRepl.Close(context.Background()))
}

func Test_Engine_Put_And_Read_RO(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, false, WaitCommit, nil)
	var err error
	var data = ""
	var read func(int) []string
	read = func(rec int) []string {
		var s []string
		err = engine.View(context.Background(), "test", func(conn Conn) error {
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

	t.Run("RO unshared can see commited data", func(t *testing.T) {
		err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
			_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", "abc"))
			return cache, err
		})
		require.NoError(t, err)
		engine.commitTXAndStartNew(true, true)
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
			return cache, err
		})
		require.NoError(t, err)
		engine.commitTXAndStartNew(true, true)
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
	require.NoError(t, engine.Close(context.Background()))
}

func Test_Engine_Slice_Params(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (id INTEGER PRIMARY KEY, oid INT);"
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, false, NoBinlog, nil)
	var err error

	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("test", "INSERT INTO test_db(oid) VALUES ($oid)", Int64("$oid", 1))
		require.NoError(t, err)
		_, err = conn.Exec("test", "INSERT INTO test_db(oid) VALUES ($oid)", Int64("$oid", 2))
		require.NoError(t, err)
		_, err = conn.Exec("test", "INSERT INTO test_db(oid) VALUES ($oid)", Int64("$oid", 3))
		require.NoError(t, err)
		return cache, err
	})
	require.NoError(t, err)
	count := 0
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
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
		return cache, err
	})
	require.Equal(t, 3*2, count)
	require.NoError(t, err)
}

func Test_Engine_Float64(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (val REAL);"
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, false, NoBinlog, nil)
	var err error
	testValues := []float64{1.0, 6.0, math.MaxFloat64}

	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		for i := 0; i < len(testValues); i++ {
			_, err = conn.Exec("test", "INSERT INTO test_db(val) VALUES ($value)", Float64("$value", testValues[i]))
			require.NoError(t, err)
		}

		return cache, err
	})
	require.NoError(t, err)
	count := 0
	result := make([]float64, 0, len(testValues))
	value := 0.0
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT val FROM test_db WHERE val > $num", Float64("$num", 0.0))

		for rows.Next() {
			count++

			value, err = rows.ColumnFloat64(0)
			require.NoError(t, err)

			result = append(result, value)
		}
		return cache, err
	})
	require.Equal(t, len(testValues), len(result))
	require.Equal(t, testValues, result)
	require.NoError(t, engine.Close(context.Background()))
}

func Test_Engine_Backup(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (id INTEGER);"
	var id int64
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, false, WaitCommit, nil)
	var err error
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		buf := make([]byte, 12)
		_, err = conn.Exec("test", "INSERT INTO test_db(id) VALUES ($id)", Int64("$id", 1))
		binary.LittleEndian.PutUint32(buf, magic)
		binary.LittleEndian.PutUint64(buf[4:], uint64(1))
		return cache, err
	})
	require.NoError(t, err)
	require.NoError(t, engine.commitTXAndStartNew(true, true))
	backupPath, _, err := engine.Backup(context.Background(), path.Join(dir, "db1"))
	require.NoError(t, err)
	require.NoError(t, engine.Close(context.Background()))
	dir, db := path.Split(backupPath)
	engine, _ = openEngine(t, dir, db, schema, false, false, false, false, WaitCommit, nil)
	err = engine.Do(context.Background(), "test", func(conn Conn, b []byte) ([]byte, error) {
		rows := conn.Query("test", "SELECT id FROM test_db")
		for rows.Next() {
			id, err = rows.ColumnInt64(0)
			if err != nil {
				return nil, err
			}
		}
		return nil, rows.Error()
	})
	require.Equal(t, int64(1), id)
}

func Test_Engine_OpenRO(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (id INTEGER);"
	var id int64
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, false, WaitCommit, nil)
	var err error
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("test", "INSERT INTO test_db(id) VALUES ($id)", Int64("$id", 1))
		return cache, err
	})
	require.NoError(t, err)
	require.NoError(t, engine.commitTXAndStartNew(true, true))
	backupPath, _, err := engine.Backup(context.Background(), path.Join(dir, "db1"))
	require.NoError(t, err)
	require.NoError(t, engine.Close(context.Background()))
	engineRO, err := OpenRO(Options{
		Path: backupPath,
	})
	require.NoError(t, err)
	err = engineRO.View(context.Background(), "test", func(conn Conn) error {
		rows := conn.Query("test", "SELECT id FROM test_db")
		for rows.Next() {
			id, err = rows.ColumnInt64(0)
			if err != nil {
				return err
			}
		}
		return rows.Error()
	})
	require.Equal(t, int64(1), id)
	require.NoError(t, engineRO.Close(context.Background()))
}

func Test_Engine_OpenRO_Journal_Wal(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (id INTEGER);"
	var id int64
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, false, WaitCommit, nil)
	var err error
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("test", "INSERT INTO test_db(id) VALUES ($id)", Int64("$id", 1))
		return cache, err
	})
	require.NoError(t, err)
	require.NoError(t, engine.commitTXAndStartNew(true, true))
	engineRO, err := OpenRO(Options{
		Path: path.Join(dir, "db"),
	})
	require.NoError(t, err)
	err = engineRO.View(context.Background(), "test", func(conn Conn) error {
		rows := conn.Query("test", "SELECT id FROM test_db")
		for rows.Next() {
			id, err = rows.ColumnInt64(0)
			if err != nil {
				return err
			}
		}
		return rows.Error()
	})
	require.Equal(t, int64(1), id)
	require.NoError(t, engine.Close(context.Background()))
	require.NoError(t, engineRO.Close(context.Background()))
}

func Test_Engine_OpenROWal(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (id INTEGER);"
	var id int64
	dir := t.TempDir()
	dbfile := "db"
	engine, _ := openEngine(t, dir, dbfile, schema, true, false, false, false, WaitCommit, nil)
	var err error
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		buf := make([]byte, 12)
		_, err = conn.Exec("test", "INSERT INTO test_db(id) VALUES ($id)", Int64("$id", 1))
		binary.LittleEndian.PutUint32(buf, magic)
		binary.LittleEndian.PutUint64(buf[4:], uint64(1))
		return cache, err
	})
	require.NoError(t, err)
	require.NoError(t, engine.commitTXAndStartNew(true, true))
	engineRO, err := OpenROWal(Options{
		Path:                   dir + "/" + dbfile,
		APPID:                  32,
		Scheme:                 schema,
		CacheMaxSizePerConnect: 999,
	})
	require.NoError(t, err)
	err = engineRO.View(context.Background(), "test", func(conn Conn) error {
		rows := conn.Query("test", "SELECT id FROM test_db")
		for rows.Next() {
			id, err = rows.ColumnInt64(0)
			if err != nil {
				return err
			}
		}
		return rows.Error()
	})
	require.Equal(t, int64(1), id)
	require.NoError(t, engine.Close(context.Background()))
	require.NoError(t, engineRO.Close(context.Background()))
}

func Test_Engine_Bad_Context(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (id INTEGER);"
	dir := t.TempDir()
	dbfile := "db"
	engine, _ := openEngine(t, dir, dbfile, schema, true, false, false, false, WaitCommit, nil)
	var err error
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = engine.Do(ctx, "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("test", "INSERT INTO test_db(id) VALUES ($id)", Int64("$id", 1))
		return cache, err
	})
	require.Error(t, err)
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("test", "INSERT INTO test_db(id) VALUES ($id)", Int64("$id", 1))
		return cache, err
	})
	require.NoError(t, err)
	require.NoError(t, engine.Close(context.Background()))
}

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"

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

func insertText(e *Engine, s string) error {
	return e.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		_, err := conn.Exec("test", "INSERT INTO test_db(t) VALUES ($t)", BlobString("$t", s))

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
		Path:              prefix + "/" + dbfile,
		APPID:             32,
		Scheme:            schema,
		DurabilityMode:    mode,
		Replica:           replica,
		ReadAndExit:       readAndExit,
		CommitOnEachWrite: commitOnEachWrite,
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
	engine, bl := openEngine(t, dir, "db", schema, true, false, false, commitOnEachWrite, mode, nil)
	agg := &testAggregation{}
	n := 100000
	if waitCommit {
		n = 300
	}
	wg := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := make([]byte, 1+rand.Int()%300)
			_, err := rand.Read(data)
			require.NoError(t, err)
			agg.mx.Lock()
			str := string(data)
			err = insertText(engine, str)
			if err == nil {
				agg.writeHistory = append(agg.writeHistory, str)
			}
			agg.mx.Unlock()
		}()
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))
	require.NoError(t, bl.Shutdown())
	history := []string{}
	mx := sync.Mutex{}
	engine, bl = openEngine(t, dir, "db1", schema, false, false, false, commitOnEachWrite, mode, func(s string) {
		mx.Lock()
		defer mx.Unlock()
		history = append(history, s)
	})
	mx.Lock()
	defer mx.Unlock()
	agg.mx.Lock()
	defer agg.mx.Unlock()
	require.NoError(t, isEquals(agg.writeHistory, history))
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))
	require.NoError(t, bl.Shutdown())
}

func Test_Engine_Reread_From_Random_Place(t *testing.T) {
	dir := t.TempDir()
	engine, bl := openEngine(t, dir, "db", schema, true, false, false, false, WaitCommit, nil)
	agg := &testAggregation{}
	n := 500 + rand.Intn(500)
	wg := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := make([]byte, 1+rand.Intn(30))
			_, err := rand.Read(data)
			require.NoError(t, err)
			agg.mx.Lock()
			str := string(data)
			err = insertText(engine, str)
			if err == nil {
				agg.writeHistory = append(agg.writeHistory, str)
			}
			agg.mx.Unlock()
		}()
	}
	wg.Wait()
	require.NoError(t, bl.Shutdown())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))
	binlogHistory := []string{}
	engine, bl = openEngine(t, dir, "db2", schema, false, false, false, false, NoWaitCommit, nil)
	binlogOffset := engine.dbOffset
	textInDb := map[string]struct{}{}
	for _, s := range agg.writeHistory {
		textInDb[s] = struct{}{}
	}
	n = 5000 + rand.Intn(1000)

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
	require.NoError(t, bl.Shutdown())
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))

	history := []string{}
	mx := sync.Mutex{}
	engine, bl = openEngine(t, dir, "db", schema, false, false, false, false, WaitCommit, func(s string) {
		mx.Lock()
		defer mx.Unlock()
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
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))
	require.NoError(t, bl.Shutdown())
}

func Test_Engine(t *testing.T) {
	dir := t.TempDir()
	engine, bl := openEngine(t, dir, "db", schema, true, false, false, false, WaitCommit, nil)
	agg := &testAggregation{}
	n := 8000
	var task int32 = 10000000
	var count int32
	wg := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				c := atomic.AddInt32(&count, 1)
				if c < task {
					break
				}
				data := make([]byte, 1+rand.Int()%20)
				_, err := rand.Read(data)
				require.NoError(t, err)
				str := string(data)
				err = insertText(engine, str)
				if err == nil {
					agg.mx.Lock()
					agg.writeHistory = append(agg.writeHistory, str)
					agg.mx.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))
	require.NoError(t, bl.Shutdown())
	mx := sync.Mutex{}
	engine, bl = openEngine(t, dir, "db", schema, false, false, false, false, WaitCommit, func(s string) {
		t.Fatal("mustn't apply music")
	})
	mx.Lock()
	defer mx.Unlock()
	agg.mx.Lock()
	defer agg.mx.Unlock()
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
	require.NoError(t, bl.Shutdown())
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))
}

func Test_Engine_NoBinlog(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	engine, err := OpenEngine(Options{
		Path:           dir + "/db",
		APPID:          32,
		Scheme:         schema,
		DurabilityMode: NoBinlog,
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))
}

func Test_Engine_NoBinlog_Close(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	engine, err := OpenEngine(Options{
		Path:           dir + "/db",
		APPID:          32,
		Scheme:         schema,
		DurabilityMode: NoBinlog,
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
		Path:           dir + "/db",
		APPID:          32,
		Scheme:         schema,
		DurabilityMode: NoBinlog,
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))
}

func Test_ReplicaMode(t *testing.T) {
	t.Skip("TODO: fix this test")
	const n = 10000
	dir := t.TempDir()
	engineMaster, _ := openEngine(t, dir, "db1", schema, true, false, false, false, NoWaitCommit, nil)
	engineRepl, _ := openEngine(t, dir, "db", schema, false, true, false, false, NoWaitCommit, nil)
	for i := 0; i < n; i++ {
		err := insertText(engineMaster, strconv.Itoa(i))
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engineMaster.Close(ctx))
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engineRepl.Close(ctx))
}

func Test_Engine_Put_And_Read_RO(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, false, false, false, WaitCommit, nil)
	var err error
	var data = ""
	var read func(bool, int) []string
	read = func(share bool, rec int) []string {
		var s []string
		view := engine.ViewCommitted
		if share {
			view = engine.ViewUncommitted
		}
		err = view(context.Background(), "test", func(conn Conn) error {
			if rec > 0 {
				s = append(s, read(share, rec-1)...)
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
		s := read(false, 0)
		require.Len(t, s, 1)
		require.Contains(t, s, "abc")
	})

	t.Run("RO unshared can't see uncommitted data", func(t *testing.T) {
		data = ""
		err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
			s := read(false, 0)
			require.Len(t, s, 1)
			require.Contains(t, s, "abc")
			_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", "def"))
			require.NoError(t, err)
			s = read(false, 0)
			require.Len(t, s, 1)
			require.Contains(t, s, "abc")
			return cache, err
		})
		require.NoError(t, err)
		engine.commitTXAndStartNew(true, true)
		s := read(false, 0)
		require.Len(t, s, 2)
		require.Contains(t, s, "abc")
		require.Contains(t, s, "def")
	})

	t.Run("RO shared can see uncommitted data", func(t *testing.T) {
		data = ""
		err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
			s := read(true, 0)
			require.Len(t, s, 2)
			require.Contains(t, s, "abc")
			require.Contains(t, s, "def")
			_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", "ggg"))
			require.NoError(t, err)
			s = read(true, 0)
			require.Len(t, s, 3)
			require.Contains(t, s, "abc")
			require.Contains(t, s, "def")
			require.Contains(t, s, "ggg")
			return cache, err
		})
		require.NoError(t, err)
		engine.commitTXAndStartNew(true, true)
		s := read(false, 0)
		require.Len(t, s, 3)
		require.Contains(t, s, "abc")
		require.Contains(t, s, "def")
		require.Contains(t, s, "ggg")
	})

	t.Run("RO unshared can work concurrently", func(t *testing.T) {
		s := read(false, maxROConn-1)
		require.Len(t, s, 3*maxROConn)
		require.Contains(t, s, "abc")
		require.Contains(t, s, "def")
		require.Contains(t, s, "ggg")

	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engine.Close(ctx))
}

func Test_ReadAndExit(t *testing.T) {
	const n = 1000
	dir := t.TempDir()
	engineMaster, _ := openEngine(t, dir, "db", schema, true, false, false, false, WaitCommit, nil)
	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := insertText(engineMaster, strconv.Itoa(i))
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	require.NoError(t, engineMaster.Close(ctx))
	engineMaster, _ = openEngine(t, dir, "db1", schema, false, false, true, false, NoBinlog, nil)
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

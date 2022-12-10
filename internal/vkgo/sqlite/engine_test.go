// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

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
	return e.Do(func(conn Conn, cache []byte) ([]byte, error) {
		_, err := conn.Exec("INSERT INTO test_db(t) VALUES ($t)", BlobString("$t", s))

		return genBinlogEvent(s, cache), err
	}, false)
}

func openEngine(t *testing.T, prefix string, dbfile, schema string, create bool, applyF func(string2 string)) (*Engine, binlog2.Binlog) {
	options := binlog2.Options{
		PrefixPath: prefix + "/test",
		Magic:      3456,
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
	}, bl, func(conn Conn, offset int64, bytes []byte) (int, error) {
		read := 0
		for len(bytes) > 0 {
			if len(bytes) < 4 {
				return fsbinlog.AddPadding(read), binlog2.ErrorNotEnoughData
			}
			var mark uint32
			mark, _, err = basictl.NatReadTag(bytes)
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
			_, err = conn.Exec("INSERT INTO test_db(t) VALUES ($t)", BlobString("$t", string(str)))
			require.NoError(t, err)
			if applyF != nil {
				applyF(string(str))
			}
			offset := fsbinlog.AddPadding(4 + 4 + int(n))
			read += offset
			bytes = bytes[offset:]
		}
		return fsbinlog.AddPadding(read), nil
	})
	require.NoError(t, err)
	return engine, bl
}

func isEquals(a, b []string) error {
	if len(a) != len(b) {
		return fmt.Errorf(strings.Join(a, ",") + "\n" + strings.Join(b, ","))
	}
	for i := range a {
		if a[i] != b[i] {
			return fmt.Errorf(strings.Join(a, ",") + "\n" + strings.Join(b, ","))
		}
	}
	return nil
}

func Test_Engine_Reread_From_Begin(t *testing.T) {
	dir := t.TempDir()
	engine, bl := openEngine(t, dir, "db", schema, true, nil)
	agg := &testAggregation{}
	n := 30000
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
	require.NoError(t, engine.Close())
	require.NoError(t, bl.Shutdown())
	history := []string{}
	mx := sync.Mutex{}
	engine, bl = openEngine(t, dir, "db1", schema, false, func(s string) {
		mx.Lock()
		defer mx.Unlock()
		history = append(history, s)
	})
	mx.Lock()
	defer mx.Unlock()
	agg.mx.Lock()
	defer agg.mx.Unlock()
	require.NoError(t, isEquals(agg.writeHistory, history))
	require.NoError(t, engine.Close())
	require.NoError(t, bl.Shutdown())
}

func Test_Engine_Reread_From_Random_Place(t *testing.T) {
	dir := t.TempDir()
	engine, bl := openEngine(t, dir, "db", schema, true, nil)
	agg := &testAggregation{}
	n := 20000 + rand.Intn(20000)
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
	binlogOffset := engine.dbOffset
	require.NoError(t, engine.Close())
	textInDb := map[string]struct{}{}
	for _, s := range agg.writeHistory {
		textInDb[s] = struct{}{}
	}
	n = 20000 + rand.Intn(20000)
	binlogHistory := []string{}

	for i := 0; i < n; i++ {
		data := make([]byte, 1+rand.Int()%30)
		_, err := rand.Read(data)
		require.NoError(t, err)
		str := string(data)
		if _, ok := textInDb[str]; !ok {
			binlogOffset, err = bl.Append(binlogOffset, genBinlogEvent(str, nil))
			require.NoError(t, err)
			textInDb[str] = struct{}{}
			agg.writeHistory = append(agg.writeHistory, str)
			binlogHistory = append(binlogHistory, str)
		}
	}
	require.NoError(t, bl.Shutdown())
	history := []string{}
	mx := sync.Mutex{}
	engine, bl = openEngine(t, dir, "db", schema, false, func(s string) {
		mx.Lock()
		defer mx.Unlock()
		history = append(history, s)
	})
	expectedMap := map[string]struct{}{}
	for _, t := range agg.writeHistory {
		expectedMap[t] = struct{}{}
	}
	actualDb := map[string]struct{}{}
	err := engine.Do(func(conn Conn, bytes []byte) ([]byte, error) {
		rows := conn.Query("SELECT t from test_db")
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
	}, true)
	require.NoError(t, err)
	require.NoError(t, isEquals(binlogHistory, history))
	require.True(t, reflect.DeepEqual(expectedMap, actualDb))
	require.NoError(t, engine.Close())
	require.NoError(t, bl.Shutdown())
}

func Test_Engine(t *testing.T) {
	dir := t.TempDir()
	engine, bl := openEngine(t, dir, "db", schema, true, nil)
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
	require.NoError(t, engine.Close())
	require.NoError(t, bl.Shutdown())
	mx := sync.Mutex{}
	engine, bl = openEngine(t, dir, "db", schema, false, func(s string) {
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
	err := engine.Do(func(conn Conn, bytes []byte) ([]byte, error) {
		rows := conn.Query("SELECT t from test_db")
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
	}, true)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(expectedMap, actualDb))
	require.NoError(t, engine.Close())
	require.NoError(t, bl.Shutdown())
}

func Test_Engine_Read_Empty_Raw(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (id INTEGER PRIMARY KEY AUTOINCREMENT, oid INT, data TEXT);"
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, nil)
	var rowID int64
	var blob []byte
	var err error
	var isNull bool

	err = engine.Do(func(conn Conn, cache []byte) ([]byte, error) {
		buf := make([]byte, 12)
		rowID, err = conn.Exec("INSERT INTO test_db(oid) VALUES ($oid)", Int64("$oid", 1))
		binary.LittleEndian.PutUint32(buf, magic)
		binary.LittleEndian.PutUint64(buf[4:], uint64(1))
		return cache, err
	}, false)
	require.NoError(t, err)

	err = engine.Do(func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("SELECT data FROM test_db WHERE id=$id or id=$id1", Int64("$id", rowID))

		for rows.Next() {
			isNull = rows.ColumnIsNull(0)
			blob, err = rows.ColumnBlob(0, nil)
			if err != nil {
				return cache, err
			}
		}
		return cache, err
	}, false)
	require.NoError(t, err)
	require.True(t, isNull)
	require.Nil(t, blob)
}

func Test_Engine_Put_Empty_String(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	engine, _ := openEngine(t, dir, "db", schema, true, nil)
	var err error
	var data = "abc"

	err = engine.Do(func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", ""))
		return cache, err
	}, false)
	require.NoError(t, err)
	err = engine.Do(func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("SELECT data from test_db")
		require.NoError(t, rows.Error())
		require.True(t, rows.Next())
		data, err = rows.ColumnBlobString(0)
		return cache, err
	}, false)
	require.NoError(t, err)
	require.Equal(t, "", data)
}

func Test_Engine_WithoutBinlog(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data TEXT NOT NULL);"
	dir := t.TempDir()
	engine, err := OpenEngine(Options{
		Path:   dir + "/db",
		APPID:  32,
		Scheme: schema,
	}, nil, nil)
	require.NoError(t, err)
	var data = ""

	err = engine.Do(func(conn Conn, cache []byte) ([]byte, error) {
		_, err = conn.Exec("INSERT INTO test_db(data) VALUES ($data)", BlobString("$data", "abc"))
		return cache, err
	}, false)
	require.NoError(t, err)
	err = engine.Do(func(conn Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("SELECT data from test_db")
		require.NoError(t, rows.Error())
		require.True(t, rows.Next())
		data, err = rows.ColumnBlobString(0)
		return cache, err
	}, false)
	require.NoError(t, err)
	require.Equal(t, "abc", data)
}

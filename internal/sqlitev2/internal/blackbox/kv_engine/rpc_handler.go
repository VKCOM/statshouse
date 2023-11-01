package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlkv_engine"
	"github.com/vkcom/statshouse/internal/sqlitev2"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	binlog2 "github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"
)

type rpc_handler struct {
	readyCh <-chan error
	engine  *sqlitev2.Engine
}

const schemeKV = "CREATE TABLE IF NOT EXISTS kv (k INTEGER PRIMARY KEY, v INTEGER);"
const magic = 3456

func genBinlogNumberEvent(k, v int64, cache []byte) []byte {
	cache = binary.LittleEndian.AppendUint32(cache, magic)
	cache = binary.LittleEndian.AppendUint64(cache, uint64(k))
	return binary.LittleEndian.AppendUint64(cache, uint64(v))
}

func apply(conn sqlitev2.Conn, payload []byte) (int, error) {
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
		if len(payload) < 8 {
			return fsbinlog.AddPadding(read), binlog2.ErrorNotEnoughData
		}
		k := binary.LittleEndian.Uint64(payload[4:12])
		v := binary.LittleEndian.Uint64(payload[12:20])

		_, err = putConn(int64(k), int64(v), conn, nil)
		if err != nil {
			return fsbinlog.AddPadding(read), err
		}

		offset := fsbinlog.AddPadding(20)
		read += offset
		payload = payload[offset:]
	}
	return fsbinlog.AddPadding(read), nil
}

func (h *rpc_handler) get(ctx context.Context, get tlkv_engine.Get) (tlkv_engine.GetResponse, error) {
	var v int64
	err := h.engine.View(ctx, "get", func(conn sqlitev2.Conn) error {
		rows := conn.Query("select", "SELECT v FROM kv WHERE k = $key", sqlitev2.Int64("$key", get.Key))
		v = rows.ColumnInt64(0)
		return rows.Error()
	})
	return tlkv_engine.GetResponse{
		Value: v,
	}, err
}

func putConn(k, v int64, c sqlitev2.Conn, cache []byte) ([]byte, error) {
	rows := c.Query("select", "SELECT v FROM kv WHERE k = $key", sqlitev2.Int64("$key", k))
	if rows.Error() != nil {
		return cache, rows.Error()
	}
	var err error
	if !rows.Next() {
		_, err = c.Exec("insert", "INSERT INTO kv (k, v) VALUES ($key, $value)", sqlitev2.Int64("$key", k), sqlitev2.Int64("$value", v))
	} else {
		_, err = c.Exec("update", "UPDATE kv SET v = $value WHERE k = $key", sqlitev2.Int64("$key", k), sqlitev2.Int64("$value", v))
	}
	if err != nil {
		return cache, err
	}
	return genBinlogNumberEvent(k, v, cache), err
}

func (h *rpc_handler) put(ctx context.Context, put tlkv_engine.Put) (tlkv_engine.ChangeResponse, error) {
	err := h.wait(ctx)
	if err != nil {
		return tlkv_engine.ChangeResponse{}, fmt.Errorf("failed to start engine")
	}
	dbOffset, committedOffset, err := h.engine.DoWithOffset(ctx, "put", func(c sqlitev2.Conn, cache []byte) ([]byte, error) {
		return putConn(put.Key, put.Value, c, cache)
	})
	return tlkv_engine.ChangeResponse{
		Meta: tlkv_engine.MetaInfo{
			DbOffset:        dbOffset,
			CommittedOffset: committedOffset,
		},
		NewValue: put.Value,
	}, err
}

func (h *rpc_handler) incr(ctx context.Context, inc tlkv_engine.Inc) (tlkv_engine.ChangeResponse, error) {
	err := h.wait(ctx)
	if err != nil {
		return tlkv_engine.ChangeResponse{}, fmt.Errorf("failed to start engine")
	}
	var v int64
	dbOffset, committedOffset, err := h.engine.DoWithOffset(ctx, "incr", func(c sqlitev2.Conn, cache []byte) ([]byte, error) {
		rows := c.Query("select", "SELECT v FROM kv WHERE k = $key", sqlitev2.Int64("$key", inc.Key))
		if rows.Error() != nil {
			return cache, err
		}
		var err error
		if !rows.Next() {
			err = fmt.Errorf("key not found")
		} else {
			v = rows.ColumnInt64(0)
			v = v + inc.Incr
			_, err = c.Exec("update", "UPDATE kv SET v = $value WHERE k = $key", sqlitev2.Int64("$key", inc.Key), sqlitev2.Int64("$value", v))
		}
		if rows.Error() != nil {
			return cache, err
		}
		if err != nil {
			return cache, err
		}
		return genBinlogNumberEvent(inc.Key, v, cache), err
	})
	return tlkv_engine.ChangeResponse{
		Meta: tlkv_engine.MetaInfo{
			DbOffset:        dbOffset,
			CommittedOffset: committedOffset,
		},
		NewValue: v,
	}, err
}

func (h *rpc_handler) Check(ctx context.Context, args tlkv_engine.Check) (bool, error) {
	expectedMap := map[int64]int64{}
	for _, kv := range args.Kv {
		expectedMap[kv.Key] = kv.Value
	}
	actualMap := map[int64]int64{}
	err := h.engine.View(ctx, "check", func(c sqlitev2.Conn) error {
		rows := c.Query("select", "SELECT k, v FROM kv")
		for rows.Next() {
			actualMap[rows.ColumnInt64(0)] = rows.ColumnInt64(1)
		}
		return rows.Error()
	})
	if err != nil {
		return false, err
	}
	ok := reflect.DeepEqual(expectedMap, actualMap)
	if !ok {
		fmt.Println("CHECK FAILED", actualMap, expectedMap)
	}
	return ok, nil
}

func (h *rpc_handler) backup(ctx context.Context, args tlkv_engine.Backup) (tlkv_engine.BackupResponse, error) {
	fmt.Println("STARTING BACKUP")
	path, offs, err := h.engine.Backup(ctx, args.Prefix)
	return tlkv_engine.BackupResponse{
		Path:   path,
		Offset: offs,
	}, err
}

func (h *rpc_handler) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-h.readyCh:
		return err
	}
}

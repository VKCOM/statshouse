package blackbox

import (
	"context"
	"fmt"
	"os"

	"github.com/vkcom/statshouse/internal/sqlitev2"
	"pgregory.net/rand"
	"pgregory.net/rapid"
)

type logEntry struct {
	k      int64
	v      int64
	offset int64
}

type Case struct {
	tempDir          string
	kv               map[int64]int64
	client           KVEngineClient
	log              []logEntry
	LastBackupPath   string
	lastBackupOffset int64
	minValue         int64
	maxValue         int64
	commitDBOffset   int64
}

func NewCase(minValue, maxValue int64, tempDir string, client KVEngineClient) *Case {
	return &Case{
		tempDir:  tempDir,
		kv:       map[int64]int64{},
		client:   client,
		minValue: minValue,
		maxValue: maxValue,
	}
}

func (c *Case) randValue() int64 {
	return c.minValue + rand.Int63n(c.maxValue-c.minValue)
}

func (c *Case) Put() error {
	k := c.randValue()
	v := c.randValue()
	resp, err := c.client.Put(k, v)
	if err != nil {
		return err
	}
	c.kv[k] = resp.NewValue
	c.commitDBOffset = resp.Meta.CommittedOffset
	c.log = append(c.log, logEntry{
		k:      k,
		v:      resp.NewValue,
		offset: resp.Meta.DbOffset,
	})
	return nil
}

func (c *Case) Incr(r *rapid.T) {
	if len(c.kv) == 0 {
		r.SkipNow()
		return
	}
	var k int64
	for k1 := range c.kv {
		k = k1
		break
	}
	v := c.randValue()
	resp, err := c.client.Incr(k, v)
	if err != nil {
		panic(err)
	}
	c.kv[k] = resp.NewValue
	c.commitDBOffset = resp.Meta.CommittedOffset
	c.log = append(c.log, logEntry{
		k:      k,
		v:      resp.NewValue,
		offset: resp.Meta.DbOffset,
	})
}

func (c *Case) Backup(r *rapid.T) {
	if len(c.log) == 0 {
		r.SkipNow()
		return
	}
	if c.log[len(c.log)-1].offset == c.lastBackupOffset {
		return
	}
	resp, err := c.client.Backup(c.tempDir)
	if err != nil {
		panic(err)
	}
	if c.LastBackupPath != "" {
		err := os.Remove(c.LastBackupPath)
		if err != nil {
			panic(err)
		}
	}
	r.Log("Last backup path", resp.Path)
	r.Log("Last backup offset", resp.Offset)
	c.lastBackupOffset = resp.Offset
	c.LastBackupPath = resp.Path
}

func (c *Case) Revert(r *rapid.T, toOffset int64) {
	r.Log("REVERT TO", toOffset)
	if toOffset < c.commitDBOffset {
		r.Errorf("try to revert to offset: %d, committedOffset: %d", toOffset, c.commitDBOffset)
		return
	}

	kv := map[int64]int64{}
	var ix int = len(c.log)
	for i, e := range c.log {
		if e.offset > toOffset {
			ix = i
			break
		}
		kv[e.k] = e.v
	}
	c.log = c.log[:ix]
	c.kv = kv
}

func (c *Case) Check(r *rapid.T) {
	ok, err := c.client.Check(c.kv)
	if err != nil {
		panic(err)
	}
	if !ok {
		r.Errorf("CHECK FAILED client %v", c.kv)
		return
	}
	if c.LastBackupPath != "" {
		ro, err := sqlitev2.OpenEngine(sqlitev2.Options{
			Path:                         c.LastBackupPath,
			APPID:                        0,
			ReadOnly:                     true,
			NotUseWALROMMode:             true,
			MaxROConn:                    10,
			CacheApproxMaxSizePerConnect: 10,
		})
		if err != nil {
			panic(err)
		}
		defer ro.Close()
		var backupOffset int64
		err = ro.View(context.Background(), "check_binlog", func(conn sqlitev2.Conn) error {
			rows := conn.Query("__select_binlog_pos", "SELECT offset from __binlog_offset")
			if rows.Error() != nil {
				return rows.Error()
			}
			for rows.Next() {
				backupOffset = rows.ColumnInt64(0)
				return nil
			}
			return fmt.Errorf("binlog position not found")
		})

		if err != nil {
			r.Error(err.Error())
			return
		}
		if backupOffset != c.lastBackupOffset {
			r.Errorf("expect backup offset: %d, got: %d", c.lastBackupOffset, backupOffset)
			return
		}
	}
}

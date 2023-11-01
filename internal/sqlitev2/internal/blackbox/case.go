package blackbox

import (
	"context"
	"fmt"
	"os"

	"github.com/vkcom/statshouse/internal/sqlitev2"
	"pgregory.net/rand"
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
}

const maxValue = 10

func NewCase(tempDir string, client KVEngineClient) *Case {
	return &Case{
		tempDir: tempDir,
		kv:      map[int64]int64{},
		client:  client,
	}
}

func (c *Case) Put() {
	k := rand.Int63n(maxValue)
	v := rand.Int63n(maxValue)
	resp, err := c.client.Put(k, v)
	if err != nil {
		panic(err)
	}
	c.kv[k] = resp.NewValue
	c.log = append(c.log, logEntry{
		k:      k,
		v:      resp.NewValue,
		offset: resp.Meta.DbOffset,
	})
}

func (c *Case) Incr() {
	if len(c.kv) == 0 {
		return
	}
	var k int64
	for k1 := range c.kv {
		k = k1
		break
	}
	v := rand.Int63n(maxValue)
	resp, err := c.client.Incr(k, v)
	if err != nil {
		panic(err)
	}
	c.kv[k] = resp.NewValue
	c.log = append(c.log, logEntry{
		k:      k,
		v:      resp.NewValue,
		offset: resp.Meta.DbOffset,
	})
}

func (c *Case) Backup() {
	if len(c.log) == 0 {
		return
	}
	if c.log[len(c.log)-1].offset == c.lastBackupOffset {
		return
	}
	fmt.Println("backup")
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
	c.lastBackupOffset = resp.Offset
	c.LastBackupPath = resp.Path
}

func (c *Case) Revert(toOffset int64) {
	fmt.Println("REVERT TO", toOffset)
	kv := map[int64]int64{}
	var ix int
	for i, e := range c.log {
		ix = i
		if e.offset > toOffset {
			break
		}
		kv[e.k] = e.v
	}
	c.log = c.log[:ix]
	c.kv = kv
}

func (c *Case) Check() {
	ok, err := c.client.Check(c.kv)
	if err != nil {
		panic(err)
	}
	if !ok {
		fmt.Println("CHECK FAILED client", c.kv)
		panic("engine is not consistent")
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
			panic(err)
		}
		fmt.Println("CHECK BACKUP OFFSET" + fmt.Sprintf("expect backup offset: %d, got: %d", c.lastBackupOffset, backupOffset))
		if backupOffset != c.lastBackupOffset {
			panic(fmt.Sprintf("expect backup offset: %d, got: %d", c.lastBackupOffset, backupOffset))
		}
	}
}

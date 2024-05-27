package blackbox

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/vkcom/statshouse/internal/sqlitev2"
	"pgregory.net/rand"
	"pgregory.net/rapid"
)

type logEntry struct {
	k             int64
	v             int64
	offset        int64
	canBeReverted bool
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
	//fmt.Println("TRY PUT", k, v)
	resp, err := c.client.Put(k, v)
	canBeReverted := false
	if err != nil {
		if !strings.Contains(err.Error(), "connection closed after request sent") {
			fmt.Println("ERROR", err.Error())
			return err
		}
		canBeReverted = true
	}
	c.kv[k] = v
	c.commitDBOffset = resp.Meta.CommittedOffset
	c.log = append(c.log, logEntry{
		k:             k,
		v:             v,
		offset:        resp.Meta.DbOffset,
		canBeReverted: canBeReverted,
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
	if c.LastBackupPath != "" {
		err := os.Remove(c.LastBackupPath)
		if err != nil {
			panic(err)
		}
	}
	resp, err := c.client.Backup(c.tempDir)
	if err != nil {
		panic(err)
	}
	r.Log("Last backup path", resp.Path)
	r.Log("Last backup offset", resp.Offset)
	c.lastBackupOffset = resp.Offset
	c.LastBackupPath = resp.Path
}

func (c *Case) HealchCheck(r *rapid.T) {
	err := c.client.HealthCheck()
	if err != nil {
		r.Fatal(err.Error())
	}
}

func (c *Case) restoreFromLog(includeReverted bool) {
	kv := map[int64]int64{}
	newLog := []logEntry{}
	for _, entry := range c.log {
		if entry.canBeReverted && !includeReverted {
			break
		}
		kv[entry.k] = entry.v
		entry.canBeReverted = false
		newLog = append(newLog, entry)
	}
	c.kv = kv
	c.log = newLog
}

func (c *Case) Check(r *rapid.T) error {
	if len(c.log) > 0 && c.log[len(c.log)-1].canBeReverted {
		ok, err := c.client.Check(c.kv)
		if err != nil {
			panic(err)
		}
		if !ok {
			fmt.Println("try revert")
			c.restoreFromLog(false)
		} else {
			c.restoreFromLog(true)
		}
	}
	ok, err := c.client.Check(c.kv)
	if err != nil {
		panic(err)
	}
	if !ok {
		err := fmt.Errorf("CHECK FAILED client %v", c.kv)
		r.Errorf(err.Error())
		return err
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
		_, err = ro.View(context.Background(), "check_binlog", func(conn sqlitev2.Conn) error {
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
			return err
		}
		if backupOffset != c.lastBackupOffset {
			r.Errorf("expect backup offset: %d, got: %d", c.lastBackupOffset, backupOffset)
			return err
		}
	}
	return nil
}

package sqlitev2

import (
	"context"
	"fmt"
	"log"
	"time"

	restart2 "github.com/vkcom/statshouse/internal/sqlitev2/checkpoint"
)

type checkpointer struct {
	ctx                  context.Context
	cancel               func()
	ch                   chan struct{}
	rw                   *sqliteBinlogConn
	re                   *restart2.RestartFile
	binlogRun            bool
	waitCheckpointOffset int64
	waitCheckpoint       bool
	StatsOptions         StatsOptions
}

const checkInterval = time.Millisecond * 10

func newCkeckpointer(rw *sqliteBinlogConn,
	re *restart2.RestartFile,
	statsOptions StatsOptions) *checkpointer {
	ctx, cancel := context.WithCancel(context.Background())
	return &checkpointer{
		ctx:                  ctx,
		cancel:               cancel,
		ch:                   make(chan struct{}, 1),
		rw:                   rw,
		re:                   re,
		StatsOptions:         statsOptions,
		waitCheckpointOffset: 0,
		waitCheckpoint:       false,
	}
}

func (c *checkpointer) goCheckpoint() {
loop:
	for {
		select {
		case <-c.ctx.Done():
			break loop
		case <-time.After(checkInterval):
		case <-c.ch:
		}
		c.DoCheckpointIfCan()
	}
}

func (c *checkpointer) stop() {
	c.cancel()
}

func (c *checkpointer) notifyCommit(commitOffset int64, snapshotMeta []byte) {
	c.re.SetCommitInfo(commitOffset, snapshotMeta)
	select {
	case c.ch <- struct{}{}:
	default:

	}

}

func (c *checkpointer) setWaitCheckpointOffsetLocked() {
	if debugFlag {
		log.Println("Wal switch")
	}
	c.waitCheckpointOffset = c.rw.getDBOffsetLocked()
	c.waitCheckpoint = true
}

func (c *checkpointer) SetBinlogRunLocked(binlogRun bool) {
	c.binlogRun = binlogRun
}

func (c *checkpointer) DoCheckpointIfCan() error {
	c.rw.mu.Lock()
	defer c.rw.mu.Unlock()
	if c.binlogRun {
		return c.checkpointBinlogLocked()
	}
	return c.checkpointNonBinlogLocked()

}

func (c *checkpointer) checkpointNonBinlogLocked() error {
	waitCheckpoint := c.waitCheckpoint
	if !waitCheckpoint {
		return nil
	}
	start := time.Now()
	err := c.rw.conn.conn.Checkpoint()

	if err != nil {
		if debugFlag {
			log.Println(fmt.Errorf("NONBINLOG CHECKPOINT ERROR: %w", err).Error())
		}
		c.StatsOptions.walCheckpointDuration("error-nonbinlog", time.Since(start))
		return err
	}
	if debugFlag {
		log.Println("NONBINLOG CHECKPOINT OK")
	}
	c.StatsOptions.walCheckpointDuration("ok-nonbinlog", time.Since(start))
	c.waitCheckpoint = false
	return nil
}

func (c *checkpointer) checkpointBinlogLocked() error {
	waitCheckpoint := c.waitCheckpoint
	waitCheckpointOffset := c.waitCheckpointOffset
	dbOffset := c.rw.getDBOffsetLocked()
	commitOffset := c.re.GetCommitOffset()
	if waitCheckpoint && waitCheckpointOffset <= commitOffset &&
		// в новом вале должен быть хотя бы один коммит
		dbOffset > waitCheckpointOffset {
		err := c.re.SyncCommitInfo()
		if err != nil {
			_ = c.rw.setErrorLocked(err)
			return err
		}
		start := time.Now()
		err = c.rw.conn.conn.Checkpoint()

		if err != nil {
			if debugFlag {
				log.Println(fmt.Errorf("BINLOG CHECKPOINT ERROR: %w", err).Error())
			}
			c.StatsOptions.walCheckpointDuration("error-binlog", time.Since(start))
			return err
		}
		if debugFlag {
			log.Println("BINLOG CHECKPOINT OK")
		}
		c.StatsOptions.walCheckpointDuration("ok-binlog", time.Since(start))
		c.waitCheckpoint = false
	}
	return nil
}

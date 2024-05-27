package sqlitev2

import (
	"context"
	"fmt"
	"log"
	"time"
)

type checkpointer struct {
	ctx                  context.Context
	cancel               func()
	ch                   chan struct{}
	e                    *Engine
	waitCheckpointOffset int64
	waitCheckpoint       bool
}

const checkInterval = time.Millisecond * 10

func newCkeckpointer(e *Engine) *checkpointer {
	ctx, cancel := context.WithCancel(context.Background())
	return &checkpointer{
		ctx:                  ctx,
		cancel:               cancel,
		ch:                   make(chan struct{}, 1),
		e:                    e,
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
		c.doCheckpointIfCan()
	}
}

func (c *checkpointer) stop() {
	c.cancel()
}

func (c *checkpointer) notifyCommit(commitOffset int64) {
	c.e.re.SetCommitOffset(commitOffset)
	select {
	case c.ch <- struct{}{}:
	default:

	}

}

func (c *checkpointer) setWaitCheckpointOffsetLocked() {
	c.waitCheckpointOffset = c.e.rw.getDBOffsetLocked()
	c.waitCheckpoint = true
}

func (c *checkpointer) doCheckpointIfCan() {
	c.e.rw.mu.Lock()
	defer c.e.rw.mu.Unlock()
	waitCheckpoint := c.waitCheckpoint
	waitCheckpointOffset := c.waitCheckpointOffset
	dbOffset := c.e.rw.getDBOffsetLocked()
	commitOffset := c.e.re.GetCommitOffset()
	if waitCheckpoint && waitCheckpointOffset <= commitOffset &&
		// в новом вале должен быть хотя бы один коммит
		dbOffset > waitCheckpointOffset {
		err := c.e.re.SetCommitOffsetAndSync(commitOffset)
		if err != nil {
			_ = c.e.rw.setErrorLocked(err)
			return
		}
		start := time.Now()
		err = c.e.rw.conn.conn.Checkpoint()

		if err != nil {
			if debugFlag {
				log.Println(fmt.Errorf("CHECKPOINT ERROR: %w", err).Error())
			}
			c.e.opt.StatsOptions.walCheckpointDuration("error", time.Since(start))
			return
		}
		if debugFlag {
			log.Println("CHECKPOINT OK: %w")
		}
		c.e.opt.StatsOptions.walCheckpointDuration("ok", time.Since(start))
		c.waitCheckpoint = false
	}
}

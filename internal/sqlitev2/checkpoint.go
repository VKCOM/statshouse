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
	c.waitCheckpointOffset = c.e.rw.dbOffset
	c.waitCheckpoint = true
}

func (c *checkpointer) doCheckpointIfCan() {
	c.e.rw.mu.Lock()
	defer c.e.rw.mu.Unlock()
	//b.checkpointMx.Lock()
	waitCheckpoint := c.waitCheckpoint
	waitCheckpointOffset := c.waitCheckpointOffset
	commitOffset := c.e.re.GetCommitOffset()
	//b.checkpointMx.Unlock() // TODO не обязательно брать лок на все время чекпоинта, можно только на время получение коммит оффсета
	if waitCheckpoint && waitCheckpointOffset <= commitOffset {
		fmt.Println("doCheckpointIfCan")
		err := c.e.re.SetCommitOffsetAndSync(commitOffset)
		if err != nil {
			panic(err)
		}
		err = c.e.rw.conn.Checkpoint()
		// TODO чекпоинт может быть не удачен если открыты read транзакции, поэтому надо:
		// 1. Недопускать долгие рид транзакции(сделать таймаут)
		// 2. Если долго не может случиться чекпоинт куда-то алертить или падать?
		if err != nil {
			for _, i := range c.e.rw.cache.queryCache {
				if !c.e.rw.cache.h.heap[i].stmt.ResetF {
					log.Println("FOUND NON RESET", c.e.rw.cache.h.heap[i].stmt.SQLStr)
				}
			}
			fmt.Println(fmt.Errorf("CHECKPOINT ERROR: %w", err).Error())
			return
		}
		fmt.Println("CHECKPOINT OK: %w")
		// TODO если ошибка то пытается еще раз через время
		c.waitCheckpoint = false
	}
}

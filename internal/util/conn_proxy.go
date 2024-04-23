package util

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"pgregory.net/rand"
)

const (
	maxSucc            = -5
	maxFailErrAlive    = 10
	maxFailErrHalfOpen = 5
	deadInterval       = time.Second * 30

	alive    = 0
	dead     = 1
	halfOpen = 2

	halfOpenSendReq = 10 // when conn has halfOpen state send only 1/halfOpenSendReq requests
)

type severConnPool struct {
	ch *chpool.Pool

	mx         sync.Mutex
	errCount   int
	state      int
	stateStart time.Time
}

func newSeverConnPool(ch *chpool.Pool) *severConnPool {
	return &severConnPool{
		ch:         ch,
		errCount:   0,
		state:      alive,
		stateStart: time.Now(),
	}
}

func (p *severConnPool) Do(ctx context.Context, q ch.Query) (err error) {
	err = p.ch.Do(ctx, q)
	p.passResult(ctx, err)
	return err
}

func (p *severConnPool) passResult(ctx context.Context, err error) {
	p.mx.Lock()
	defer p.mx.Unlock()
	if p.state == dead {
		return
	}
	var netErr = &net.OpError{}
	if err != nil {
		if errors.As(err, &netErr) {
			p.errCount++
		} else if ctx.Err() == nil { // some query can timeout and this is ok (in this case err != nil and ctx.Err() != nil)
			p.errCount++
		}
	} else if p.errCount > maxSucc {
		p.errCount--
	}
	if p.state == alive && p.errCount >= maxFailErrAlive {
		p.errCount = 0
		p.state = halfOpen
		p.stateStart = time.Now()
	}
	if p.state == halfOpen && p.errCount >= maxFailErrHalfOpen {
		p.errCount = 0
		p.state = dead
		p.stateStart = time.Now()
	}
	if p.errCount <= maxSucc {
		p.errCount = 0
		p.state = alive
		p.stateStart = time.Now()
	}

}

func (p *severConnPool) IsReady() bool {
	p.mx.Lock()
	defer p.mx.Unlock()
	if p.state == dead && time.Since(p.stateStart) < deadInterval {
		return false
	}
	if p.state == dead {
		p.errCount = 0
		p.state = halfOpen
	}
	if p.state == halfOpen {
		return rand.Intn(halfOpenSendReq) < 1
	}
	return true
}

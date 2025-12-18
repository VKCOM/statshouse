// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlitev2

import (
	"fmt"
	"sync"

	"go.uber.org/multierr"

	"github.com/VKCOM/statshouse/internal/vkgo/vkd/logz"
)

type connPool struct {
	roMx    sync.Mutex
	roFree  []*sqliteConn
	roCond  *sync.Cond
	roCount int

	maxROConn int
	newConn   func() (*sqliteConn, error)

	log *logz.Logger
}

func newConnPool(maxROConn int, newConn func() (*sqliteConn, error), log *logz.Logger) *connPool {
	if maxROConn <= 0 {
		maxROConn = 128
	}
	p := &connPool{maxROConn: maxROConn, newConn: newConn, log: log}
	p.roCond = sync.NewCond(&p.roMx)
	return p
}

func (p *connPool) Get() (*sqliteConn, error) {
	var err error
	p.roMx.Lock()
	var conn *sqliteConn
	for len(p.roFree) == 0 && p.roCount >= p.maxROConn {
		p.roCond.Wait()
	}
	if len(p.roFree) == 0 {
		conn, err = p.newConn()
		if err != nil {
			p.roMx.Unlock()
			return nil, fmt.Errorf("failed to open RO connection: %w", err)
		}
		p.roCount++
	} else {
		n := len(p.roFree)
		conn = p.roFree[n-1]
		p.roFree = p.roFree[:n-1]
	}
	p.roMx.Unlock()
	return conn, nil
}

func (p *connPool) Put(conn *sqliteConn) {
	p.roMx.Lock()
	p.roFree = append(p.roFree, conn)
	p.roMx.Unlock()
	p.roCond.Signal()
}

func (p *connPool) Close(error *error) {
	p.roMx.Lock()
	defer p.roMx.Unlock()
	if len(p.roFree) != p.roCount {
		p.log.Warn("should finish all ViewTx queries before close")
	}
	for _, conn := range p.roFree {
		err := conn.Close()
		if err != nil {
			multierr.AppendInto(error, fmt.Errorf("failed to close RO connection: %w", err))
		}
	}
}

package sqlitev2

import (
	"fmt"
	"sync"

	"go.uber.org/multierr"
)

type connPool struct {
	roMx    sync.Mutex
	roFree  []*sqliteConn
	roCond  *sync.Cond
	roCount int

	maxROConn int
	newConn   func() (*sqliteConn, error)
}

func newConnPool(maxROConn int, newConn func() (*sqliteConn, error)) *connPool {
	if maxROConn <= 0 {
		maxROConn = 128
	}
	p := &connPool{maxROConn: maxROConn, newConn: newConn}
	p.roCond = sync.NewCond(&p.roMx)
	return p
}

func (p *connPool) get() (*sqliteConn, error) {
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

func (p *connPool) put(conn *sqliteConn) {
	p.roMx.Lock()
	p.roFree = append(p.roFree, conn)
	p.roMx.Unlock()
	p.roCond.Signal()
}

func (p *connPool) close(error *error) {
	p.roMx.Lock()
	defer p.roMx.Unlock()
	for _, conn := range p.roFree {
		err := conn.Close()
		if err != nil {
			multierr.AppendInto(error, fmt.Errorf("failed to close RO connection: %w", err))
		}
	}
}
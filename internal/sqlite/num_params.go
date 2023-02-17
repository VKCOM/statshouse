package sqlite

import (
	"strconv"
	"sync"
)

type numParams struct {
	mx     sync.RWMutex
	params []string
}

func paramName(i int) string {
	return "$int" + strconv.FormatInt(int64(i), 10)
}

func newNumParams(n int) *numParams {
	res := &numParams{}
	res.fillLocked(n)
	return res
}

func (p *numParams) fillLocked(length int) {
	for i := 0; i < length; i++ {
		l := len(p.params)
		p.params = append(p.params, paramName(l))
	}
}

func (p *numParams) get(i int) string {
	p.mx.RLock()
	if len(p.params) > i {
		res := p.params[i]
		p.mx.RUnlock()
		return res
	}
	p.mx.Lock()
	p.fillLocked(len(p.params))
	p.mx.Unlock()
	return p.get(i)
}

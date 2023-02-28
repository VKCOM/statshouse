package sqlite

import (
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"
)

type queryBuilder struct {
	query       string
	args        []Arg
	paramsIndex []indexedArg
	buffer      []byte
	nameBuffer  []byte
}

type indexedArg struct {
	a Arg
	i int
}

func (p *queryBuilder) reset(query string) {
	p.query = query
	p.args = p.args[:0]
	p.paramsIndex = p.paramsIndex[:0]
	p.buffer = p.buffer[:0]
}

func (p *queryBuilder) addSliceParam(arg Arg) {
	p.args = append(p.args, arg)
}

func (p *queryBuilder) buildQueryLocked() ([]byte, error) {
	if len(p.args) == 0 {
		p.buffer = append(p.buffer, p.query...)
		return p.buffer, nil
	}
	p.paramsIndex = p.paramsIndex[:0]
	for _, param := range p.args {
		i := strings.Index(p.query, param.name)
		j := strings.LastIndex(p.query, param.name)
		if i != j || i == -1 {
			return nil, fmt.Errorf("query doesn't contain %s arg", param.name)
		}
		p.paramsIndex = append(p.paramsIndex, indexedArg{
			a: param,
			i: i,
		})
	}
	slices.SortFunc(p.paramsIndex, func(a, b indexedArg) bool {
		return a.i < b.i
	})
	start := 0
	for i, indexed := range p.paramsIndex {
		index := indexed.i
		if i == 0 {
			p.buffer = append(p.buffer, p.query[:index]...)
		}
		p.buffer = genParams(p.buffer, start, indexed.a.length)
		from := index + len(indexed.a.name)
		to := len(p.query)
		if i != len(p.paramsIndex)-1 {
			to = p.paramsIndex[i+1].i
		}
		p.buffer = append(p.buffer, p.query[from:to]...)
		start += indexed.a.length
	}
	return p.buffer, nil
}

func (p *queryBuilder) nameLocked(n int) []byte {
	p.nameBuffer = p.nameBuffer[:0]
	p.nameBuffer = append(p.nameBuffer, "$internal"...)
	p.nameBuffer = strconv.AppendInt(p.nameBuffer, int64(n), 10)
	return p.nameBuffer
}

func genParams(buf []byte, start, n int) []byte {
	for i := 0; i < n; i++ {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, "$internal"...)
		buf = strconv.AppendInt(buf, int64(start), 10)
		start++
	}
	return buf
}

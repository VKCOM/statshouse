package sqlitev2

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"

	"go4.org/mem"
	"golang.org/x/exp/slices"
)

type queryBuilder struct {
	sliceArgs   []Arg
	paramsIndex []indexedArg
	nameBuffer  []byte
	buffer      []byte
}

type indexedArg struct {
	a Arg
	i int
}

func (p *queryBuilder) reset() {
	p.sliceArgs = p.sliceArgs[:0]
	p.paramsIndex = p.paramsIndex[:0]
	p.buffer = p.buffer[:0]
}

func (p *queryBuilder) BuildQuery(query mem.RO, args ...Arg) ([]byte, error) {
	p.reset()
	for _, arg := range args {
		if strings.HasPrefix(arg.name, "$internal") {
			return nil, fmt.Errorf("prefix $internal is reserved")
		}
		if arg.slice {
			if !checkSliceParamName(arg.name) {
				return nil, fmt.Errorf("invalid list arg name %s", arg.name)
			}
			p.sliceArgs = append(p.sliceArgs, arg)
		}
	}
	if len(p.sliceArgs) == 0 {
		p.buffer = mem.Append(p.buffer, query)
		return p.buffer, nil
	}
	p.paramsIndex = p.paramsIndex[:0]
	for _, param := range p.sliceArgs {
		i := mem.Index(query, mem.S(param.name))
		j := mem.LastIndex(query, mem.S(param.name))

		if i != j || i == -1 {
			return nil, fmt.Errorf("query doesn't contain %s arg", param.name)
		}
		p.paramsIndex = append(p.paramsIndex, indexedArg{
			a: param,
			i: i,
		})
	}
	slices.SortFunc(p.paramsIndex, func(a, b indexedArg) int {
		return cmp.Compare(a.i, b.i)
	})
	start := 0
	for i, indexed := range p.paramsIndex {
		index := indexed.i
		if i == 0 {
			p.buffer = mem.Append(p.buffer, query.SliceTo(index))
		}
		p.buffer = genParams(p.buffer, start, indexed.a.length)
		from := index + len(indexed.a.name)
		to := query.Len()
		if i != len(p.paramsIndex)-1 {
			to = p.paramsIndex[i+1].i
		}
		p.buffer = mem.Append(p.buffer, query.Slice(from, to))
		start += indexed.a.length
	}

	return p.buffer, nil
}

func (p *queryBuilder) NameBy(paramNumber int) []byte {
	p.nameBuffer = p.nameBuffer[:0]
	p.nameBuffer = append(p.nameBuffer, "$internal"...)
	p.nameBuffer = strconv.AppendInt(p.nameBuffer, int64(paramNumber), 10)
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

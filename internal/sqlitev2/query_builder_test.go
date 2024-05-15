package sqlitev2

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go4.org/mem"
	"pgregory.net/rapid"
)

func genStr(start, n int) string {
	b := strings.Builder{}
	for i := 0; i < n; i++ {
		if i != 0 {
			b.WriteString(",")
		}
		b.WriteString("$internal")
		b.WriteString(strconv.FormatInt(int64(start), 10))
		start++
	}
	return b.String()

}

func TestBuildQuery(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p := queryBuilder{}
		sql := rapid.StringMatching("[^\\$]+").Draw(t, "head")
		qExpected := sql
		n := rapid.IntRange(0, 10).Draw(t, "sliceArgs count")
		start := 0
		var args []Arg
		for i := 0; i < n; i++ {
			m := rapid.IntRange(1, 1000).Draw(t, "arg length")
			name := "$" + strconv.FormatInt(int64(i), 10) + "$"
			args = append(args, Arg{
				name:   name,
				typ:    argInt64,
				slice:  true,
				length: m,
				ns:     nil,
			})
			sql += "(" + name + ")"
			qExpected += "(" + genStr(start, m) + ")"
			between := rapid.StringMatching("[^\\$]+").Draw(t, "tail")
			sql += between
			qExpected += between
			start += m
		}
		q, err := p.BuildQuery(mem.S(sql), args...)
		require.NoError(t, err, sql, qExpected)
		require.Equal(t, qExpected, string(q))
	})
}

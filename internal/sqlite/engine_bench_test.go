package sqlite

import (
	"context"
	"fmt"
	"testing"

	binlog2 "github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"
	"pgregory.net/rand"
)

const schemeNumbers = "CREATE TABLE IF NOT EXISTS numbers (n INTEGER PRIMARY KEY);"

func initDb(b *testing.B, scheme, prefix string, dbFile string, durabilityMode DurabilityMode) (*Engine, binlog2.Binlog) {
	options := binlog2.Options{
		PrefixPath: prefix + "/test",
		Magic:      3456,
	}
	var bl binlog2.Binlog
	if durabilityMode != NoBinlog {
		_, err := fsbinlog.CreateEmptyFsBinlog(options)
		if err != nil {
			b.Fatal(err)
		}
		bl, err = fsbinlog.NewFsBinlog(&Logger{}, options)
		if err != nil {
			b.Fatal(err)
		}
	}
	engine, err := OpenEngine(Options{
		Path:           prefix + "/" + dbFile,
		APPID:          32,
		Scheme:         scheme,
		DurabilityMode: durabilityMode,
	}, bl, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	return engine, bl
}

// TODO: use stable and bigger dataset for benchmarks
func fillDB(b *testing.B, engine *Engine, table string, n, m int, gen func(i, j int) Arg) []Arg {
	b.Helper()
	res := make([]Arg, n*m)
	for i := 0; i < n; i++ {
		err := engine.Do(context.Background(), func(c Conn, cache []byte) ([]byte, error) {
			for j := 0; j < m; j++ {
				k := gen(i, j)
				q := fmt.Sprintf("INSERT INTO %s (n) VALUES ($n)", table)
				_, err := c.Exec(q, k)
				if err != nil {
					b.Fatal(err)
				}
				res = append(res, k)
			}
			return cache, nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	return res
}

func queryLoop(b *testing.B, eng *Engine, query func(c Conn, i int) Rows) {
	for i := 0; i < b.N; i++ {
		err := eng.Do(context.Background(), func(c Conn, cache []byte) ([]byte, error) {
			rows := query(c, i)
			if rows.err != nil {
				b.Fatal(rows.err)
			}
			if !rows.Next() {
				b.Fatal("no rows")
			}
			return cache, nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadNumbers(b *testing.B) {
	const m = 1000
	eng, _ := initDb(b, schemeNumbers, b.TempDir(), "test.db", NoBinlog)
	r := fillDB(b, eng, "numbers", 1000, m, func(i, j int) Arg {
		return Int64("$n", int64(i*m+j))
	})
	b.ResetTimer()
	queryLoop(b, eng, func(c Conn, i int) Rows {
		return c.Query("SELECT n FROM numbers WHERE n = $n", Int64("$n", r[rand.Int()%len(r)].n))
	})
}

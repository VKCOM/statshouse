package sqlite

import (
	"context"
	"fmt"
	"testing"

	"pgregory.net/rand"

	binlog2 "github.com/VKCOM/statshouse/internal/vkgo/binlog"
	"github.com/VKCOM/statshouse/internal/vkgo/binlog/fsbinlog"
)

const schemeNumbers = "CREATE TABLE IF NOT EXISTS numbers (n INTEGER PRIMARY KEY);"

func initDb(b *testing.B, scheme, prefix string, dbFile string, durabilityMode DurabilityMode, commitOnEachWrite, waitCommit bool) (*Engine, binlog2.Binlog) {
	options := fsbinlog.Options{
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
		Path:                   prefix + "/" + dbFile,
		APPID:                  32,
		Scheme:                 scheme,
		DurabilityMode:         durabilityMode,
		CacheMaxSizePerConnect: 1,
		CommitOnEachWrite:      commitOnEachWrite,
		WaitBinlogCommitDebug:  waitCommit,
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
		err := engine.Do(context.Background(), "test", func(c Conn, cache []byte) ([]byte, error) {
			for j := 0; j < m; j++ {
				k := gen(i, j)
				q := fmt.Sprintf("INSERT INTO %s (n) VALUES ($n)", table)
				_, err := c.Exec("insert", q, k)
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
		err := eng.View(context.Background(), "test", func(c Conn) error {
			rows := query(c, i)
			if rows.err != nil {
				b.Fatal(rows.err)
			}
			if !rows.Next() {
				b.Fatal("no rows")
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadNumbers(b *testing.B) {
	const m = 1000
	eng, _ := initDb(b, schemeNumbers, b.TempDir(), "test.db", NoBinlog, false, false)
	r := fillDB(b, eng, "numbers", 1000, m, func(i, j int) Arg {
		return Int64("$n", int64(i*m+j))
	})
	b.ResetTimer()
	list := []int64{}
	queryLoop(b, eng, func(c Conn, i int) Rows {
		list := list[:0]
		list = append(list, r[rand.Int()%len(r)].n)
		return c.Query("select", "SELECT n FROM numbers WHERE n in ($n$)", Int64Slice("$n$", list))
	})
}

func BenchmarkWriteNumbers(b *testing.B) {
	f := func(b *testing.B, waitCommit bool) {
		engine, _ := initDb(b, schemeNumbers, b.TempDir(), "test.db", NoWaitCommit, true, waitCommit)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := engine.Do(context.Background(), "test", func(c Conn, cache []byte) ([]byte, error) {
				_, err := c.Exec("insert", "INSERT INTO numbers (n) VALUES ($n)", Int64("$n", int64(i)))
				if err != nil {
					b.Fatal(err)
				}
				return append(cache, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.Run("wait binlog commit", func(b *testing.B) {
		f(b, true)
	})
	b.Run("not wait binlog commit", func(b *testing.B) {
		f(b, false)
	})

}

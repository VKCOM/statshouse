package sqlitev2

import (
	"context"
	"fmt"
	"testing"

	binlog2 "github.com/VKCOM/statshouse/internal/vkgo/binlog"
	"github.com/VKCOM/statshouse/internal/vkgo/binlog/fsbinlog"
)

const schemeNumbers = "CREATE TABLE IF NOT EXISTS numbers (n INTEGER PRIMARY KEY);"

func initDb(b *testing.B, scheme, prefix string, dbFile string, useBinlog bool) (*Engine, binlog2.Binlog) {
	options := fsbinlog.Options{
		PrefixPath: prefix + "/test",
		Magic:      3456,
	}
	var bl binlog2.Binlog
	if useBinlog {
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
		Path:                         prefix + "/" + dbFile,
		APPID:                        32,
		Scheme:                       scheme,
		CacheApproxMaxSizePerConnect: 100,
	})
	if err != nil {
		b.Fatal(err)
	}
	if useBinlog {
		fmt.Println("Start run")
		go func() {
			err = engine.Run(bl, userEngine{}, nil)
			if err != nil {
				panic(err)
			}
		}()

		fmt.Println("START WaitReady")
		err = <-engine.ReadyCh()
		if err != nil {
			panic(err)
		}
		fmt.Println("FINISH WaitReady")
	}
	return engine, bl
}

// TODO: use stable and bigger dataset for benchmarks
func fillDB(b *testing.B, engine *Engine, table string, n, m int, gen func(i, j int) Arg) []Arg {
	res := make([]Arg, 0, n*m)
	for i := 0; i < n; i++ {
		_, err := engine.DoTx(context.Background(), "test", func(c Conn, cache []byte) ([]byte, error) {
			for j := 0; j < m; j++ {
				k := gen(i, j)
				q := fmt.Sprintf("INSERT INTO %s (n) VALUES ($n)", table)
				err := c.Exec("insert", q, k)
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
		_, err := eng.ViewTx(context.Background(), "test", func(c Conn) error {
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
	eng, _ := initDb(b, schemeNumbers, b.TempDir(), "test.db", false)
	r := fillDB(b, eng, "numbers", 1000, m, func(i, j int) Arg {
		return Integer("$n", int64(i*m+j))
	})
	b.ResetTimer()
	queryLoop(b, eng, func(c Conn, i int) Rows {
		return c.Query("select", "SELECT n FROM numbers WHERE n = $n", r[i%len(r)])
	})
}

func BenchmarkWrite(b *testing.B) {
	const schemeNumbers = "CREATE TABLE IF NOT EXISTS numbers (n INTEGER PRIMARY KEY);"
	eng, _ := initDb(b, schemeNumbers, b.TempDir(), "test.db", true)

	b.ResetTimer()
	b.ReportAllocs()

	var bytes [32]byte
	for i := 0; i < b.N; i++ {
		_, err := eng.DoTx(context.Background(), "dododo", func(c Conn, cache []byte) ([]byte, error) {
			err := c.Exec("dodod", "INSERT OR REPLACE INTO numbers(n) VALUES($i)", Integer("$i", int64(i)))
			return append(cache, bytes[:]...), err
		})
		if err != nil {
			panic(err)
		}
	}

	err := eng.Close()
	if err != nil {
		panic(err)
	}
}

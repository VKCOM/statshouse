
# Example
```go 
        const scheme = "CREATE TABLE IF NOT EXISTS example (c1 INTEGER, c2 BLOB);"
	eng, _ := sqlitev2.OpenEngine(sqlitev2.Options{
		APPID:  0xFF,
		Scheme: scheme,
	})
	go func() {
		log.Fatal(eng.Run(binlog, func(conn sqlitev2.Conn, payload []byte) (int, error) {
			// binlog apply function
		}))
	}()
	_ = eng.WaitReady()
	_ = eng.Do(context.TODO(), "insert_example_tx", func(c sqlitev2.Conn, cache []byte) ([]byte, error) {
		_, _ = c.Exec("insert_example_exec", "INSERT INTO example(c1, c2) VALUES ($c1, $c2)",
			sqlitev2.Int64("$1", 0),
			sqlitev2.BlobString("$2", "text"))
		return append(cache, "binlog bytes"...), nil
	})

	_ = eng.View(context.TODO(), "read_example_tx", func(conn sqlitev2.Conn) error {
		rows := conn.Query("read_example_query", "SELECT c1, c2 FROM example")
		for rows.Next() {
			_ = rows.ColumnInt64(0)
			_, _ = rows.ColumnBlobString(1)
		}
		return rows.Error()
	})

	_ = eng.ViewOpts(context.TODO(), sqlitev2.ViewTxOptions{
		QueryName:  "read_example_tx_2",
		WaitOffset: 1024,
	}, func(conn sqlitev2.Conn) error {
		rows := conn.Query("read_example_query", "SELECT c1, c2 FROM example")
		for rows.Next() {
		}
		return rows.Error()
	})


```

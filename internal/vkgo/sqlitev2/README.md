# Обертка sqlite V2

## Сборка sqlite клиента для работы с базой

Клиент собирается из ветки, которая реализует механику двух wal

```bash
    git clone https://github.com/sqlite/sqlite.git && cd sqlite && git checkout wal2
    ./configure && make
    ./sqlite3 --version
```

## Отличия от sqlite V1:

1. Транзакции могут терять данные
2. Все write операции не требующие бинлога нужно делать до вызова Run

## Особенности реализации:

### Термины

- `wal` - файл со страницами базы, каждый раз когда транзакция хочет поменять страницы базы она пишет ее в этот файл
- `checkpoint` - операция переноса страниц wal'а в файл базы
- `wal2 режим` - режим работы sqlite в котором используются 2 wal файла. Это позволяет избежать бесконечного роста wal файла ([подробнее](https://www.sqlite.org/cgi/src/doc/wal2/doc/wal2.md))
- `wal switch` - операция переключения текущего wal'a. После ее выполнения все послеющии страницы будут записываться в новый wal файл
- `бинлог`/`барсик`/`коммит`/`реверт`/... - ?

### -

- Коммит в sqlite происходит сразу после применения операций пользователя. Это требуется чтобы View операции могли видеть изменения
- `DoTx` завершается сразу после коммита в sqlite. Ожидание коммита барсика не происходит (будет добавлена такая возможность позже)
- В связи с пунктом `1` требуется уметь откатывать транзакции sqlite, так как барсик может запросить revert.
  
Для выполнения операции revert используется следующая схема

- Создается отдельный файл в котором хранится позиция последнего барсик коммита
- Во время wal switch, в памяти запоминается позиция бинлог оффсета которому соответствует старый wal
- Checkpoint происходит мануально только после того как коммит позиция барсика будет больше или равна позиции старого wal'а И в новом wal есть хотя бы один коммит

Рестарт:

- Если у базы нет wal'ов просто стартуем
- Если есть один wal, то его можно удалить, так как checkpoint не мог быть начат
- Если есть два wal'а, то новый удаляем, а для старого проверяем меньше или равен его оффсет комита барсика. Если да, то делаем checkpoint, если нет,
то также удаляем его

## Example
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

# Logger

## Настройка логирования в ClickHouse (КХ)

> Инструкция больше рекомендательная и описывает базовый алгоритм. Дальше будет видно, что схему таблицы можно менять по своему усмотрению

### Пререквезит

- Определиться с цифрами
  - Сколько сообщение в день будет писать твоё приложение
    - Пример: Ожидаем 10к сообщений в день
  - Какой TTL (время жизни) будет у этих сообщений
    - Пример: TTL записей 2 недели
- Определиться в какой кластер будет идти запись. Уже знаешь и есть договорённость с владельцем кластера? Отлично!
  - Если договорённости нет, то надо общаться с коллегами в чате MNT
  - Они у тебя попросят информацию из первых пунктов

Так же коллеги спросят у тебя какую табличку надо сделать. Для этого можешь воспользоваться шпаргалкой
```clickhouse
CREATE TABLE <your_service_name>_logs ON CLUSTER default_g2 (
    date                 Date DEFAULT toDate(time),
    time                 DateTime,
    server               String,
    level                String,
    caller               String,
    message              String,
    fields               String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/<your_service_name>_logs', '{replica}')
PARTITION BY toYYYYMM(date)
ORDER BY (date, time)
SETTINGS index_granularity = 8192;

CREATE TABLE <your_service_name>_logs_buffer ON CLUSTER default_g2 AS <your_service_name>_logs
ENGINE = Buffer(default, <your_service_name>_logs, 2, 60, 60, 10000000, 10000000, 100000000, 100000000)
```

В скрипте заменить `<your_service_name>` на имя твоего сервиса. По-идее таблицу можно звать как угодно, но общее направление лучше сохранить
Остальные параметры как `{shard}`, `{replica}`, `default_g2` и цифры доверь править твоим коллегам из MNT, они точно знают лучше что туда вставлять, по твоим требованиям ;)

### Как подключить это в коде

По пути /projects/<project-name>/internal/app/logger кладёшь файл

> Файл можно класть в другое место, но для удобства package лучше назвать logger

```go
package logger
// logger.go

import (
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap/zapcore"

	"gitlab.mvk.com/go/vkgo/pkg/kittenhouseclient"
	"gitlab.mvk.com/go/vkgo/pkg/vkd/logz"
)

const (
	DefaultName = "adm512" // любое удобное имя
)

var (
	defaultLogger       *logz.Logger // этот логер будешь использовать дальше везде в коде
	clickHouseLogsTable = "<your_service_name>_logs_buffer" // пишем всегда в буфферную таблицу
)

// clickhouseRow описывает формат строки, которая будет передана в КХ
// поля должны совпадать со схемой таблицы. В целом схема таблицы может быть любой, главное записывать правильные поля
type clickhouseRow struct {
	Time    time.Time `clickhouse:"time"`
	// Имя нужно использовать для идентификации приложения. stg, prod. Или ваш username, если отлаживаетесь на adm512
	// По нему потом можно будет фильтровать данные в КХ
	Server  string    `clickhouse:"server"`
	Level   string    `clickhouse:"level"`
	// Caller будет содержать файл и строку из которой произведена запись
	Caller  string    `clickhouse:"caller"`
	Message string    `clickhouse:"message"`
	Fields  string    `clickhouse:"fields"`
}

// mapZapEntry выполняет конвертацию строки лога в запись для КХ
func mapZapEntry(server string) logz.EntryToRowMapper {
	return func(entry zapcore.Entry, fields map[string]interface{}) interface{} {
		var sb strings.Builder
		for k, v := range fields {
			sb.WriteString(fmt.Sprintf("%s: %v\n", k, v))
		}
        
		return &clickhouseRow{
			Time:    time.Now(),
			Server:  server,
			Level:   entry.Level.String(),
			Caller:  entry.Caller.String(),
			Message: entry.Message,
			Fields:  sb.String(),
        }
	}
}

// SetDefaultLogger поможет установить уже настроенный логер для проекта
func SetDefaultLogger(l *logz.Logger) {
	defaultLogger = l
}

// InitClickHouseLogger занимается инициализацией логера который пишет в КХ
func InitClickHouseLogger(kh *kittenhouseclient.Client, serverName string, lvl logz.Level, origLogger *logz.Logger) *logz.Logger {
	khOption := logz.WithKittenHouse(
		kh,
		clickHouseLogsTable,
		false,
		mapZapEntry(serverName),
		lvl,
	)

	logger := origLogger.WithOptions(khOption)
	defaultLogger = logger
    
	return logger
}

func Trace(message string, args ...logz.Field) {
  defaultLogger.Trace(message, args...)
}

func Debug(message string, args ...logz.Field) {
  defaultLogger.Debug(message, args...)
}

func Info(message string, args ...logz.Field) {
  defaultLogger.Info(message, args...)
}

func Warn(message string, args ...logz.Field) {
  defaultLogger.Warn(message, args...)
}

func Error(message string, args ...logz.Field) {
  defaultLogger.Error(message, args...)
}

func Panic(message string, args ...logz.Field) {
  defaultLogger.Panic(message, args...)
}

func Fatal(message string, args ...logz.Field) {
  defaultLogger.Fatal(message, args...)
}

func Tracef(message string, args ...interface{}) {
  defaultLogger.Tracef(message, args...)
}

func Debugf(message string, args ...interface{}) {
  defaultLogger.Debugf(message, args...)
}

func Infof(message string, args ...interface{}) {
  defaultLogger.Infof(message, args...)
}

func Warnf(message string, args ...interface{}) {
  defaultLogger.Warnf(message, args...)
}

func Errorf(message string, args ...interface{}) {
  defaultLogger.Errorf(message, args...)
}

func Panicf(message string, args ...interface{}) {
  defaultLogger.Panicf(message, args...)
}

func Fatalf(message string, args ...interface{}) {
  defaultLogger.Fatalf(message, args...)
}
```

Подключение логера в проект

> Не забываем, что для корректной работы требуется настройка KittenHouse через параметры запуска vkd `--kittenhouse-addr`

```go
package main
// main.go

import (
	"os"

	"gitlab.mvk.com/go/vkgo/pkg/vkd"
	"gitlab.mvk.com/go/vkgo/projects/<your_service_name>/internal/app/logger"
)

func main() {
	daemon := vkd.NewDaemonWithOptions(...)

	daemon.Log = logger.InitClickHouseLogger(
		daemon.GetKittenHouseClient(),
        // Как вы задаёте имя сервера - не важно. Но тут может пригодиться logger.DefaultName
		// если "снаружи" в приложение никак не передали имя сервера
        os.Getenv("LOGGER_SERVER"), 
		daemon.Log.Level,
		daemon.Log,
	)

	// ... дальше какой-то код
	// daemon.Run() и тд
}
```

Использование логера в проекте

> В результате работы лог будет записан в КХ и консоль приложения

```go

import (
    "gitlab.mvk.com/go/vkgo/projects/<your_service_name>/internal/app/logger"
)

func noMain() {
    logger.Errorf("Some%sMessage", "Cool")
}

```

### Как читать логи из КХ

Есть статейка в confluense. От неё можно отталкиваться
- https://confluence.mvk.com/pages/viewpage.action?pageId=20054673

Как серебряная пуля - консольный клиент на твоём adm512

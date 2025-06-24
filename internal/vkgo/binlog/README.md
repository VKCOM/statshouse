# Набор интерфейсов для работы с бинлогами

## Чеклист для перевода движка на барсик

- движок использует интерфейс binlog.Binlog и реализует интерфейс binlog.Engine из этого пакета
- движок стартует rpc.Server только после прихода ChangeRole с ready == true
- движок умеет менять статус мастерства при приходе ChangeRole (в том числе должен уметь отвечать BarsicNotAMasterRPCError на write запросы, если он стал репликой)
- в rpc.Server добавлен хендлер для engine.Stats 
```go
binlogHandle, migrateFunc, err := barsiclib.NewBinlog(daemon.Log, options)
...
daemon.InitRPCServer(
    rpc.ServerWithHandler(handle),
    rpc.ServerWithStatsHandler(binlogHandle.AddStats), // <-----
);
```
- (для движков, которые мигрируют с старого бинлога) уметь принимать rpc вызов engine.switchToBarsic и вызывать в нем коллбек migrateFunc (из примера выше)

// TODO: часть этих пунктов по-хорошему надо внести в vkd, чтобы клиенты не парились

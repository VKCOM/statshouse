package sqlitev2

import "github.com/VKCOM/statshouse/internal/vkgo/binlog"

// Все колбэки вызываются в барсик горутине, не стоит в них выполнять долгие операции
type UserEngine interface {
	// Движок должен инициировать завершение работы движка(отправить сигнал на закрытие rpc, engine.close и тд).
	// Само завершение в этом колбэке выполнять не следует
	Shutdown()
	Revert(toOffset int64) // в текущей реализации, требуется действовать аналогично Shutdown
	ChangeRole(info binlog.ChangeRoleInfo)
}

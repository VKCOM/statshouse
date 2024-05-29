package sqlitev2

import "github.com/vkcom/statshouse/internal/vkgo/binlog"

type UserEngine interface {
	// Движок должен завершить все зпросы и выполнить Close на Engine
	Shutdown()
	ChangeRole(info binlog.ChangeRoleInfo)
}

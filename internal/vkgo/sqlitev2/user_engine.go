// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlitev2

import "github.com/VKCOM/statshouse/internal/vkgo/binlog"

// Все колбэки вызываются в барсик горутине, не стоит в них выполнять долгие операции
type UserEngine interface {
	// Движок должен инициировать завершение работы движка(отправить сигнал на закрытие rpc, engine.close и тд).
	// Само завершение в этом колбэке выполнять не следует
	Shutdown()
	Revert(toOffset int64) // в текущей реализации, требуется действовать аналогично Shutdown
	ChangeRole(info binlog.ChangeRoleInfo)
	Reindex(operator binlog.ReindexOperator)
}

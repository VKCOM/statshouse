// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package binlog

import (
	"errors"
	"fmt"

	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
	"github.com/VKCOM/statshouse/internal/vkgo/tlbarsic"
)

type StartCmd = tlbarsic.Start

const (
	BarsicNotAMasterRPCError = -2011 // TODO - move to better place

	// names of parameters in engine.Stat
	StatClusterName     = "barsic-cluster"
	StatShardName       = "barsic-shard"
	StatSwitchStatus    = "barsic-status"        // TODO: remove this stats after full switch to barsic
	StatUseCommonHeader = "barsic-common-header" // TODO: remove this stats after full switch to barsic
	StatRunUnderBarsic  = "barsic-exec"
)

var (
	// ErrorUnknownMagic mean engine encounter unknown magic number.
	// This is make sense only for old binlog implementation
	ErrorUnknownMagic = errors.New("unknown magic")

	// ErrorNotEnoughData mean that Binlog do not provide enough data to Apply.
	// This is make sense only for old binlog implementation
	ErrorNotEnoughData = errors.New("not enough data")
)

type ChangeRoleInfo struct {
	IsMaster    bool
	IsReady     bool
	ViewNumber  int64
	EpochNumber int64
}

func CompareEpochNumberViewNumber(e1, v1, e2, v2 int64) bool {
	return e1 < e2 ||
		e1 == e2 && v1 < v2
}

func (c ChangeRoleInfo) IsReadyMaster() bool { return c.IsMaster && c.IsReady }

type EngineStatus struct { // copy of tlbarsic.EngineStatus
	Version                  string
	LoadedSnapshot           string
	LoadedSnapshotOffset     int64
	LoadedSnapshotProgress   int64
	LoadedSnapshotSize       int64
	PreparedSnapshot         string
	PreparedSnapshotOffset   int64
	PreparedSnapshotProgress int64
	PreparedSnapshotSize     int64
}

type ReindexOperator interface {
	// if snapshotCreated == false, barsic will not try to find new snapshot on fs
	FinishedOk(snapshotCreated bool)
	FinishedError()
}

// Engine represent all operation that engine need to implement
//
// All callbacks called from single event reactor, so they won't be called concurrently.
type Engine interface {
	// Apply получает payload с событиями, которые должен обработать движок. После обработки движок должен вернуть
	// свою позицию бинлога. Семантика newOffset - все до этой позиции движок успешно обработал
	// (позиция начинается с нуля, при обработке первых 20 байт, возвращайте 20).
	//
	//  Содержание payload немного отличается в зависимости от имплементации:
	//
	// * для старых бинлогов (fsBinlog) payload может содержать сервисные события (crc, rotateTo и т.п.).
	//   Движок должен реагировать на них ошибкой ErrorUnknownMagic. Также в payload может прийти неполные события,
	//   на них движок должен реагировать ошибкой ErrorNotEnoughData, io.EOF или io.ErrUnexpectedEOF.
	//   ВАЖНОЕ ЗАМЕЧАНИЕ. Все события в бинлоге выровнены по 4 байта, при записи пользовательских событий может быть
	//   добавлен паддинг, поэтому при чтении может потребоваться его пропускать. Можно использовать хелпер fsbinlog.AddPadding.
	//   Если пользовательские события сериализуются при помощи TL, делать этого не нужно (выравнивание там включено)
	//
	// * для Barsic payload всегда содержит только полные пользовательские события (возможно несколько). Движок обязан
	//   обработать все данные из payload.
	//   содержимое payload перетирается после завершения обработчика, не сохраняйте этот слайс или его подслайс.
	Apply(payload []byte) (newOffset int64, err error)

	// Skip говорит движку, что нужно пропустить skipLen байт бинлога. Это может быть вызвано тем, что внутри
	// обработались служебные события (в случае старых бинлогов) или тем, что в настройках явно указанно пропустить
	// эти байты (они вызывают проблемы в работе движка). Движок должен вернуть новую позицию.
	Skip(skipLen int64) (newOffset int64, err error)

	// Commit говорит движку offset событий, которые гарантированно записаны в систему и не будут инвалидированны (Revert).
	// В offset записана позиция первого байта, который ещё не закоммичен. snapshotMeta - это непрозрачный набор байтов,
	// которые нужно записать в следующий снепшот и вернуть при старте движка в Binlog.Start
	// safeSnapshotOffset это минимум от закоммиченной и локально fsync-нутой позиции, можно начинать реальную запись сделанного
	// снапшота только когда эта позиция станет >= безопасной позиции снапшота
	// содержимое snapshotMeta перетирается после завершения обработчика, не сохраняйте этот слайс или его подслайс.
	Commit(toOffset int64, snapshotMeta []byte, safeSnapshotOffset int64) (err error)

	// Revert говорит движку, что все события начиная с позиции toOffset невалидны и движок должен откатить их из своего состояния.
	// Если движок не умеет этого делать, он должен вернуть false. Дальше Barsic будет решать что с ним делать (скорее всего перезапустит)
	Revert(toOffset int64) (bool, error)

	// ChangeRole сообщает движку об изменении его роли
	// Когда info.IsReadyMaster(), движок уже стал пишущим мастером, и имеет право ещё до выхода из этой функции
	//     начать вызывать Append для новых команд
	// Когда !info.IsReadyMaster(), движок обязан гарантировать, что ещё до выхода из этой функции сложит полномочия
	//     пишущего мастера, то есть больше не вызовет ни один Append
	// Под капотом, библиотека Barsic записывает в первом случае эхо команды до вызова ChangeRole, а во втором случае - сразу после.
	//
	// Отдельную семантику имеет первый вызов ChangeRole с флагом IsReady, это означает, что мы прочитали текущий бинлог
	// примерно до конца, и некоторым движкам удобно в этот момент начать отвечать на запросы
	// это поведение экспериментальное,
	// TODO: подробно описать сценарий
	ChangeRole(info ChangeRoleInfo) error

	// StartReindex signal that engine need to reindex it's state. After reindex engine must call one of
	// Finished* function from ReindexOperator.
	//
	// Be aware that this function called in common event reactor, so it should not block
	StartReindex(operator ReindexOperator)

	// Split command engine to forget state not belonging to new shard and remember new shardID.
	// engine should learn about which state to forget by parsing toShardID
	// also engine must reset snapshotMeta and controlMeta to empty state
	// before split, engine will always receive commit for split offset
	// engine must return true if it implements this method, otherwise it will be killed
	Split(offset int64, toShardID string) bool

	// Shutdown tells engine that it will be shutdown soon.
	// On receive, engine must stop answering engine.pid so "пинговалка" mark us as dead and we stop receiving requests.
	// After some time, barsic will stop us for good
	//
	// Note: this is (hopefully) temporary solution for "no error" graceful restart.
	Shutdown()
}

type Binlog interface {
	// Run binlog event reactor. Blocking for whole duration of Engine lifetime.
	//
	// Engine start getting event only during Run. If Run is exit, engine should close rpc server and exit.
	// Arguments should be restored from snapshot (see Engine.Commit for details)
	// Slices are cloned, safe for reuse
	Run(offset int64, snapshotMeta []byte, controlMeta []byte, engine Engine) error

	// Append асинхронно добавляет событие в бинлог. Это не дает гарантий, что событие останется в бинлоге,
	// для этого см. функцию Engine.Commit. Возвращает позицию в которую будет записанно следующее событие.
	// Может блокироваться только если в имплементации реализованн back pressure механизм.
	//
	// Функция не thread safe, движок должен сам следить за линеаризуемостью событий. Append не поглощает
	// payload, этот слайс можно сразу переиспользовать
	Append(onOffset int64, payload []byte) (nextLevOffset int64, err error)

	// AppendASAP тоже что и Append, только просит имплементацию закоммитить события как только сможет
	AppendASAP(onOffset int64, payload []byte) (nextLevOffset int64, err error)

	// EngineStatus отправляет статус Барсику для показа в консоли. Можно отправлять как угодно часто, но лучше, когда меняется.
	// Первый статус с версией и именем снапшота лучше отправить сразу же, как тольео выбрали, какой снапшот грузить.
	EngineStatus(status EngineStatus)

	// Return Start command sent by Barsic on handshake. If we not running under barsic, false is returned
	GetStartCmd() (StartCmd, bool)

	// RequestShutdown send barsic request for exit.
	//
	// Common shutdown states is (some steps may alter by barsic config)
	// - if engine is leader, it demotes to follower
	// - wait until quorum + 1 of engines in shard is working
	// - send Engine.Shutdown
	// - unblock Binlog.Run
	// - wait until engine process exit
	RequestShutdown()

	// RequestReindex from barsic.
	//
	// Usually reindex happen on some schedule, but engine might decide it need reindex now.
	// After this call barsic will put engine in indexing queue and call StartReindex in some time.
	RequestReindex(diff bool, fast bool)

	// AddStats записывает статистику в stats. Формат соответствует стандартным 7enginestat
	AddStats(stats map[string]string)
}

// Experimental, TODO - move to better place
func (c ChangeRoleInfo) ValidateWriteRequest(hctx *rpc.HandlerContext) error {
	c.ValidateReadRequest(hctx)
	if !c.IsReadyMaster() {
		return &rpc.Error{
			Code:        BarsicNotAMasterRPCError,
			Description: fmt.Sprintf("%d:%d not master", c.EpochNumber, c.ViewNumber),
		}
	}
	return nil
}

// Experimental, TODO - move to better place
func (c ChangeRoleInfo) ValidateReadRequest(hctx *rpc.HandlerContext) {
	if hctx != nil {
		hctx.ResponseExtra.SetViewNumber(c.ViewNumber)
		hctx.ResponseExtra.EpochNumber = c.EpochNumber
	}
}

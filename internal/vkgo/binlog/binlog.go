// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package binlog

import (
	"errors"
	"fmt"

	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

const BarsicNotAMasterRPCError = -2011 // TODO - move to better place

var (
	// ErrorUnknownMagic mean engine encounter unknown magic number.
	// This is make sense only for old binlog implementation
	ErrorUnknownMagic = errors.New("unknown magic")

	// ErrorNotEnoughData mean that Binlog do not provide enough data to Apply.
	// This is make sense only for old binlog implementation
	ErrorNotEnoughData = errors.New("not enough data")
)

type ChangeRoleInfo struct {
	IsMaster   bool
	IsReady    bool
	ViewNumber int64
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

type Engine interface {
	// Apply получает payload с событиями, которые должен обработать движок. После обработки движок должен вернуть
	// свою позицию бинлога. Семантика newOffset - все до этой позиции движок успешно обработал.
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
	//     начать вызывать Apply для новых команд
	// Когда !info.IsReadyMaster(), движок обязан гарантировать, что ещё до выхода из этой функции сложит полномочия
	//     пишущего мастера, то есть больше не вызовет ни один Apply
	// Под капотом, библиотека Barsic записывает в первом случае эхо команды до вызова ChangeRole, а во втором случае - сразу после.
	//
	// Отдельную семантику имеет первый вызов ChangeRole с флагом IsReady, это означает, что мы прочитали текущий бинлог
	// примерно до конца, и некоторым движкам удобно в этот момент начать отвечать на запросы
	// это поведение экспериментальное,
	// TODO: подробно описать сценарий
	ChangeRole(info ChangeRoleInfo) error
}

type Binlog interface {
	// Run запускает работу бинлогов. Если движок уже прочитал часть состояния из снепшота, он должен передать
	// offset и snapshotMeta (берутся из Engine.Commit).
	// Блокируется на все время работы бинлогов
	// содержимое snapshotMeta копируется, слайс или подслайс не сохраняется
	Run(offset int64, snapshotMeta []byte, engine Engine) error

	// Restart просит Барсик перезапустить движок. Может быть вызван в любой момент, даже до Start, вызвать Restart
	// 1 или более раз. Если движок был мастером, Барсик запустит процесс безоткатного снятия роли, затем
	// когда роль станет безопасна для рестарта, закроет сокет, дождётся выхода и перезапустит движок.
	// также учитывается состояние кластера, если другие движки перезапускаются, перезапуск этого может быть отложен.
	Restart()

	// Append асинхронно добавляет событие в бинлог. Это не дает гарантий, что событие останется в бинлоге,
	// для этого см. функцию Engine.Commit. Возвращает позицию в которую будет записанно следующее событие.
	// Может блокироваться только если в имплементации реализованн back pressure механизм.
	//
	// Функция не thread safe, движок должен сам следить за линеаризуемостью событий. Append не поглощает
	// payload, этот слайс можно сразу переиспользовать
	Append(onOffset int64, payload []byte) (nextLevOffset int64, err error)

	// AppendASAP тоже что и Append, только просит имплементацию закоммитить события как только сможет
	AppendASAP(onOffset int64, payload []byte) (nextLevOffset int64, err error)

	// AddStats записывает статистику в stats. Формат соответствует стандартным 7enginestat
	AddStats(stats map[string]string)

	// Shutdown дает сигнал на завершение работы бинлога. Неблокирующий.
	Shutdown() error

	// EngineStatus отправляет статус Барсику для показа в консоли. Можно отправлять как угодно часто, но лучше, когда меняется.
	// Первый статус с версией и именем снапшота лучше отправить сразу же, как тольео выбрали, какой снапшот грузить.
	EngineStatus(status EngineStatus)
}

type Options struct {
	// Настройки для fsBinlog имплементации, барсик настраиваится в конфиге барсика
	PrefixPath        string // путь до бинлога, включающий префикс для файлов финлога (например `some/path/gopusher_binlog`)
	Magic             uint32 // Ожидаемый magic (scheme) движка (0 - если можно открывать любой тип)
	ReplicaMode       bool   // Бинлог открывается в режиме читающей реплики (постоянно читаем файл, если EOF, ждем новой записи)
	ReadAndExit       bool   // Может быть полезен для инструментария, не имеет смысла вместе с ReplicaMode
	MaxChunkSize      uint32 // Примерный порог (в байтах) для срабатывания ротации бинлога
	HardMemLimit      int    // Лимит буффера, при котором включается механизм back pressure: Append начнет блокироваться, пока буффер не передастся на запись
	EngineIDInCluster uint   // Номер данного шарда в кластере, для статы
	ClusterSize       uint   // Размер кластера для данного шарда, для статы
	DoNotFSyncOnRead  bool   // Не делать fsync при коммите в режиме реплики. При отключении теряется гарантия того, что Commit() реально записал на диск. Для тестов перфа
}

// Experimental, TODO - move to better place
func (c ChangeRoleInfo) ValidateWriteRequest(hctx *rpc.HandlerContext) error {
	c.ValidateReadRequest(hctx)
	if !c.IsReadyMaster() {
		return rpc.Error{
			Code:        BarsicNotAMasterRPCError,
			Description: fmt.Sprintf("%d not master", c.ViewNumber),
		}
	}
	return nil
}

// Experimental, TODO - move to better place
func (c ChangeRoleInfo) ValidateReadRequest(hctx *rpc.HandlerContext) {
	if hctx != nil {
		hctx.ResponseExtra.SetViewNumber(c.ViewNumber)
	}
}

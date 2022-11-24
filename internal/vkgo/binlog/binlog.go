// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package binlog

import (
	"errors"
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
	IsMaster   bool
	Ready      bool
	ViewNumber int64
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
	// * для GMS payload всегда содержит только полные пользовательские события (возможно несколько). Движок обязан
	//   обработать все данные из payload.
	Apply(payload []byte) (newOffset int64, err error)

	// Skip говорит движку, что нужно пропустить skipLen байт бинлога. Это может быть вызвано тем, что внутри
	// обработались служебные события (в случае старых бинлогов) или тем, что в настройках явно указанно пропустить
	// эти байты (они вызывают проблемы в работе движка). Движок должен вернуть новую позицию.
	Skip(skipLen int64) (newOffset int64)

	// Commit говорит движку offset событий, которые гарантированно записаны в систему и не будут инвалидированны (Revert).
	// В offset записана позиция первого байта, который ещё не закоммичен. snapshotMeta - это непрозрачный набор байтов,
	// которые нужно записать в снепшот и вернуть при старте движка в Binlog.Start
	Commit(toOffset int64, snapshotMeta []byte)

	// Revert говорит движку, что все события начиная с позиции toOffset невалидны и движок должен откатить их из своего состояния.
	// Если движок не умеет этого делать, он должен вернуть false. Дальше GMS будет решать что с ним делать (скорее всего перезапустит)
	Revert(toOffset int64) bool

	// ChangeRole сообщает движку об изменении состояния его роли. Движок обязан заблокироваться в этом вызове до тех пор,
	// пока не сможет выполнять новую роль. Например если роль поменялась с мастера на реплику, движок должен обработать
	// все текущие write запросы и потом выйти из этой функции.
	//
	// Отдельную семантику имеет первый вызов ChangeRole с флагом Ready, это означает, что мы прочитали текущий бинлог
	// до конца и движок может начинать работу.
	// TODO: подробно описать сценарий
	ChangeRole(info ChangeRoleInfo)
}

type Binlog interface {
	// Start запускает работу бинлогов. Если движок уже прочитал часть состояния из снепшота, он должен передать
	// offset и snapshotMeta (берутся из Engine.Commit).
	// Блокируется на все время работы бинлогов
	Start(offset int64, snapshotMeta []byte, engine Engine) error

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
}

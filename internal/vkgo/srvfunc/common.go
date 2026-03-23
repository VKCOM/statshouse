// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package srvfunc

type (
	// MemStats содержит статистику по использованию памяти в байтах
	// @see man proc по /proc/*/statm
	MemStats struct {
		Size  uint64
		Res   uint64
		Share uint64
		Text  uint64
		Lib   uint64
		Data  uint64
		Dt    uint64
	}

	// GCStats содержит статистику по работе GC
	GCStats struct {
		// PauseTotalMs это общее время работы GC в миллисекундах
		PauseTotalMs uint64
		// PauseTotalMcs это общее время работы GC в микросекундах
		PauseTotalMcs uint64
		// LastPausesMs это длительность всех пауз GC в мс с прошлого вызова GetGCStats (но не более размера циклического буфера)
		LastPausesMs []uint64
		// LastPausesMcs это длительность всех пауз GC в микросекундах с прошлого вызова GetGCStats (но не более размера циклического буфера)
		LastPausesMcs []uint64
		// GCCPUFraction это процент времени (real time), потраченного на GC
		GCCPUFraction float64

		prevNumGC uint64
	}
)

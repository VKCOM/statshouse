// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"sync"
)

const (
	hashDataSize = 16 * 1024
)

// buffExchange contains logic for exchange buffer between user and writer goroutine
type buffExchange struct {
	mu           sync.Mutex
	buff         []byte
	maxFileSize  int64
	finishAccept bool

	hashBuff1 [16 * 1024]byte // first 16K
	hashBuff2 []byte          // last ~16K

	rd replaceData
}

type replaceData struct {
	commitASAP   bool
	rotatePos    []int64
	crc          uint32
	offsetGlobal int64
	offsetLocal  int64
}

func newBuffEx(curCrc uint32, processedLocal int64, processedGlobal int64, maxFileSize int64) *buffExchange {
	return &buffExchange{
		maxFileSize: maxFileSize,
		buff:        make([]byte, 0, defaultBuffSize),
		rd: replaceData{
			crc:          curCrc,
			offsetGlobal: processedGlobal,
			offsetLocal:  processedLocal,
		},
	}
}

func (b *buffExchange) getSizeUnsafe() int {
	return len(b.buff)
}

func (b *buffExchange) appendLevUnsafe(data []byte) {
	startOffset := len(b.buff)

	b.buff = append(b.buff, data...)

	if padding := len(b.buff) % 4; padding != 0 {
		var zero [4]byte
		b.buff = append(b.buff, zero[:4-padding]...)
	}

	b.updatePos(b.buff[startOffset:])
}

func (b *buffExchange) replaceBuff(reuseBuff []byte) ([]byte, replaceData) {
	b.mu.Lock()
	defer b.mu.Unlock()

	buff := b.buff
	out := b.rd

	b.buff = reuseBuff[:0]
	b.rd.commitASAP = false
	b.rd.rotatePos = nil

	return buff, out
}

func (b *buffExchange) updatePos(p []byte) {
	if b.rd.offsetGlobal < hashDataSize {
		copy(b.hashBuff1[b.rd.offsetGlobal:hashDataSize], p)
	}

	hash2Boundary := b.maxFileSize - hashDataSize
	if b.rd.offsetLocal+int64(len(p)) > hash2Boundary {
		b.hashBuff2 = append(b.hashBuff2, p...)
	}

	b.rd.crc = updateCrc(b.rd.crc, p)
	b.rd.offsetLocal += int64(len(p))
	b.rd.offsetGlobal += int64(len(p))
}

func (b *buffExchange) rotateFile() {
	b.rd.offsetLocal = 0
	b.rd.rotatePos = append(b.rd.rotatePos, int64(len(b.buff)))
}

func (b *buffExchange) stopAccept() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.finishAccept = true
}

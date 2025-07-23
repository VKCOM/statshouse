// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"encoding/binary"
	"encoding/hex"
	"errors"

	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tltracing"
)

type (
	TraceID      = tltracing.TraceID
	TraceContext = tltracing.TraceContext
	TraceStatus  = int
)

var (
	errTraceIDFormat = errors.New("trace id string must have exactly 32 hex chars or be empty")

	TraceStatusDrop   TraceStatus = 0
	TraceStatusRecord TraceStatus = 1
	TraceStatusDefer  TraceStatus = 2
)

func TraceIDToByteSlice(traceID TraceID, data []byte) {
	if len(data) != 16 {
		panic("wrong TraceID size, expected 16 bytes")
	}
	binary.BigEndian.PutUint64(data[:], uint64(traceID.Hi))
	binary.BigEndian.PutUint64(data[8:], uint64(traceID.Lo))
}

func TraceIDFromByteSlice(data []byte) TraceID {
	if len(data) != 16 {
		panic("wrong TraceID size, expected 16 bytes")
	}
	return TraceID{
		Lo: int64(binary.BigEndian.Uint64(data[8:])),
		Hi: int64(binary.BigEndian.Uint64(data[:8])),
	}
}

func TraceIDToString(traceID TraceID) string {
	if traceID.Hi == 0 && traceID.Lo == 0 { // All bytes as zero is invalid trace ID
		return ""
	}
	var data [16]byte
	TraceIDToByteSlice(traceID, data[:])
	var dataHex [32]byte
	hex.Encode(dataHex[:], data[:])
	return string(dataHex[:])
}

func TraceIDFromString(str string) (TraceID, error) {
	if len(str) == 0 { // Special case for empty trace ID
		return TraceID{}, nil
	}
	if len(str) != 32 {
		return TraceID{}, errTraceIDFormat
	}
	var data [16]byte
	if _, err := hex.Decode(data[:], []byte(str)); err != nil {
		return TraceID{}, errTraceIDFormat
	}
	return TraceIDFromByteSlice(data[:]), nil
}

func GetTraceStatus(tc *TraceContext) TraceStatus {
	var status int
	if tc.IsSetReservedStatus0() {
		status |= 1
	}
	if tc.IsSetReservedStatus1() {
		status |= 2
	}
	// unknown status fallbacks to drop
	if status > 2 || status < 0 {
		status = 0
	}
	return TraceStatus(status)
}

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package logzcore

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	TraceLevel = zap.DebugLevel - 1
	DebugLevel = zap.DebugLevel
	InfoLevel  = zap.InfoLevel
	WarnLevel  = zap.WarnLevel
	ErrorLevel = zap.ErrorLevel
)

func CapitalLevelEncoder(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	if level == TraceLevel {
		enc.AppendString("TRACE")
		return
	}
	zapcore.CapitalLevelEncoder(level, enc)
}

func LowerCaseLevelEncoder(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	if level == TraceLevel {
		enc.AppendString("trace")
		return
	}
	zapcore.LowercaseLevelEncoder(level, enc)
}

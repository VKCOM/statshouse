// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package logz

import "sync/atomic"

var facadeLogger atomic.Value

const FacadeDefaultCallerSkipOffset = 2

func init() {
	logger, err := New(Config{
		Level:             "info",
		App:               "facade",
		Version:           "v0.0.0",
		Encoding:          "console",
		DisableStacktrace: false,
		Outputs:           []string{"stderr"},
		CallerSkipOffset:  FacadeDefaultCallerSkipOffset,
	})
	if err != nil {
		panic(err)
	}

	StoreFacadeLogger(logger)
}

func StoreFacadeLogger(logger *Logger) {
	facadeLogger.Store(logger)
}

func loadFacadeLogger() *Logger {
	return facadeLogger.Load().(*Logger)
}

func Sync() error {
	return loadFacadeLogger().Sync()
}

func With(args ...Field) *Logger {
	return loadFacadeLogger().With(args...)
}

func Trace(message string, args ...Field) {
	loadFacadeLogger().Trace(message, args...)
}

func Debug(message string, args ...Field) {
	loadFacadeLogger().Debug(message, args...)
}

func Info(message string, args ...Field) {
	loadFacadeLogger().Info(message, args...)
}

func Warn(message string, args ...Field) {
	loadFacadeLogger().Warn(message, args...)
}

func Error(message string, args ...Field) {
	loadFacadeLogger().Error(message, args...)
}

func Panic(message string, args ...Field) {
	loadFacadeLogger().Panic(message, args...)
}

func Fatal(message string, args ...Field) {
	loadFacadeLogger().Fatal(message, args...)
}

func Tracef(message string, args ...interface{}) {
	loadFacadeLogger().Tracef(message, args...)
}

func Debugf(message string, args ...interface{}) {
	loadFacadeLogger().Debugf(message, args...)
}

func Infof(message string, args ...interface{}) {
	loadFacadeLogger().Infof(message, args...)
}

func Warnf(message string, args ...interface{}) {
	loadFacadeLogger().Warnf(message, args...)
}

func Errorf(message string, args ...interface{}) {
	loadFacadeLogger().Errorf(message, args...)
}

func Panicf(message string, args ...interface{}) {
	loadFacadeLogger().Panicf(message, args...)
}

func Fatalf(message string, args ...interface{}) {
	loadFacadeLogger().Fatalf(message, args...)
}

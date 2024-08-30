// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package binlog

import "fmt"

type Logger interface {
	Tracef(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type EmptyLogger struct {
}

func (*EmptyLogger) Tracef(format string, args ...interface{}) {}
func (*EmptyLogger) Debugf(format string, args ...interface{}) {}
func (*EmptyLogger) Infof(format string, args ...interface{})  {}
func (*EmptyLogger) Warnf(format string, args ...interface{})  {}
func (*EmptyLogger) Errorf(format string, args ...interface{}) {}

type StdoutLogger struct {
}

func (*StdoutLogger) Tracef(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
func (*StdoutLogger) Debugf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
func (*StdoutLogger) Infof(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
func (*StdoutLogger) Warnf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
func (*StdoutLogger) Errorf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

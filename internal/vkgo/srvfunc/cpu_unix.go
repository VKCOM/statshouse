// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build unix

package srvfunc

import "syscall"

func (ci *CPUInfo) readProcSelfStat() (totalTime float64, err error) {
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ci.rUsage)
	totalTime = float64(ci.rUsage.Stime.Nano()+ci.rUsage.Utime.Nano()) / 1e9
	return totalTime, nil
}

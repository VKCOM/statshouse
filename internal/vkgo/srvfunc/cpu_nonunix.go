// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !unix

package srvfunc

func (ci *CPUInfo) readProcSelfStat() (totalTime float64, err error) {
	return 0, nil
}

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build linux

package rpc

import (
	"syscall"
)

func (pc *PacketConn) TCPSetsockoptInt(level int, opt int, value int) error {
	if pc.tcpconn_fd == nil {
		return nil
	}
	return syscall.SetsockoptInt(int(pc.tcpconn_fd.Fd()), level, opt, value)
}

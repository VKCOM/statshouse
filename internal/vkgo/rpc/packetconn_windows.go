// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build windows
// +build windows

package rpc

import (
	"syscall"
)

func (pc *PacketConn) TCPSetsockoptInt(level int, opt int, value int) error {
	return nil
}

func ControlSetTCPReuse(addr bool, port bool) func(_ /*network*/ string, _ /*address*/ string, c syscall.RawConn) error {
	return func(_ string, _ string, c syscall.RawConn) error {
		return nil
	}
}

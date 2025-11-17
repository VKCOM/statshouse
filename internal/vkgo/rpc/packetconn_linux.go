// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build linux

package rpc

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// We want to dynamically set traffic class in Barsic and other projects, hence optimization with pc.tcpconn_fd
func (pc *PacketConn) TCPSetsockoptInt(level int, opt int, value int) error {
	if pc.tcpconn_fd == nil {
		return nil
	}
	return syscall.SetsockoptInt(int(pc.tcpconn_fd.Fd()), level, opt, value)
}

// addr: service can listen on the port again after restart, despite sockets in WAIT_CLOSED state
// port: 2 or more instances of the service can listen on the same port
func ControlSetTCPReuse(addr bool, port bool) func(_ /*network*/ string, _ /*address*/ string, c syscall.RawConn) error {
	return func(_ string, _ string, c syscall.RawConn) error {
		if !addr && !port {
			return nil
		}
		var opErr error
		err := c.Control(func(fd uintptr) {
			// this is a no-op for Unix sockets
			if addr {
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
			}
			if opErr == nil && port {
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			}
		})
		if err != nil {
			return err
		}
		return opErr
	}
}

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build darwin
// +build darwin

package receiver

import "syscall"

func setSocketReceiveBufferSize(fd int, bufferSize int) {
	if syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, bufferSize) != nil {
		// Either we don't have CAP_NET_ADMIN priviledge to set SO_RCVBUFFORCE
		// or buffer size is beyond configured system limit.
		// Trying to set the largest value possible.
		var curr int
		if n, err := syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF); err == nil {
			curr = n
		}
		for bufferSize /= 2; curr < bufferSize; bufferSize /= 2 {
			if syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, bufferSize) == nil {
				break
			}

		}
	}
}

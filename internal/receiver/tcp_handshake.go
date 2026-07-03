// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package receiver

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
)

const (
	TCPPrefix          = "statshousev"
	TCPMagicV1Default  = byte('1')
	TCPMagicV2Balancer = byte('2')
	MaxTCPFrameBody    = math.MaxUint16
)

func readTCPHandshake(conn net.Conn) (host string, err error) {
	ver, err := readConnByte(conn)
	if err != nil {
		return "", err
	}
	switch ver {
	case TCPMagicV1Default:
		return "", nil
	case TCPMagicV2Balancer:
		var lenBuf [4]byte
		if _, err = io.ReadFull(conn, lenBuf[:]); err != nil {
			return "", err
		}
		hostLen := binary.LittleEndian.Uint32(lenBuf[:])
		if hostLen > MaxTCPFrameBody {
			return "", fmt.Errorf("statshouse TCP host length %d exceeds max %d", hostLen, MaxTCPFrameBody)
		}
		if hostLen != 0 {
			hostBytes := make([]byte, hostLen)
			if _, err = io.ReadFull(conn, hostBytes); err != nil {
				return string(hostBytes), err
			}
		}
		return "", nil
	default:
		return "", fmt.Errorf("unsupported statshouse TCP version %q", ver)
	}
}

func readConnByte(conn net.Conn) (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(conn, b[:])
	return b[0], err
}

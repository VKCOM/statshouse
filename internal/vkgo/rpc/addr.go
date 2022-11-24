// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"encoding/binary"
	"net"
	"strconv"
	"strings"
)

func extractIPPort(addr net.Addr) (uint32, uint16) {
	switch addr := addr.(type) {
	case *net.TCPAddr:
		ip := addr.IP.To4()
		if ip == nil {
			return 0, uint16(addr.Port)
		}
		return binary.BigEndian.Uint32(ip), uint16(addr.Port)
	case *net.UnixAddr:
		i := strings.LastIndexByte(addr.Name, '/')
		if i == -1 || i == len(addr.Name)-1 {
			return 0, 0
		}
		port, err := strconv.ParseUint(addr.Name[i+1:], 10, 16)
		if err != nil {
			return 0, 0
		}
		return 0, uint16(port)
	default:
		return 0, 0
	}
}

func sameMachine(local net.Addr, remote net.Addr) bool {
	_, localIsUnix := local.(*net.UnixAddr)
	_, remoteIsUnix := remote.(*net.UnixAddr)
	if localIsUnix && remoteIsUnix {
		return true
	}

	localTCP, _ := local.(*net.TCPAddr)
	remoteTCP, _ := remote.(*net.TCPAddr)
	if localTCP != nil && remoteTCP != nil {
		return localTCP.IP.IsLoopback() && remoteTCP.IP.IsLoopback()
	}

	return false
}

func sameSubnetGroup(local net.Addr, remote net.Addr, subnetGroups [][]*net.IPNet) bool {
	localTCP, _ := local.(*net.TCPAddr)
	remoteTCP, _ := remote.(*net.TCPAddr)
	if localTCP == nil || remoteTCP == nil {
		return false
	}

	for _, group := range subnetGroups {
		foundLoc := false
		foundRem := false
		for _, n := range group {
			foundLoc = foundLoc || n.Contains(localTCP.IP)
			foundRem = foundRem || n.Contains(remoteTCP.IP)
		}
		if foundLoc && foundRem {
			return true
		}
	}

	return false
}

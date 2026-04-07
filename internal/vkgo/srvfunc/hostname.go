// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package srvfunc

import (
	"net"
	"os"
	"strconv"
	"strings"
)

var hostNameCache string

func init() {
	hostNameCache, _ = os.Hostname() // TODO - panic/fatal on error?
}

// cached, not expected to change
func Hostname() string {
	return hostNameCache
}

// Deprecated: always prefer Hostname() when saving in logs, etc
func NumericHost() (uint64, error) {
	numHost := ""
	for _, v := range hostNameCache {
		if v >= '0' && v <= '9' {
			numHost += string(v)
		}
	}
	return strconv.ParseUint(numHost, 10, 64)
}

// For now, return only IPv4 addresses. If they are empty, return all.
func ExternalIPAddrs() string {
	var ipv4 []string
	var all []string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "<error>"
	}
	for _, addr := range addrs {
		switch v := addr.(type) {
		case *net.IPNet:
			if !v.IP.IsLoopback() {
				if v.IP.To4() != nil {
					ipv4 = append(ipv4, v.IP.String())
				}
				all = append(all, v.IP.String())
			}
		default:
		}
	}
	if len(ipv4) != 0 {
		return strings.Join(ipv4, ",")
	}
	return strings.Join(all, ",")
}

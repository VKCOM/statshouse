// Copyright 2022 V Kontakte LLC
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
var hostNameCacheForStatshouse string

func init() {
	hostNameCache, _ = os.Hostname() // TODO - panic/fatal on error?
	// Motivation - Kubernetes hostname includes deployment ID which prevents grouping and creates mapping flood
	// glavred-production-mltasks-5649b7d9b4-d5qtf
	// keanu-rq-production-mltasks-d665549fb-6phdc
	// we check for pattern, then strip deployment ID.
	hostNameCacheForStatshouse = hostNameCache
	n, ok := removeKuberDeploymentName(hostNameCache)
	if !ok {
		return // this check is faster than env
	}
	insideKuber :=
		os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS") != "" ||
			os.Getenv("KUBERNETES_PORT") != "" ||
			os.Getenv("KUBERNETES_PORT_443_TCP") != "" ||
			os.Getenv("KUBERNETES_PORT_443_TCP_PROTO") != "" ||
			os.Getenv("KUBERNETES_PORT_443_TCP_PORT") != "" ||
			os.Getenv("KUBERNETES_PORT_443_TCP_ADDR") != "" ||
			os.Getenv("KUBERNETES_SERVICE_HOST") != "" ||
			os.Getenv("KUBERNETES_SERVICE_PORT") != ""
	if !insideKuber {
		return
	}
	hostNameCacheForStatshouse = n
}

func removeKuberDeploymentName(str string) (string, bool) {
	if len(str) < 6 || str[len(str)-5-1] != '-' {
		return str, false
	}
	if len(str) > 17 && str[len(str)-5-1-10-1] == '-' {
		return str[:len(str)-5-1], true
	}
	if len(str) > 16 && str[len(str)-5-1-9-1] == '-' {
		return str[:len(str)-5-1], true
	}
	return str, false
}

// cached, not expected to change
func Hostname() string {
	return hostNameCache
}

// cached, without Kubernetes deployment ID, recommended for use as statshouse keys
func HostnameForStatshouse() string {
	return hostNameCacheForStatshouse
}

// Deprecated: always prefer Hostname() or HostnameForStatshouse() when saving in logs, etc
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

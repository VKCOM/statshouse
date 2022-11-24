// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package srvfunc

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	cachePeriod = time.Second
	hostsFile   = "/etc/hosts"
)

var (
	emptyDialer = &net.Dialer{}

	hostsCache = struct {
		m        map[string]string // hostname => ip, if nil, first invocation
		lastStat os.FileInfo       // stats of hostsFile just before we read it
		sync.RWMutex
	}{}
)

func readHosts() map[string]string {
	newMap := make(map[string]string)
	body, err := os.ReadFile(hostsFile) // We presume we have enough memory to load hosts file
	if err != nil {
		log.Printf("srvfunc.CachingDialer could not open '%s': %v", hostsFile, err)
		return newMap
	}
	for len(body) != 0 {
		line := body
		nextLineIndex := bytes.IndexByte(body, '\n')
		if nextLineIndex >= 0 {
			line = body[:nextLineIndex]
			body = body[nextLineIndex+1:]
		} else {
			body = nil
		}

		commentIndex := bytes.IndexByte(line, '#')
		if commentIndex >= 0 {
			line = line[:commentIndex]
		}

		// 127.0.0.1       localhost loclahost loclhsot lolcahost
		fields := bytes.Fields(line)

		if len(fields) <= 1 {
			continue
		}
		ips := string(fields[0])

		// ensure that it is IPv4 address because we do not support dual stack in this resolver anyway
		if ip := net.ParseIP(ips); ip == nil || ip.To4() == nil {
			continue
		}

		for _, f := range fields[1:] {
			newMap[string(f)] = ips
		}
	}
	return newMap
}

func hostsUpdater() {
	for {
		time.Sleep(cachePeriod) // At start, because hosts was just read by the caller of go hostsUpdater
		stat, _ := os.Stat(hostsFile)
		hostsCache.RLock()
		if (stat == nil && hostsCache.lastStat == nil) || (stat != nil && hostsCache.lastStat != nil && hostsCache.lastStat.ModTime() == stat.ModTime() && hostsCache.lastStat.Size() == stat.Size()) {
			hostsCache.RUnlock()
			continue
		}
		hostsCache.RUnlock()
		newMap := readHosts()
		hostsCache.Lock()
		hostsCache.lastStat = stat
		hostsCache.m = newMap
		hostsCache.Unlock()
	}
}

func etcHostsLookup(hostname string) string {
	hostsCache.RLock()
	if hostsCache.m == nil {
		hostsCache.RUnlock()
		hostsCache.Lock()
		if hostsCache.m == nil {
			hostsCache.lastStat, _ = os.Stat(hostsFile)
			hostsCache.m = readHosts()
			go hostsUpdater()
		}
		hostsCache.Unlock()
		hostsCache.RLock()
	}
	ip := hostsCache.m[hostname]
	hostsCache.RUnlock()
	return ip
}

func MaybeResolveHost(network string, addr string) string {
	switch network {
	case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
	default:
		return addr
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	if ip := net.ParseIP(host); ip != nil {
		return addr
	}

	if hostIP := etcHostsLookup(host); hostIP != "" {
		return net.JoinHostPort(hostIP, port)
	}

	return addr
}

// CachingDialer should be used as DialContext function in http.Transport to speed up DNS resolution dramatically.
func CachingDialer(ctx context.Context, network, addr string) (net.Conn, error) {
	return emptyDialer.DialContext(ctx, network, MaybeResolveHost(network, addr))
}

// if address contains no port, defaultPort will be added, unless it is empty, then error is returned
func ValidateAddress(address string, defaultPort string) (string, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", fmt.Errorf("address must not be empty")
	}
	// TODO - discuss and make better validation. For now, anything injectable will contain slash or percent
	if strings.IndexByte(address, '/') >= 0 || strings.IndexByte(address, '%') >= 0 {
		return "", fmt.Errorf("address must not contain slash or percent in %q", address)
	}
	if !strings.ContainsRune(address, ':') {
		if defaultPort == "" {
			return "", fmt.Errorf("address contains no port and no default port specified in %q", address)
		}
		// TODO - disallow empty addresses
		return net.JoinHostPort(address, defaultPort), nil
	}
	_, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("could not split host and port in %q: %w", address, err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return "", fmt.Errorf("could not parse port %q: %w", portStr, err)
	}
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("port outside allowed range %q", portStr)
	}
	return address, nil
}

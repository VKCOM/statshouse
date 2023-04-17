// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package srvfunc

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/vkcom/statshouse/internal/vkgo/etchosts"
)

var emptyDialer = &net.Dialer{}

// MaybeResolveHost resolves hostnames to ip:port if it's TCP or UDP network
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

	if hostIP := etchosts.Resolve(host); hostIP != "" {
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

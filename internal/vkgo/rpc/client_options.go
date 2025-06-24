// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"net"
	"time"
)

type ClientOptions struct {
	Logf                LoggerFunc // defaults to log.Printf; set to NoopLogf to disable all logging
	Hooks               func() ClientHooks
	TracingInject       TracingInjectFunc
	TrustedSubnetGroups [][]*net.IPNet
	ForceEncryption     bool
	CryptoKey           string
	ConnReadBufSize     int
	ConnWriteBufSize    int
	PacketTimeout       time.Duration
	ProtocolVersion     uint32        // if >0, will send modified nonce packet. Server must be upgraded to at least ignore higher bits of nonce.Schema
	DefaultTimeout      time.Duration // has no effect if <= 0
	localUDPAddress     string        // experimental, for tests only
	MaxReconnectDelay   time.Duration
	DebugUdp            int

	trustedSubnetGroupsParseErrors []error
}

type ClientOptionsFunc func(*ClientOptions)

func ClientWithLogf(f LoggerFunc) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.Logf = f
	}
}

func ClientWithHooks(hooks func() ClientHooks) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.Hooks = hooks
	}
}

func ClientWithTracingInject(inject TracingInjectFunc) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.TracingInject = inject
	}
}

func ClientWithTrustedSubnetGroups(groups [][]string) ClientOptionsFunc {
	return func(o *ClientOptions) {
		gs, errs := ParseTrustedSubnets(groups)
		o.TrustedSubnetGroups = gs
		o.trustedSubnetGroupsParseErrors = errs
	}
}

func ClientWithForceEncryption(force bool) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.ForceEncryption = force
	}
}

func ClientWithCryptoKey(key string) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.CryptoKey = key
	}
}

func ClientWithConnReadBufSize(size int) ClientOptionsFunc {
	return func(o *ClientOptions) {
		if size > 0 {
			o.ConnReadBufSize = size
		} else {
			o.ConnReadBufSize = DefaultClientConnReadBufSize
		}
	}
}

func ClientWithConnWriteBufSize(size int) ClientOptionsFunc {
	return func(o *ClientOptions) {
		if size > 0 {
			o.ConnWriteBufSize = size
		} else {
			o.ConnWriteBufSize = DefaultClientConnWriteBufSize
		}
	}
}

// Changing this setting is not recommended and only added for use in testing environments.
// timeout <= 0 means no timeout. This can lead to connections stuck forever.
// timeout on the order of ping latency will lead to random disconnects and even inability to communicate at all.
func ClientWithPacketTimeout(timeout time.Duration) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.PacketTimeout = timeout
	}
}

func ClientWithMaxReconnectDelay(delay time.Duration) ClientOptionsFunc {
	return func(o *ClientOptions) {
		if delay > 0 {
			o.MaxReconnectDelay = delay
		} else {
			o.MaxReconnectDelay = maxReconnectDelay
		}
	}
}

func ClientWithProtocolVersion(protocolVersion uint32) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.ProtocolVersion = protocolVersion
	}
}

// Services must not communicate via UDP directly, they must use TCP/Unix connection to local RPC Proxy
// This option is only for tests and implementing RPC proxies.
func ClientWithExperimentalLocalUDPAddress(localUDPAddress string) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.localUDPAddress = localUDPAddress
	}
}

func ClientWithDebugUdp(DebugUdp int) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.DebugUdp = DebugUdp
	}
}

// Default timeout for requests with no timeout in context
// Not recommended, as affects all requests even those which need infinite timeout (some long polls)
// If this timeout set, and you need request with an infinite timeout, use Extra.SetTimeoutMs(0)
// Commented out for now to prevent abuse.
// func ClientWithRequestTimeout(timeout time.Duration) ClientOptionsFunc {
//	return func(opts *ClientOptions) {
//		opts.DefaultTimeout = timeout
//	}
// }

// Copyright 2024 V Kontakte LLC
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
	TrustedSubnetGroups [][]*net.IPNet
	ForceEncryption     bool
	CryptoKey           string
	ConnReadBufSize     int
	ConnWriteBufSize    int
	PacketTimeout       time.Duration
	ProtocolVersion     uint32 // if >0, will send modified nonce packet. Server must be upgraded to at least ignore higher bits of nonce.Schema

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
		}
	}
}

func ClientWithConnWriteBufSize(size int) ClientOptionsFunc {
	return func(o *ClientOptions) {
		if size > 0 {
			o.ConnWriteBufSize = size
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

func ClientWithProtocolVersion(protocolVersion uint32) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.ProtocolVersion = protocolVersion
	}
}

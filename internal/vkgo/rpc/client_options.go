// Copyright 2022 V Kontakte LLC
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
	Hooks               func() ClientHookState
	TrustedSubnetGroups [][]*net.IPNet
	ForceEncryption     bool
	CryptoKey           string
	ConnReadBufSize     int
	ConnWriteBufSize    int
	PongTimeout         time.Duration

	trustedSubnetGroupsParseErrors []error
}

type ClientOptionsFunc func(*ClientOptions)

func ClientWithLogf(f LoggerFunc) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.Logf = f
	}
}

func ClientWithHooks(hooks func() ClientHookState) ClientOptionsFunc {
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

func ClientWithPongTimeout(timeout time.Duration) ClientOptionsFunc {
	return func(o *ClientOptions) {
		if timeout > 0 {
			o.PongTimeout = timeout
		}
	}
}

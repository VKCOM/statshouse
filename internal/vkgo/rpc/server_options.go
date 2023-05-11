// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"bytes"
	"net"
	"time"
)

type ServerHooks struct {
	InitState  func() any
	ResetState func(any)
	Handler    HandlerHooks
}

type HandlerHooks struct {
	BeforeCall func(state any, hctx *HandlerContext)
	AfterCall  func(state any, hctx *HandlerContext, err error)
}

type ServerOptions struct {
	Logf                   LoggerFunc // defaults to log.Printf; set to NoopLogf to disable all logging
	Hooks                  ServerHooks
	SyncHandler            HandlerFunc
	Handler                HandlerFunc
	StatsHandler           StatsHandlerFunc
	VerbosityHandler       VerbosityHandlerFunc
	Version                string
	TransportHijackHandler func(conn *PacketConn) // Experimental, server handles connection to this function if FlagP2PHijack client flag set
	TrustedSubnetGroups    [][]*net.IPNet
	ForceEncryption        bool
	CryptoKeys             []string
	MaxConns               int           // defaults to DefaultMaxConns
	MaxWorkers             int           // defaults to DefaultMaxWorkers; negative values disable worker pool completely
	MaxInflightPackets     int           // defaults to DefaultMaxInflightPackets
	RequestMemoryLimit     int           // defaults to DefaultRequestMemoryLimit
	ResponseMemoryLimit    int           // defaults to DefaultResponseMemoryLimit
	ConnReadBufSize        int           // defaults to DefaultServerConnReadBufSize
	ConnWriteBufSize       int           // defaults to DefaultServerConnWriteBufSize
	RequestBufSize         int           // defaults to DefaultServerRequestBufSize
	ResponseBufSize        int           // defaults to DefaultServerResponseBufSize
	ResponseMemEstimate    int           // defaults to DefaultResponseMemEstimate; must be greater than ResponseBufSize
	DefaultResponseTimeout time.Duration // defaults to no timeout
	ResponseTimeoutAdjust  time.Duration
	DisableContextTimeout  bool
	DisableTCPReuseAddr    bool
	LogCommonNetworkErrors bool
	SocketHijackAddr       net.Addr

	trustedSubnetGroupsParseErrors []error
}

type ServerOptionsFunc func(*ServerOptions)

func ServerWithHooks(hooks ServerHooks) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.Hooks = hooks
	}
}

func ServerWithLogf(logf LoggerFunc) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.Logf = logf
	}
}

func ServerWithHandler(handler HandlerFunc) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.Handler = handler
	}
}

// syncHandler is called directly from receive loop and must not wait anything
// if syncHandler returns ErrNoHandler, normal handler will be called from worker
// Only syncHandler can hujack responses for later processing
func ServerWithSyncHandler(syncHandler HandlerFunc) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.SyncHandler = syncHandler
	}
}

func ServerWithStatsHandler(handler StatsHandlerFunc) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.StatsHandler = handler
	}
}

func ServerWithVerbosityHandler(handler VerbosityHandlerFunc) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.VerbosityHandler = handler
	}
}

func ServerWithVersion(version string) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.Version = version
	}
}

func ServerWithTransportHijackHandler(handler func(conn *PacketConn)) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.TransportHijackHandler = handler
	}
}

func ServerWithTrustedSubnetGroups(groups [][]string) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		gs, errs := ParseTrustedSubnets(groups)
		opts.TrustedSubnetGroups = gs
		opts.trustedSubnetGroupsParseErrors = errs
	}
}

func ServerWithForceEncryption(status bool) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.ForceEncryption = status
	}
}

func ServerWithCryptoKeys(keys []string) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.CryptoKeys = keys
	}
}

func ServerWithMaxConns(maxConns int) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if maxConns > 0 {
			opts.MaxConns = maxConns
		}
	}
}

func ServerWithMaxWorkers(maxWorkers int) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if maxWorkers > 0 {
			opts.MaxWorkers = maxWorkers
		} else {
			opts.MaxWorkers = 0
		}
	}
}

func ServerWithMaxInflightPackets(maxInflightPackets int) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if maxInflightPackets > 0 {
			opts.MaxInflightPackets = maxInflightPackets
		}
	}
}

func ServerWithRequestMemoryLimit(limit int) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if limit > maxPacketLen {
			opts.RequestMemoryLimit = limit
		}
	}
}

func ServerWithResponseMemoryLimit(limit int) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if limit > maxPacketLen {
			opts.ResponseMemoryLimit = limit
		}
	}
}

func ServerWithConnReadBufSize(size int) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if size > 0 {
			opts.ConnReadBufSize = size
		}
	}
}

func ServerWithConnWriteBufSize(size int) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if size > 0 {
			opts.ConnWriteBufSize = size
		}
	}
}

func ServerWithRequestBufSize(size int) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if size > bytes.MinRead {
			opts.RequestBufSize = size
		}
	}
}

func ServerWithResponseBufSize(size int) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if size > bytes.MinRead {
			opts.ResponseBufSize = size
		}
	}
}

func ServerWithResponseMemEstimate(size int) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if size > 0 {
			opts.ResponseMemEstimate = size
		} else if size == -1 {
			opts.ResponseMemEstimate = 0
		}
	}
}

func ServerWithDefaultResponseTimeout(timeout time.Duration) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if timeout > 0 {
			opts.DefaultResponseTimeout = timeout
		}
	}
}

func ServerWithResponseTimeoutAdjust(adjust time.Duration) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if adjust > 0 {
			opts.ResponseTimeoutAdjust = adjust
		}
	}
}

func ServerWithDisableContextTimeout(status bool) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.DisableContextTimeout = status
	}
}

func ServerWithDisableTCPReuseAddr() ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.DisableTCPReuseAddr = true
	}
}

func ServerWithLogCommonNetworkErrors(status bool) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.LogCommonNetworkErrors = status
	}
}

// Experimental - you must use server as net.Listener, otherwise it will block on accepting new connections
func ServerWithSocketHijackAddr(addr net.Addr) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.SocketHijackAddr = addr
	}
}

func (opts *ServerOptions) noopHooks() {
	if opts.Hooks.InitState == nil {
		opts.Hooks.InitState = func() any { return nil }
	}
	if opts.Hooks.ResetState == nil {
		opts.Hooks.ResetState = func(any) {}
	}
	if opts.Hooks.Handler.BeforeCall == nil {
		opts.Hooks.Handler.BeforeCall = func(any, *HandlerContext) {}
	}
	if opts.Hooks.Handler.AfterCall == nil {
		opts.Hooks.Handler.AfterCall = func(any, *HandlerContext, error) {}
	}
}

func (opts *ServerOptions) maxInflightPacketsPreAlloc() int {
	if opts.MaxInflightPackets < DefaultMaxInflightPackets {
		return opts.MaxInflightPackets
	}
	return DefaultMaxInflightPackets
}

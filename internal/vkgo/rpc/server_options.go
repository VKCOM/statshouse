// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"bytes"
	"log"
	"net"
	"strings"
	"time"

	"github.com/vkcom/statshouse-go"
)

type ServerHookState interface {
	Reset()
	BeforeCall(hctx *HandlerContext)
	AfterCall(hctx *HandlerContext, err error)
}

type ServerOptions struct {
	Logf                   LoggerFunc // defaults to log.Printf; set to NoopLogf to disable all logging
	Hooks                  func() ServerHookState
	SyncHandler            HandlerFunc
	Handler                HandlerFunc
	StatsHandler           StatsHandlerFunc
	VerbosityHandler       VerbosityHandlerFunc
	Version                string
	TransportHijackHandler func(conn *PacketConn) // Experimental, server handles connection to this function if FlagP2PHijack client flag set
	SocketHijackHandler    func(conn *HijackConnection)
	TrustedSubnetGroupsSt  string // for stats
	TrustedSubnetGroups    [][]*net.IPNet
	ForceEncryption        bool
	cryptoKeys             []string
	MaxConns               int           // defaults to DefaultMaxConns
	MaxWorkers             int           // defaults to DefaultMaxWorkers; <= value disables worker pool completely
	MaxInflightPackets     int           // defaults to DefaultMaxInflightPackets
	RequestMemoryLimit     int           // defaults to DefaultRequestMemoryLimit
	ResponseMemoryLimit    int           // defaults to DefaultResponseMemoryLimit
	ConnReadBufSize        int           // defaults to DefaultServerConnReadBufSize
	ConnWriteBufSize       int           // defaults to DefaultServerConnWriteBufSize
	RequestBufSize         int           // defaults to DefaultServerRequestBufSize
	ResponseBufSize        int           // defaults to DefaultServerResponseBufSize
	ResponseMemEstimate    int           // defaults to DefaultResponseMemEstimate; must be greater than ResponseBufSize
	DefaultResponseTimeout time.Duration // defaults to no timeout (0)
	ResponseTimeoutAdjust  time.Duration
	DisableContextTimeout  bool
	DisableTCPReuseAddr    bool
	DebugRPC               bool // prints all incoming and outgoing RPC activity (very slow, only for protocol debug)
	CommonTags             statshouse.Tags
}

func (opts *ServerOptions) AddCryptoKey(key string) {
	opts.cryptoKeys = append(opts.cryptoKeys, key)
}

type ServerOptionsFunc func(*ServerOptions)

func ServerWithHooks(hooks func() ServerHookState) ServerOptionsFunc {
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

func ServerWithDebugRPC(debugRpc bool) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.DebugRPC = debugRpc
	}
}

// syncHandler is called directly from receive loop and must not wait anything
// if syncHandler returns ErrNoHandler, normal handler will be called from worker
// Only syncHandler can hujack longpoll responses for later processing
// You must not use Request or UserData after return from sync hanlder, they are reused by other calls
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

func TrustedSubnetGroupsString(groups [][]string) string {
	b := strings.Builder{}
	for i, g := range groups {
		if i != 0 {
			b.WriteString(";")
		}
		for j, m := range g {
			if j != 0 {
				b.WriteString(",")
			}
			b.WriteString(m)
		}
	}
	return b.String()
}

func ServerWithTrustedSubnetGroups(groups [][]string) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		gs, errs := ParseTrustedSubnets(groups)
		for _, err := range errs {
			// we do not return error from this function, and do not want to ignore this error
			log.Panicf("[rpc] failed to parse server trusted subnet: %v", err)
		}
		opts.TrustedSubnetGroups = gs
		opts.TrustedSubnetGroupsSt = TrustedSubnetGroupsString(groups)
	}
}

func ServerWithForceEncryption(status bool) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.ForceEncryption = status
	}
}

func ServerWithCryptoKeys(keys []string) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		for _, key := range keys {
			opts.AddCryptoKey(key)
		}
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
		opts.MaxWorkers = maxWorkers
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
		} else {
			opts.RequestBufSize = bytes.MinRead
		}
	}
}

func ServerWithResponseBufSize(size int) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if size > bytes.MinRead {
			opts.ResponseBufSize = size
		} else {
			opts.ResponseBufSize = bytes.MinRead
		}
	}
}

func ServerWithResponseMemEstimate(size int) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		if size > 0 {
			opts.ResponseMemEstimate = size
		} else {
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

// Reduces client timeout, compensating for network latecny
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

// All connections not classified as PacketConn are passed here. You can then insert them into HijackListener
// If you have more than 1 protocol in your app, You can examine HijackConnection.Magic in your handler to classify connection
func ServerWithSocketHijackHandler(handler func(conn *HijackConnection)) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.SocketHijackHandler = handler
	}
}

func ServerWithEnvironment(env Environment) ServerOptionsFunc {
	return func(opts *ServerOptions) {
		opts.CommonTags = statshouse.Tags{
			env.Name,
			env.Service,
			env.Cluster,
			env.DataCenter,
		}
	}
}

func (opts *ServerOptions) maxInflightPacketsPreAlloc() int {
	if opts.MaxInflightPackets < DefaultMaxInflightPackets {
		return opts.MaxInflightPackets
	}
	return DefaultMaxInflightPackets
}

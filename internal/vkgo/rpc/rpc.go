// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/constants"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tlnetUdpPacket"
)

const (
	flagCRC32C    = uint32(0x00000800) // #define RPC_CRYPTO_USE_CRC32C         0x00000800
	flagCancelReq = uint32(0x00001000) // #define RPC_CRYPTO_RPC_CANCEL_REQ     0x00001000
	FlagP2PHijack = uint32(0x40000000) // #define RPC_CRYPTO_P2P_HIJACK         0x40000000

	packetTypeRPCNonce     = uint32(0x7acb87aa)
	packetTypeRPCHandshake = uint32(0x7682eef5)

	PacketTypeRPCPing = constants.RpcPing
	PacketTypeRPCPong = constants.RpcPong
	// contains 8 byte payload

	// rpc-error-codes.h
	TLErrorSyntax           = -1000 // TL_ERROR_SYNTAX
	TlErrorNoHandler        = -2000 // TL_ERROR_UNKNOWN_FUNCTION_ID
	TlErrorTooLongString    = -2003 // TOO_LONG_STRING
	TlErrorValueNotInRange  = -2004 // VALUE_NOT_IN_RANGE
	TlErrorQueryIncorrect   = -2005 // QUERY_INCORRECT
	TlErrorBadValue         = -2006 // BAD_VALUE
	TlErrorGracefulShutdown = -2014 // TL_ERROR_GRACEFUL_SHUTDOWN
	TlErrorTimeout          = -3000 // TL_ERROR_QUERY_TIMEOUT
	TLErrorNoConnections    = -3002 // TL_ERROR_NO_CONNECTIONS
	TlErrorInternal         = -3003 // TL_ERROR_INTERNAL
	TLErrorResultToLarge    = -3011 // TL_ERROR_RESULT_TOO_LARGE
	TlErrorUnknown          = -4000 // TL_ERROR_UNKNOWN
	TlErrorResponseSyntax   = -4101 // RESPONSE_SYNTAX

	DefaultPacketTimeout = 10 * time.Second
	// keeping this above 10 seconds helps to avoid disconnecting engines with default 10 seconds ping interval
	//
	// We have single timeout for all activity.
	// Network connect is similar to request/response so must happen within this timeout
	// Then handshake is series of request/responses which must each happen within this timeout
	//
	// Before reading next packet, timer is set to this timeout.
	// If not a single byte is read from the peer before timeout triggers, ping is sent and timer is reset.
	// If not a single byte is read from the peer before timeout triggers with ping sent, connection is closed.
	// If a single byte is read, full packet must be read before timeout triggers, otherwise connection is closed.
	//
	// Before writing next packet, separate timer is set to this timeout.
	// If full packet is not completely written before timeout triggers, connection is closed.

	DefaultConnTimeoutAccuracy = 100 * time.Millisecond
	// We optimize excess SetDeadline calls
)

type RequestExtra = tl.RpcInvokeReqExtra
type ResponseExtra = tl.RpcReqResultExtra

type InvokeReqExtra struct { // additional parameters to auto generated client code
	RequestExtra // embedded for historic reasons

	// Requests fail immediately when connection fails, so that switch to fallback is faster
	// Here, because generated code calls GetRequest() so caller has no access to request
	FailIfNoConnection bool

	// By settings this, client certifies that deployed server (and all RPC proxies between) supports TL2
	// If combinator is TL1-only, request will still use TL1.
	// If combinator is TL2-only, request will always use TL2.
	// If TL2 is selected for body format, then rpc2.invokeReq will also be used as a packet format
	// Once all deployed production servers and proxies support rpc2.invokeReq packet format,
	// rpc2.invokeReq will always be used, independent of body format (which will stay TL1 for many engines).
	PreferTL2 bool

	ResponseExtra ResponseExtra // after call, response extra is available here
}

type UnencHeader = tlnetUdpPacket.UnencHeader // TODO - move to better place when UDP impl is ready
type EncHeader = tlnetUdpPacket.EncHeader     // TODO - move to better place when UDP impl is ready

// NoopLogf is a do-nothing log function
func NoopLogf(string, ...any) {}

type Error struct {
	Code        int32
	Description string
}

func NewError(code int32, description string) *Error {
	return &Error{
		Code:        code,
		Description: description,
	}
}

func NewDefaultError(description string) *Error {
	return &Error{
		Description: description,
	}
}

type tagError struct {
	tag string // can be empty, but in most cases is not
	err error  // never nil
}

func (err *Error) Error() string {
	return fmt.Sprintf("RPC error %v: %v", err.Code, err.Description)
}

func ErrorTag(err error) string {
	if err == nil {
		return ""
	}
	if e, _ := err.(*tagError); e != nil {
		s := [2]string{e.tag, ErrorTag(e.err)}
		if s[1] == "" {
			return s[0]
		}
		if s[0] == "" {
			return s[1]
		}
		return strings.Join(s[:], ":")
	}
	if e, _ := err.(net.Error); e != nil && e.Timeout() {
		return "timeout" // enough for tag value
	}
	if e := errors.Unwrap(err); e != nil {
		return ErrorTag(e)
	}
	return err.Error() // TODO - return "" instead
}

func (err *tagError) Error() string {
	return err.err.Error()
}

func (err *tagError) Unwrap() error {
	return err.err
}

type NetAddr struct {
	Network string
	Address string
}

func (na NetAddr) String() string {
	return na.Network + "://" + na.Address
}

type packetHeader struct {
	length uint32 // (body size + 16 bytes) for historic reasons
	seqNum uint32
	tip    uint32
}

// after header, there is body, then
// crc32 of (header | body)
// then (only if encrypted) must contain 0 to 3 zero bytes aligning packet to multiple of 4
// then can contain 0 to 3 values of uint32(4), aligning packet to multiple of AES encryption block.

func humanByteCountIEC(b int64) string {
	const unit int64 = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := unit, 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}

// rpc-error-codes.h
func IsUserError(errCode int32) bool {
	return errCode > -3000 && errCode <= -1000
}

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package receiver

import (
	"bytes"
	"context"
	"math"
	"net"
	"strings"
	"syscall"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/mapping"

	"go.uber.org/atomic"

	"golang.org/x/sys/unix"
)

const (
	DefaultConnBufSize = 16 * 1024 * 1024

	errClosed = "use of closed network connection" // TODO: migrate to net.ErrClosed for Go 1.16+
)

var (
	jsonPacketPrefix   = []byte("{")                    // 0x7B
	legacyPacketPrefix = []byte("SH")                   // 0x53, 0x48
	metricsBatchPrefix = []byte{0x39, 0x02, 0x58, 0x56} // little-endian 0x56580239
	// msgPack maps: 0x8* (fixmap) 0xDE (map 16), 0xDF (map 32)
	// protobuf (13337): 0x99, 0x68 (next one will be 0x9a, 0x9b, etc.)
)

type Handler interface {
	HandleMetrics(*tlstatshouse.MetricBytes, mapping.MapCallbackFunc) (h data_model.MappedMetricHeader, done bool) // if cannot process immediately, and needs to own data, should swap another metric from the pool
	HandleParseError([]byte, error)
}

type CallbackHandler struct {
	Metrics    func(*tlstatshouse.MetricBytes, mapping.MapCallbackFunc) (h data_model.MappedMetricHeader, done bool)
	ParseError func([]byte, error)
}

func (c CallbackHandler) HandleMetrics(b *tlstatshouse.MetricBytes, cb mapping.MapCallbackFunc) (h data_model.MappedMetricHeader, done bool) {
	if c.Metrics != nil {
		return c.Metrics(b, cb)
	}
	return h, true
}

func (c CallbackHandler) HandleParseError(pkt []byte, err error) {
	if c.ParseError != nil {
		c.ParseError(pkt, err)
	}
}

type UDP struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	statPacketsTotal    atomic.Uint64
	statBytesTotal      atomic.Uint64
	statBatchesTotalOK  atomic.Uint64
	statBatchesTotalErr atomic.Uint64

	conn      *net.UDPConn
	logPacket func(format string, args ...interface{})

	ag                   *agent.Agent
	batchSizeTLOK        *agent.BuiltInItemValue
	batchSizeTLErr       *agent.BuiltInItemValue
	batchSizeMsgPackOK   *agent.BuiltInItemValue
	batchSizeMsgPackErr  *agent.BuiltInItemValue
	batchSizeJSONOK      *agent.BuiltInItemValue
	batchSizeJSONErr     *agent.BuiltInItemValue
	batchSizeProtobufOK  *agent.BuiltInItemValue
	batchSizeProtobufErr *agent.BuiltInItemValue

	packetSizeTLOK        *agent.BuiltInItemValue
	packetSizeTLErr       *agent.BuiltInItemValue
	packetSizeMsgPackOK   *agent.BuiltInItemValue
	packetSizeMsgPackErr  *agent.BuiltInItemValue
	packetSizeJSONOK      *agent.BuiltInItemValue
	packetSizeJSONErr     *agent.BuiltInItemValue
	packetSizeProtobufOK  *agent.BuiltInItemValue
	packetSizeProtobufErr *agent.BuiltInItemValue
	packetSizeLegacyErr   *agent.BuiltInItemValue
	packetSizeEmptyErr    *agent.BuiltInItemValue
}

func ControlReusePort(network string, address string, conn syscall.RawConn) error {
	var operr error
	if err := conn.Control(func(fd uintptr) {
		operr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	}); err != nil {
		return err
	}
	return operr
}

func ListenUDP(address string, bufferSize int, reusePort bool, bm *agent.Agent, logPacket func(format string, args ...interface{})) (*UDP, error) {
	var conn net.PacketConn
	var err error
	if reusePort {
		config := &net.ListenConfig{Control: ControlReusePort}
		conn, err = config.ListenPacket(context.Background(), "udp", address)
	} else {
		conn, err = net.ListenPacket("udp", address)
	}
	if err != nil {
		return nil, err
	}
	uc := conn.(*net.UDPConn)

	for { // On Mac setting too large buffer is error. So we set the largest possible
		if err = uc.SetReadBuffer(bufferSize); err == nil {
			break
		}
		if bufferSize == 0 {
			return nil, err
		}
		bufferSize /= 2
	}

	return &UDP{
		conn:                  uc,
		logPacket:             logPacket,
		ag:                    bm,
		batchSizeTLOK:         createBatchSizeValue(bm, format.TagValueIDPacketFormatTL, format.TagValueIDAgentReceiveStatusOK),
		batchSizeTLErr:        createBatchSizeValue(bm, format.TagValueIDPacketFormatTL, format.TagValueIDAgentReceiveStatusError),
		batchSizeMsgPackOK:    createBatchSizeValue(bm, format.TagValueIDPacketFormatMsgPack, format.TagValueIDAgentReceiveStatusOK),
		batchSizeMsgPackErr:   createBatchSizeValue(bm, format.TagValueIDPacketFormatMsgPack, format.TagValueIDAgentReceiveStatusError),
		batchSizeJSONOK:       createBatchSizeValue(bm, format.TagValueIDPacketFormatJSON, format.TagValueIDAgentReceiveStatusOK),
		batchSizeJSONErr:      createBatchSizeValue(bm, format.TagValueIDPacketFormatJSON, format.TagValueIDAgentReceiveStatusError),
		batchSizeProtobufOK:   createBatchSizeValue(bm, format.TagValueIDPacketFormatProtobuf, format.TagValueIDAgentReceiveStatusOK),
		batchSizeProtobufErr:  createBatchSizeValue(bm, format.TagValueIDPacketFormatProtobuf, format.TagValueIDAgentReceiveStatusError),
		packetSizeTLOK:        createPacketSizeValue(bm, format.TagValueIDPacketFormatTL, format.TagValueIDAgentReceiveStatusOK),
		packetSizeTLErr:       createPacketSizeValue(bm, format.TagValueIDPacketFormatTL, format.TagValueIDAgentReceiveStatusError),
		packetSizeMsgPackOK:   createPacketSizeValue(bm, format.TagValueIDPacketFormatMsgPack, format.TagValueIDAgentReceiveStatusOK),
		packetSizeMsgPackErr:  createPacketSizeValue(bm, format.TagValueIDPacketFormatMsgPack, format.TagValueIDAgentReceiveStatusError),
		packetSizeJSONOK:      createPacketSizeValue(bm, format.TagValueIDPacketFormatJSON, format.TagValueIDAgentReceiveStatusOK),
		packetSizeJSONErr:     createPacketSizeValue(bm, format.TagValueIDPacketFormatJSON, format.TagValueIDAgentReceiveStatusError),
		packetSizeProtobufOK:  createPacketSizeValue(bm, format.TagValueIDPacketFormatProtobuf, format.TagValueIDAgentReceiveStatusOK),
		packetSizeProtobufErr: createPacketSizeValue(bm, format.TagValueIDPacketFormatProtobuf, format.TagValueIDAgentReceiveStatusError),
		packetSizeLegacyErr:   createPacketSizeValue(bm, format.TagValueIDPacketFormatLegacy, format.TagValueIDAgentReceiveStatusError),
		packetSizeEmptyErr:    createPacketSizeValue(bm, format.TagValueIDPacketFormatEmpty, format.TagValueIDAgentReceiveStatusError),
	}, nil
}

func (u *UDP) Close() error {
	return u.conn.Close()
}

func (u *UDP) Addr() string {
	return u.conn.LocalAddr().String()
}

func (u *UDP) Serve(h Handler) error {
	data := make([]byte, math.MaxUint16) // enough for any UDP packet
	var batch tlstatshouse.AddMetricsBatchBytes
outer:
	for {
		pktLen, readErr := u.conn.Read(data)
		if readErr != nil {
			if strings.Contains(readErr.Error(), errClosed) {
				return nil
			}
			return readErr
		}
		u.statPacketsTotal.Inc()
		u.statBytesTotal.Add(uint64(pktLen))
		pkt := data[:pktLen]
		if u.logPacket != nil { // formatting is slow
			u.logPacket("Incoming packet %x", pkt)
		}

		// order is important. If JSON has leading whitespaces, it will not be detected/parsed
		switch {
		case bytes.HasPrefix(pkt, metricsBatchPrefix):
			for len(pkt) > 0 {
				var err error
				was := pkt
				pkt, err = batch.ReadBoxed(pkt)
				if u.handleMetricsBatch(h, batch, was, err) {
					setValueSize(u.batchSizeTLOK, len(was)-len(pkt))
				} else {
					setValueSize(u.batchSizeTLErr, len(was))
					setValueSize(u.packetSizeTLErr, pktLen)
					continue outer
				}
			}
			setValueSize(u.packetSizeTLOK, pktLen)
		case bytes.HasPrefix(pkt, jsonPacketPrefix):
			err := batch.UnmarshalJSON(pkt)
			if u.handleMetricsBatch(h, batch, pkt, err) {
				setValueSize(u.batchSizeJSONOK, pktLen)
			} else {
				setValueSize(u.batchSizeJSONErr, pktLen)
				setValueSize(u.packetSizeJSONErr, pktLen)
				continue outer
			}
			setValueSize(u.packetSizeJSONOK, pktLen)
		case bytes.HasPrefix(pkt, legacyPacketPrefix):
			setValueSize(u.packetSizeLegacyErr, pktLen)
		case msgpackLooksLikeMap(pkt):
			for len(pkt) > 0 {
				var err error
				was := pkt
				pkt, err = msgpackUnmarshalStatshouseAddMetricBatch(&batch, pkt)
				if u.handleMetricsBatch(h, batch, was, err) {
					setValueSize(u.batchSizeMsgPackOK, len(was)-len(pkt))
				} else {
					setValueSize(u.batchSizeMsgPackErr, len(was))
					setValueSize(u.packetSizeMsgPackErr, pktLen)
					continue outer
				}
			}
			setValueSize(u.packetSizeMsgPackOK, pktLen)
		case pktLen == 0: // Do not count empty packets as protobuf
			setValueSize(u.packetSizeEmptyErr, pktLen)
		default: // assume Protobuf, it is too flexible, many wrong packets will be accounted for here
			for len(pkt) > 0 {
				var err error
				was := pkt
				pkt, err = protobufUnmarshalStatshouseAddMetricBatch(&batch, pkt)
				if u.handleMetricsBatch(h, batch, pkt, err) {
					setValueSize(u.batchSizeProtobufOK, len(was)-len(pkt))
				} else {
					setValueSize(u.batchSizeProtobufErr, len(was))
					setValueSize(u.packetSizeProtobufErr, pktLen)
					continue outer
				}
			}
			setValueSize(u.packetSizeProtobufOK, pktLen)
		}
	}
}

func (u *UDP) handleMetricsBatch(h Handler, b tlstatshouse.AddMetricsBatchBytes, pkt []byte, parseErr error) bool {
	if parseErr != nil {
		u.statBatchesTotalErr.Inc()
		h.HandleParseError(pkt, parseErr)
		return false
	}
	u.statBatchesTotalOK.Inc()

	for i := range b.Metrics {
		_, _ = h.HandleMetrics(&b.Metrics[i], nil) // might move out metric, if needs to
	}

	return true
}

func createBatchSizeValue(ag *agent.Agent, formatTagValueID int32, statusTagValueID int32) *agent.BuiltInItemValue {
	if ag != nil {
		return ag.CreateBuiltInItemValue(data_model.Key{
			Metric: format.BuiltinMetricIDAgentReceivedBatchSize,
			Keys:   [format.MaxTags]int32{0 /* env */, formatTagValueID, statusTagValueID},
		})
	}
	return nil
}

func createPacketSizeValue(bm *agent.Agent, formatTagValueID int32, statusTagValueID int32) *agent.BuiltInItemValue {
	if bm != nil {
		return bm.CreateBuiltInItemValue(data_model.Key{
			Metric: format.BuiltinMetricIDAgentReceivedPacketSize,
			Keys:   [format.MaxTags]int32{0 /* env */, formatTagValueID, statusTagValueID},
		})
	}
	return nil
}

func setValueSize(i *agent.BuiltInItemValue, length int) {
	if i != nil {
		i.AddValueCounter(float64(length), 1)
	}
}

func (u *UDP) StatPacketsTotal() uint64    { return u.statPacketsTotal.Load() }
func (u *UDP) StatBatchesTotalOK() uint64  { return u.statBatchesTotalOK.Load() }
func (u *UDP) StatBatchesTotalErr() uint64 { return u.statBatchesTotalErr.Load() }
func (u *UDP) StatBytesTotal() uint64      { return u.statBytesTotal.Load() }

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package receiver

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net"
	"syscall"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"

	"go.uber.org/atomic"
	"golang.org/x/sys/unix"
)

const (
	DefaultConnBufSize = 16 * 1024 * 1024
)

var (
	jsonPacketPrefix   = []byte("{")                    // 0x7B
	legacyPacketPrefix = []byte("SH")                   // 0x53, 0x48
	metricsBatchPrefix = []byte{0x39, 0x02, 0x58, 0x56} // little-endian 0x56580239
	// msgPack maps: 0x8* (fixmap) 0xDE (map 16), 0xDF (map 32)
	// protobuf (13337): 0x99, 0x68 (next one will be 0x9a, 0x9b, etc.)
)

type Handler interface {
	HandleMetrics(data_model.HandlerArgs) (h data_model.MappedMetricHeader, done bool) // if cannot process immediately, and needs to own data, should swap another metric from the pool
	HandleParseError([]byte, error)
}

type CallbackHandler struct {
	Metrics    func(*tlstatshouse.MetricBytes, data_model.MapCallbackFunc) (h data_model.MappedMetricHeader, done bool)
	ParseError func([]byte, error)
}

func (c CallbackHandler) HandleMetrics(args data_model.HandlerArgs) (h data_model.MappedMetricHeader, done bool) {
	if c.Metrics != nil {
		return c.Metrics(args.MetricBytes, args.MapCallback)
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

	conn      net.Conn
	rawConn   syscall.RawConn
	logPacket func(format string, args ...interface{})

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

func listenPacket(network string, address string, fn func(int) error) (conn net.PacketConn, err error) {
	cfg := &net.ListenConfig{Control: func(network, address string, c syscall.RawConn) error {
		var err2 error
		err := c.Control(func(fd uintptr) {
			err2 = fn(int(fd))
		})
		if err != nil {
			return err
		}
		return err2
	}}
	conn, err = cfg.ListenPacket(context.Background(), network, address)
	return conn, err
}

func ListenUDP(network string, address string, bufferSize int, reusePort bool, bm *agent.Agent, logPacket func(format string, args ...interface{})) (*UDP, error) {
	packetConn, err := listenPacket(network, address, func(fd int) error {
		setSocketReceiveBufferSize(fd, bufferSize)
		if reusePort {
			return syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	conn := packetConn.(net.Conn)
	scConn := packetConn.(syscall.Conn)
	rawConn, err := scConn.SyscallConn()
	if err != nil {
		return nil, err
	}
	return &UDP{
		conn:                  conn,
		rawConn:               rawConn,
		logPacket:             logPacket,
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

func (u *UDP) Duplicate() (*UDP, error) {
	result := *u // copy all fields
	cf, err := u.conn.(*net.UDPConn).File()
	if err != nil {
		return nil, err
	}
	defer cf.Close() // File() and FileConn() both dup FD
	result.conn, err = net.FileConn(cf)
	if err != nil {
		return nil, err
	}
	scConn := result.conn.(syscall.Conn)
	result.rawConn, err = scConn.SyscallConn()
	if err != nil {
		return nil, err
	}
	return &result, nil
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
			if errors.Is(readErr, net.ErrClosed) {
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
		_, _ = h.HandleMetrics(data_model.HandlerArgs{MetricBytes: &b.Metrics[i]}) // might move out metric, if needs to
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

func (u *UDP) ReceiveBufferSize() int {
	res := -1 // Special valuein case of error
	_ = u.rawConn.Control(func(fd uintptr) {
		res, _ = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	})
	return res
}

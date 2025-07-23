// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package receiver

import (
	"strings"

	"go.uber.org/atomic"

	"github.com/VKCOM/statshouse/internal/agent"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/mapping"
)

const (
	DefaultConnBufSize = 16 * 1024 * 1024
)

const (
	jsonPacketPrefix   = "{"                // 0x7B
	legacyPacketPrefix = "SH"               // 0x53, 0x48
	metricsBatchPrefix = "\x39\x02\x58\x56" // little-endian 0x56580239
	// msgPack maps: 0x8* (fixmap) 0xDE (map 16), 0xDF (map 32)
	// protobuf (13337): 0x99, 0x68 (next one will be 0x9a, 0x9b, etc.)
)

type Handler interface {
	HandleMetrics(data_model.HandlerArgs) data_model.MappedMetricHeader
	HandleParseError([]byte, error)
}

type CallbackHandler struct {
	Metrics    func(*tlstatshouse.MetricBytes) (h data_model.MappedMetricHeader)
	ParseError func([]byte, error)
}

func (c CallbackHandler) HandleMetrics(args data_model.HandlerArgs) (h data_model.MappedMetricHeader) {
	if c.Metrics != nil {
		return c.Metrics(args.MetricBytes)
	}
	return h
}

func (c CallbackHandler) HandleParseError(pkt []byte, err error) {
	if c.ParseError != nil {
		c.ParseError(pkt, err)
	}
}

type parser struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	statPacketsTotal    atomic.Uint64
	statBytesTotal      atomic.Uint64
	statBatchesTotalOK  atomic.Uint64
	statBatchesTotalErr atomic.Uint64

	logPacket func(format string, args ...interface{})

	sh2     *agent.Agent
	network string

	batchSizeTLOK        *agent.BuiltInItemValue
	batchSizeTLErr       *agent.BuiltInItemValue
	batchSizeMsgPackOK   *agent.BuiltInItemValue
	batchSizeMsgPackErr  *agent.BuiltInItemValue
	batchSizeJSONOK      *agent.BuiltInItemValue
	batchSizeJSONErr     *agent.BuiltInItemValue
	batchSizeProtobufOK  *agent.BuiltInItemValue
	batchSizeProtobufErr *agent.BuiltInItemValue
	batchSizeRPCOK       *agent.BuiltInItemValue
	batchSizeRPCErr      *agent.BuiltInItemValue

	packetSizeTLOK        *agent.BuiltInItemValue
	packetSizeTLErr       *agent.BuiltInItemValue
	packetSizeMsgPackOK   *agent.BuiltInItemValue
	packetSizeMsgPackErr  *agent.BuiltInItemValue
	packetSizeJSONOK      *agent.BuiltInItemValue
	packetSizeJSONErr     *agent.BuiltInItemValue
	packetSizeProtobufOK  *agent.BuiltInItemValue
	packetSizeProtobufErr *agent.BuiltInItemValue
	packetSizeRPCOK       *agent.BuiltInItemValue
	packetSizeRPCErr      *agent.BuiltInItemValue

	packetSizeConnect      *agent.BuiltInItemValue
	packetSizeFramingError *agent.BuiltInItemValue
	packetSizeNetworkError *agent.BuiltInItemValue
	packetSizeDisconnect   *agent.BuiltInItemValue

	packetSizeLegacyErr *agent.BuiltInItemValue
	packetSizeEmptyErr  *agent.BuiltInItemValue
}

func (u *parser) createMetrics() {
	var protocolTagValueID int32 // stays 0 if protocol is unknown to us
	switch {
	case strings.HasPrefix(u.network, "http"): // not actual network
		protocolTagValueID = format.TagValueIDPacketProtocolHTTP
	case strings.HasPrefix(u.network, "rpc"): // not actual network
		protocolTagValueID = format.TagValueIDPacketProtocolVKRPC
	case strings.HasPrefix(u.network, "udp"):
		protocolTagValueID = format.TagValueIDPacketProtocolUDP
	case strings.HasPrefix(u.network, "tcp"):
		protocolTagValueID = format.TagValueIDPacketProtocolTCP
	case strings.HasPrefix(u.network, "unixgram"):
		protocolTagValueID = format.TagValueIDPacketProtocolUnixGram
	}
	u.batchSizeTLOK = createBatchSizeValue(u.sh2, format.TagValueIDPacketFormatTL, format.TagValueIDAgentReceiveStatusOK, protocolTagValueID)
	u.batchSizeTLErr = createBatchSizeValue(u.sh2, format.TagValueIDPacketFormatTL, format.TagValueIDAgentReceiveStatusError, protocolTagValueID)
	u.batchSizeMsgPackOK = createBatchSizeValue(u.sh2, format.TagValueIDPacketFormatMsgPack, format.TagValueIDAgentReceiveStatusOK, protocolTagValueID)
	u.batchSizeMsgPackErr = createBatchSizeValue(u.sh2, format.TagValueIDPacketFormatMsgPack, format.TagValueIDAgentReceiveStatusError, protocolTagValueID)
	u.batchSizeJSONOK = createBatchSizeValue(u.sh2, format.TagValueIDPacketFormatJSON, format.TagValueIDAgentReceiveStatusOK, protocolTagValueID)
	u.batchSizeJSONErr = createBatchSizeValue(u.sh2, format.TagValueIDPacketFormatJSON, format.TagValueIDAgentReceiveStatusError, protocolTagValueID)
	u.batchSizeProtobufOK = createBatchSizeValue(u.sh2, format.TagValueIDPacketFormatProtobuf, format.TagValueIDAgentReceiveStatusOK, protocolTagValueID)
	u.batchSizeProtobufErr = createBatchSizeValue(u.sh2, format.TagValueIDPacketFormatProtobuf, format.TagValueIDAgentReceiveStatusError, protocolTagValueID)
	u.batchSizeRPCOK = createBatchSizeValue(u.sh2, format.TagValueIDPacketFormatRPC, format.TagValueIDAgentReceiveStatusOK, protocolTagValueID)
	u.batchSizeRPCErr = createBatchSizeValue(u.sh2, format.TagValueIDPacketFormatRPC, format.TagValueIDAgentReceiveStatusError, protocolTagValueID)
	u.packetSizeTLOK = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatTL, format.TagValueIDAgentReceiveStatusOK, protocolTagValueID)
	u.packetSizeTLErr = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatTL, format.TagValueIDAgentReceiveStatusError, protocolTagValueID)
	u.packetSizeMsgPackOK = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatMsgPack, format.TagValueIDAgentReceiveStatusOK, protocolTagValueID)
	u.packetSizeMsgPackErr = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatMsgPack, format.TagValueIDAgentReceiveStatusError, protocolTagValueID)
	u.packetSizeJSONOK = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatJSON, format.TagValueIDAgentReceiveStatusOK, protocolTagValueID)
	u.packetSizeJSONErr = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatJSON, format.TagValueIDAgentReceiveStatusError, protocolTagValueID)
	u.packetSizeProtobufOK = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatProtobuf, format.TagValueIDAgentReceiveStatusOK, protocolTagValueID)
	u.packetSizeProtobufErr = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatProtobuf, format.TagValueIDAgentReceiveStatusError, protocolTagValueID)
	u.packetSizeRPCOK = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatRPC, format.TagValueIDAgentReceiveStatusOK, protocolTagValueID)
	u.packetSizeRPCErr = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatRPC, format.TagValueIDAgentReceiveStatusError, protocolTagValueID)

	// we do not want 0 in format, use empty format as a convenient marker
	u.packetSizeConnect = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatEmpty, format.TagValueIDAgentReceiveStatusConnect, protocolTagValueID)
	u.packetSizeFramingError = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatEmpty, format.TagValueIDAgentReceiveStatusFramingError, protocolTagValueID)
	u.packetSizeNetworkError = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatEmpty, format.TagValueIDAgentReceiveStatusNetworkError, protocolTagValueID)
	u.packetSizeDisconnect = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatEmpty, format.TagValueIDAgentReceiveStatusDisconnect, protocolTagValueID)

	u.packetSizeLegacyErr = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatLegacy, format.TagValueIDAgentReceiveStatusError, protocolTagValueID)
	u.packetSizeEmptyErr = createPacketSizeValue(u.sh2, format.TagValueIDPacketFormatEmpty, format.TagValueIDAgentReceiveStatusError, protocolTagValueID)
}

func createBatchSizeValue(sh2 *agent.Agent, formatTagValueID int32, statusTagValueID int32, protocolTagValueID int32) *agent.BuiltInItemValue {
	if sh2 != nil {
		return sh2.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentReceivedBatchSize,
			[]int32{0, formatTagValueID, statusTagValueID, protocolTagValueID})
	}
	return nil
}

func createPacketSizeValue(sh2 *agent.Agent, formatTagValueID int32, statusTagValueID int32, protocolTagValueID int32) *agent.BuiltInItemValue {
	if sh2 != nil {
		return sh2.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentReceivedPacketSize,
			[]int32{0, formatTagValueID, statusTagValueID, protocolTagValueID})
	}
	return nil
}

func setValueSize(i *agent.BuiltInItemValue, length int) {
	if i != nil {
		i.AddValueCounter(float64(length), 1)
	}
}

func (u *parser) StatPacketsTotal() uint64    { return u.statPacketsTotal.Load() }
func (u *parser) StatBatchesTotalOK() uint64  { return u.statBatchesTotalOK.Load() }
func (u *parser) StatBatchesTotalErr() uint64 { return u.statBatchesTotalErr.Load() }
func (u *parser) StatBytesTotal() uint64      { return u.statBytesTotal.Load() }

func (u *parser) parse(h Handler, ingestionError *error, pkt []byte, batch *tlstatshouse.AddMetricsBatchBytes, scratch *[]byte) error {
	pktLen := len(pkt)
	if u.logPacket != nil { // formatting is slow
		u.logPacket("Incoming packet %x", pkt)
	}

	// order is important. If JSON has leading whitespaces, it will not be detected/parsed
	switch {
	case pktLen == 0: // Otherwise empty packets count as protobuf
		setValueSize(u.packetSizeEmptyErr, pktLen)
	case len(pkt) >= len(metricsBatchPrefix) && string(pkt[0:len(metricsBatchPrefix)]) == metricsBatchPrefix:
		for len(pkt) > 0 {
			var err error
			was := pkt
			pkt, err = batch.ReadBoxed(pkt)
			if !u.handleMetricsBatch(h, ingestionError, batch, was, err, scratch) {
				setValueSize(u.batchSizeTLErr, len(was))
				setValueSize(u.packetSizeTLErr, pktLen)
				return err
			}
			setValueSize(u.batchSizeTLOK, len(was)-len(pkt))
		}
		setValueSize(u.packetSizeTLOK, pktLen)
	case len(pkt) >= len(jsonPacketPrefix) && string(pkt[0:len(jsonPacketPrefix)]) == jsonPacketPrefix:
		err := batch.UnmarshalJSON(pkt)
		if !u.handleMetricsBatch(h, ingestionError, batch, pkt, err, scratch) {
			setValueSize(u.batchSizeJSONErr, pktLen)
			setValueSize(u.packetSizeJSONErr, pktLen)
			return err
		}
		setValueSize(u.batchSizeJSONOK, pktLen)
		setValueSize(u.packetSizeJSONOK, pktLen)
	case len(pkt) >= len(legacyPacketPrefix) && string(pkt[0:len(legacyPacketPrefix)]) == legacyPacketPrefix:
		setValueSize(u.packetSizeLegacyErr, pktLen)
	case msgpackLooksLikeMap(pkt):
		for len(pkt) > 0 {
			var err error
			was := pkt
			pkt, err = msgpackUnmarshalStatshouseAddMetricBatch(batch, pkt)
			if !u.handleMetricsBatch(h, ingestionError, batch, was, err, scratch) {
				setValueSize(u.batchSizeMsgPackErr, len(was))
				setValueSize(u.packetSizeMsgPackErr, pktLen)
				return err
			}
			setValueSize(u.batchSizeMsgPackOK, len(was)-len(pkt))
		}
		setValueSize(u.packetSizeMsgPackOK, pktLen)
	default: // assume Protobuf, it is too flexible, many wrong packets will be accounted for here
		for len(pkt) > 0 {
			var err error
			was := pkt
			pkt, err = protobufUnmarshalStatshouseAddMetricBatch(batch, pkt)
			if !u.handleMetricsBatch(h, ingestionError, batch, pkt, err, scratch) {
				setValueSize(u.batchSizeProtobufErr, len(was))
				setValueSize(u.packetSizeProtobufErr, pktLen)
				return err
			}
			setValueSize(u.batchSizeProtobufOK, len(was)-len(pkt))
		}
		setValueSize(u.packetSizeProtobufOK, pktLen)
	}
	return nil
}

func (u *parser) handleMetricsBatch(handler Handler, ingestionError *error, b *tlstatshouse.AddMetricsBatchBytes, pkt []byte, parseErr error, scratch *[]byte) bool {
	if parseErr != nil {
		u.statBatchesTotalErr.Inc()
		if len(pkt) != 0 {
			handler.HandleParseError(pkt, parseErr)
		}
		return false
	}
	u.statBatchesTotalOK.Inc()
	for i := range b.Metrics {
		h := handler.HandleMetrics(data_model.HandlerArgs{
			MetricBytes: &b.Metrics[i],
			Scratch:     scratch,
		}) // might move out metric, if needs to
		if ingestionError != nil && *ingestionError == nil && h.IngestionStatus != 0 {
			*ingestionError = mapping.MapErrorFromHeader(b.Metrics[i], h)
		}
	}
	return true
}

func (u *parser) handleAndWaitMetrics(handler Handler, args *tlstatshouse.AddMetricsBatchBytes, scratch *[]byte) error {
	var firstError error
	_ = u.handleMetricsBatch(handler, &firstError, args, nil, nil, scratch)
	return firstError
}

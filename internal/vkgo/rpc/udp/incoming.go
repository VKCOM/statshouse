// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"fmt"
	"log"

	"github.com/VKCOM/statshouse/internal/vkgo/algo"
	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tlnetUdpPacket"
)

type IncomingMessage struct {
	data      *[]byte
	seqNo     uint32
	parts     uint32
	remaining uint32
}

// IncomingChunk seqNo is index in IncomingConnection windowData
type IncomingChunk struct {
	message     *IncomingMessage
	messageSize uint32
}

func (c IncomingChunk) received() bool {
	return c.messageSize > 0
}

// when we receive chunks without prev and next parts
// and call handler from read buffer, we set message pointer to this fake message
var fakeMessage = IncomingMessage{}

type IncomingConnection struct {
	transport *Transport
	conn      *Connection

	// we use this slice to avoid escaping the `payload` slice to heap in receiveMessageChunk
	chunkPayloadForMyFriendEscapeAnalyzer []byte

	// Every chunk can have 3 states
	// 1) has no pointer to message:                    no chunk of this message is received
	// 2) has pointer to message but messageSize == 0:  not received
	// 3) has message pointer and messageSize > 0:      received
	windowChunks         algo.TreeMap[uint32, IncomingChunk, seqNumCompT]
	ackPrefix            uint32 // seqNo of first chunk in windowChunks
	nextSeqNo            uint32 // seqNo of first chunk after windowChunks
	messagesBeginOffset  int64
	messagesTotalOffset  int64
	inMemoryWaitersQueue bool
	requestedMemorySize  int64

	windowControl int64 // seqNo of first chunk that sender should not send
}

func handleUdpMsgIfAny(t *Transport, data []byte, resendReq *tlnetUdpPacket.ResendRequest) bool {
	if len(data) >= 4 {
		var tag uint32
		_, err := basictl.NatRead(data, &tag)
		if err != nil {
			panic("Unreachable")
		}

		switch tag {
		case tlnetUdpPacket.ObsoletePid{}.TLTag():
			// We do not deserialize obsolete pid and do not compare it with our known remote pid, because
			// if we really have obsolete remote pid, we close connection in processIncomingDatagramPid
			t.stats.ObsoletePidReceived.Add(1)
			return true
		case tlnetUdpPacket.ObsoleteGeneration{}.TLTag():
			// We do not deserialize obsolete generation and do not compare it with ours, because
			// if we really have obsolete generation, we update it in processIncomingDatagramPid
			t.stats.ObsoleteGenerationReceived.Add(1)
			return true
		case tlnetUdpPacket.ObsoleteHash{}.TLTag():
			// We do not deserialize obsolete hash and do not compare it with ours, because
			// if we really have obsolete hash, we update remote pid at first and then recalculate new hash
			t.stats.ObsoleteHashReceived.Add(1)
			return true
		case tlnetUdpPacket.ResendRequest{}.TLTag():
			if resendReq == nil {
				panic("ResendRequest received as reliable message!")
			}
			_, err = resendReq.ReadBoxed(data)
			if err != nil {
				// TODO
				log.Panicf("incorrect resend request: %+v", err)
			}
			t.stats.ResendRequestReceived.Add(1)
			return true
		case 0x6e321c96: // udp wait
			return true
		}
	}
	return false
}

func (c *IncomingConnection) receiveMessageChunk(
	resendReq *tlnetUdpPacket.ResendRequest,
	seqNo uint32,
	prevParts uint32,
	nextParts uint32,
	chunkOffset int64,
	prevLength uint32,
	nextLength uint32,
	chunk IncomingChunk,
	payload []byte,
) bool {
	if seqNo == ^uint32(0) {
		c.transport.stats.UnreliableMessagesReceived.Add(1)
		if !handleUdpMsgIfAny(c.transport, payload, resendReq) {
			// We do not call c.conn.MessageHandle(&chunk.payload, false), because this will force escape analyzer to allocate chunk on heap
			c.chunkPayloadForMyFriendEscapeAnalyzer = payload
			c.conn.MessageHandle(&c.chunkPayloadForMyFriendEscapeAnalyzer, false)
			c.chunkPayloadForMyFriendEscapeAnalyzer = nil
			// we do not increment c.transport.stats.MessageHandlerCalled, because it is unreliable message
		}
		return true
	}
	if seqNo < c.ackPrefix {
		return true
	}
	if !c.ensureWindowSize(seqNo, prevParts, nextParts, chunkOffset, prevLength, nextLength, chunk, payload) {
		return false
	}

	chunkWas, _ := c.windowChunks.Get(seqNo)
	if !chunkWas.received() {
		message := chunkWas.message
		if message == nil {
			c.transport.stats.NewIncomingMessages.Add(1)
			c.transport.stats.IncomingMessagesInflight.Add(1)
			if prevParts == 0 && nextParts == 0 && !c.conn.StreamLikeIncoming {
				// optimization for small one-chunk messages: call handler right from read buffer
				// without allocation of IncomingMessage and its data slice
				c.chunkPayloadForMyFriendEscapeAnalyzer = payload
				c.conn.MessageHandle(&c.chunkPayloadForMyFriendEscapeAnalyzer, false)
				c.chunkPayloadForMyFriendEscapeAnalyzer = nil

				c.transport.stats.MessageHandlerCalled.Add(1)
				c.transport.stats.MessageReleased.Add(1)
				c.transport.releaseMemory(int64(len(payload)))
				c.transport.stats.IncomingMessagesInflight.Add(-1)
				chunk.message = &fakeMessage
				chunk.messageSize = uint32(len(payload))
				c.windowChunks.Set(seqNo, chunk)

				c.moveWindowPrefix()
				return true
			}

			if n := len(c.transport.incomingMessagesPool) - 1; n >= 0 {
				message = c.transport.incomingMessagesPool[n]
				c.transport.incomingMessagesPool = c.transport.incomingMessagesPool[:n]
			} else {
				message = &IncomingMessage{}
			}

			messageSize := prevLength + uint32(len(payload)) + nextLength
			message.data = c.transport.messageAllocator(int(messageSize))
			message.seqNo = seqNo - prevParts
			message.parts = prevParts + 1 + nextParts
			message.remaining = message.parts

			firstChunkSeqNo := max(c.ackPrefix, seqNo-prevParts)
			lastChunkSeqNo := min(c.nextSeqNo-1, seqNo+nextParts)
			for s := firstChunkSeqNo; s <= lastChunkSeqNo; s++ {
				chunkPtr := c.windowChunks.GetPtr(s)
				if chunkPtr == nil {
					c.windowChunks.Set(s, IncomingChunk{
						message:     message,
						messageSize: 0,
					})
				} else {
					chunkPtr.message = message
				}
			}
		}

		chunk.message = message
		chunk.messageSize = uint32(len(*message.data))
		c.windowChunks.Set(seqNo, chunk)

		copy((*message.data)[prevLength:], payload)
		message.remaining--
		if message.remaining == 0 {
			if !c.conn.StreamLikeIncoming {
				messageSize := int64(len(*message.data))
				if !handleUdpMsgIfAny(c.transport, *message.data, nil) {
					c.conn.MessageHandle(message.data, true)
				}
				c.transport.stats.MessageHandlerCalled.Add(1)
				c.transport.stats.MessageReleased.Add(1)
				c.transport.releaseMemory(messageSize)
				c.transport.stats.IncomingMessagesInflight.Add(-1)
				message.data = nil
			}
		}

		c.moveWindowPrefix()
	}
	return true
}

// OnAcquiredMemory called from Transport::checkMemoryWaiters when there is enough free memory
func (c *IncomingConnection) OnAcquiredMemory() {
	c.messagesTotalOffset += c.requestedMemorySize
	c.requestedMemorySize = 0
}

func (c *IncomingConnection) moveWindowPrefix() {
	for {
		if c.windowChunks.Empty() {
			return
		}
		frontEntry := c.windowChunks.Front()
		chunk := frontEntry.V
		if c.ackPrefix < frontEntry.K || !chunk.received() {
			return
		}

		message := chunk.message
		c.windowChunks.Delete(frontEntry.K)
		c.ackPrefix++

		if chunk.message != &fakeMessage && c.ackPrefix < chunk.message.seqNo+chunk.message.parts {
			if c.windowChunks.Empty() {
				// To leave pointer to message
				c.windowChunks.Set(c.nextSeqNo, IncomingChunk{message, 0})
				c.nextSeqNo++
			}
		} else {
			if c.conn.StreamLikeIncoming {
				messageSize := int64(len(*message.data))
				// `chunk` is the last chunk of message, so it's time to call handler
				if !handleUdpMsgIfAny(c.transport, *message.data, nil) {
					c.conn.MessageHandle(message.data, true)
				}
				c.transport.stats.MessageHandlerCalled.Add(1)
				c.transport.stats.MessageReleased.Add(1)
				c.transport.releaseMemory(messageSize)
				c.transport.stats.IncomingMessagesInflight.Add(-1)
				message.data = nil
			}
			messageSize := chunk.messageSize
			if message != &fakeMessage {
				c.transport.incomingMessagesPool = append(c.transport.incomingMessagesPool, message)
			}
			c.messagesBeginOffset += int64(messageSize)
		}
	}
}

func (c *IncomingConnection) ensureWindowSize(
	seqNo uint32,
	prevParts uint32,
	nextParts uint32,
	chunkOffset int64,
	prevLength uint32,
	nextLength uint32,
	chunk IncomingChunk,
	payload []byte,
) bool {
	if seqNo < c.nextSeqNo {
		return true
	}

	if int(seqNo-c.ackPrefix+1) > c.transport.maxIncomingWindowSize {
		return false
	}

	messageEndOffset := chunkOffset + int64(len(payload)) + int64(nextLength)
	needMemoryDelta := messageEndOffset - c.messagesTotalOffset
	if needMemoryDelta <= 0 {
		// enough memory
		c.extendWindow(seqNo)
		return true
	}

	if c.conn.incoming.inMemoryWaitersQueue {
		if needMemoryDelta >= c.requestedMemorySize {
			// we will better not request more memory than we wanted earlier
			// TODO window control ?????
			return false
		}
		// otherwise we will reduce our request
	}

	c.requestedMemorySize = needMemoryDelta
	if !c.transport.tryAcquireMemory(c.conn) {
		// window control just at the beginning of this chunk message
		c.windowControl = int64(seqNo - prevParts)
		return false
	}

	c.extendWindow(seqNo)
	c.messagesTotalOffset += c.requestedMemorySize

	c.requestedMemorySize = 0
	return true
}

func (c *IncomingConnection) extendWindow(seqNo uint32) {
	c.windowControl = -1
	var lastMessage *IncomingMessage
	var lastMessageChunkSeqNo uint32
	if !c.windowChunks.Empty() {
		lastMessage = c.windowChunks.Back().V.message
		if lastMessage == &fakeMessage {
			lastMessage = nil
		} else if lastMessage != nil {
			lastMessageChunkSeqNo = lastMessage.seqNo + lastMessage.parts - 1
		}
	}
	// to propagate last message pointer till necessary
	if lastMessage != nil {
		for c.nextSeqNo <= min(seqNo, lastMessageChunkSeqNo) {
			c.windowChunks.Set(c.nextSeqNo, IncomingChunk{message: lastMessage})
			c.nextSeqNo++
		}
	}
	if lastMessage == nil || lastMessageChunkSeqNo < seqNo {
		c.windowChunks.Set(seqNo, IncomingChunk{})
		c.nextSeqNo = seqNo + 1
	}
}

// ReceiveDatagram parses payload in packet bytes, updates IncomingConnection state and calls MessageHandler if possible.
// Also updates enc.PacketNum or enc.PacketFrom + enc.PacketCount, if any chunks weren't actually received at first time:
// if chunks were outside the window,
// 1) either because they were in already received prefix,
// 2) or we couldn't afford enough memory for them,
// they are removed from header info.
//
// We need this behaviour to avoid situation, when we hadn't actually received chunk,
// but goWrite() received enc header with this chunk, and sent ack to the peer, leading to protocol deadlock.
func (c *IncomingConnection) ReceiveDatagram(enc *tlnetUdpPacket.EncHeader, resendReq *tlnetUdpPacket.ResendRequest, packet []byte) error {
	if !enc.IsSetPacketNum() && !enc.IsSetPacketsFrom() {
		return nil
	}

	seqNo := enc.PacketNum
	var count uint32 = 1
	if enc.IsSetPacketsFrom() {
		seqNo = enc.PacketsFrom
	}
	if enc.IsSetPacketsCount() {
		count = enc.PacketsCount
	}

	anyReceived := false
	firstReceivedSeqNum := uint32(0)
	lastReceivedSeqNum := uint32(0)

	offset := enc.PacketOffset
	for i := 0; i < int(count); i++ {
		var partSize uint32
		var err error
		if count == 1 {
			partSize = uint32(len(packet))
		} else {
			packet, err = basictl.NatRead(packet, &partSize)
			if err != nil {
				return err
			}
		}
		if uint32(len(packet)) < partSize {
			return fmt.Errorf("message part size %d > remaining bytes %d", partSize, len(packet))
		}

		var prev, next uint32
		var prevLength, nextLength uint32
		if i == 0 {
			prev = enc.PrevParts
			prevLength = enc.PrevLength
		}

		// if last part
		if i+1 == int(count) {
			next = enc.NextParts
			nextLength = enc.NextLength
		}

		chunk := IncomingChunk{
			message: nil,
		}
		received := c.receiveMessageChunk(
			resendReq,
			seqNo,
			prev,
			next,
			offset,
			prevLength,
			nextLength,
			chunk,
			packet[:partSize],
		)
		if received {
			if !anyReceived {
				firstReceivedSeqNum = seqNo
				anyReceived = true
			}
			lastReceivedSeqNum = seqNo
		}

		seqNo++
		offset += int64(partSize)
		packet = packet[partSize:]
	}

	enc.ClearPacketNum()
	enc.ClearPacketsFrom()
	enc.ClearPacketsCount()
	if anyReceived {
		count = lastReceivedSeqNum - firstReceivedSeqNum + 1
		if count == 1 {
			enc.SetPacketNum(firstReceivedSeqNum)
		} else {
			enc.SetPacketsFrom(firstReceivedSeqNum)
			enc.SetPacketsCount(count)
		}
	}

	return nil
}

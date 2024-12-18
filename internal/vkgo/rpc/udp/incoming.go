// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"fmt"
	"log"

	"github.com/vkcom/statshouse/internal/vkgo/algo"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tlnetUdpPacket"
)

type IncomingMessage struct {
	data      *[]byte
	size      uint32 // size equal to len(*data) when IncomingMessage owns data pointer
	remaining uint32
}

// IncomingChunk seqNo is index in IncomingConnection windowData
type IncomingChunk struct {
	message     *IncomingMessage
	payload     []byte
	prevParts   uint32
	nextParts   uint32
	chunkOffset int64
	prevLength  uint32
	nextLength  uint32

	received bool
}

type IncomingConnection struct {
	transport *Transport
	conn      *Connection

	inMemoryWaitersQueue     bool
	requestedMemorySize      int64
	requestedChunk           IncomingChunk
	requestedChunkSeqNo      uint32
	requestedChunkDataReused []byte

	// TODO replace with IncomingConnectionConfig
	maxChunkSize  int
	maxWindowSize int

	firstMessageOffset int64

	// Every chunk can have 3 states
	// 1) Not received and has no pointer to message
	// 2) Not received but has pointer to message and correct prevParts and nextParts
	// 3) Received and has all correct fields
	windowChunks      algo.CircularSlice[IncomingChunk]
	ackPrefix         uint32 // seqNo of first chunk in windowChunks
	messagesTotalSize int64

	messagesPool []*IncomingMessage
	// when we receive chunks without prev and next parts
	// and call handler from read buffer, we set message pointer to this fake message
	fakeMessage IncomingMessage

	chunk IncomingChunk

	windowControl int64 // seqNo of first chunk that sender should not send
}

func handleUdpMsgIfAny(data []byte, resendReq *tlnetUdpPacket.ResendRequest) bool {
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
			return true
		case tlnetUdpPacket.ObsoleteGeneration{}.TLTag():
			// We do not deserialize obsolete generation and do not compare it with ours, because
			// if we really have obsolete generation, we update it in processIncomingDatagramPid
			return true
		case tlnetUdpPacket.ObsoleteHash{}.TLTag():
			// We do not deserialize obsolete hash and do not compare it with ours, because
			// if we really have obsolete hash, we update remote pid at first and then recalculate new hash
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
			return true
		case 0x6e321c96:
			return true
		}
	}
	return false
}

func (c *IncomingConnection) ReceiveMessageChunk(resendReq *tlnetUdpPacket.ResendRequest, seqNo uint32) bool {
	if seqNo == ^uint32(0) {
		if !handleUdpMsgIfAny(c.chunk.payload, resendReq) {
			// We do not call c.conn.MessageHandle(&chunk.payload, false), because this will force escape analyzer to allocate chunk on heap
			c.fakeMessage.data = &c.chunk.payload
			// Should we allow call handler on unreliable udp messages or restrict it?
			c.conn.MessageHandle(c.fakeMessage.data, false)
		}
		return true
	}
	if seqNo < c.ackPrefix {
		return true
	}
	if !c.ensureWindowSize(c.chunk, seqNo) {
		return false
	}

	chunkIndex := int(seqNo - c.ackPrefix)
	chunkWas := c.windowChunks.Index(chunkIndex)
	if !chunkWas.received {
		message := chunkWas.message
		if message == nil {
			if c.chunk.prevParts == 0 && c.chunk.nextParts == 0 && !c.conn.StreamLikeIncoming {
				// optimization for small one-chunk messages: call handler right from read buffer
				// without allocation of IncomingMessage and its data slice
				c.conn.MessageHandle(&c.chunk.payload, false)

				c.chunk.message = &c.fakeMessage
				c.chunk.received = true
				*c.windowChunks.IndexRef(chunkIndex) = c.chunk

				c.moveWindowPrefix()
				return true
			}

			if n := len(c.messagesPool) - 1; n >= 0 {
				message = c.messagesPool[n]
				c.messagesPool = c.messagesPool[:n]
			} else {
				message = &IncomingMessage{}
			}

			message.size = c.chunk.prevLength + uint32(len(c.chunk.payload)) + c.chunk.nextLength
			message.data = c.transport.messageAllocator(int(message.size))
			message.remaining = c.chunk.prevParts + 1 + c.chunk.nextParts

			firstChunkIndex := max(0, chunkIndex-int(c.chunk.prevParts))
			lastChunkIndex := min(c.windowChunks.Len()-1, chunkIndex+int(c.chunk.nextParts))
			for i := firstChunkIndex; i <= lastChunkIndex; i++ {
				c.windowChunks.IndexRef(i).message = message
				seqNoDelta := uint32(chunkIndex - i)
				c.windowChunks.IndexRef(i).prevParts = c.chunk.prevParts - seqNoDelta
				c.windowChunks.IndexRef(i).nextParts = c.chunk.nextParts + seqNoDelta
			}
		}

		c.chunk.message = message
		c.chunk.received = true
		*c.windowChunks.IndexRef(chunkIndex) = c.chunk

		copy((*message.data)[c.chunk.prevLength:], c.chunk.payload)
		// TODO clear chunk.payload field to reduce read buffer pointers count
		message.remaining--
		if message.remaining == 0 {
			if !c.conn.StreamLikeIncoming {
				if !handleUdpMsgIfAny(*message.data, nil) {
					c.conn.MessageHandle(message.data, true)
				}
			}
		}

		c.moveWindowPrefix()
	}
	return true
}

// OnAcquiredMemory called from Transport::checkMemoryWaiters when there is enough free memory
func (c *IncomingConnection) OnAcquiredMemory() {
	c.extendWindow(int(c.requestedChunkSeqNo - c.ackPrefix + 1))
	c.messagesTotalSize += c.requestedMemorySize
	c.chunk = c.requestedChunk
	c.ReceiveMessageChunk(nil, c.requestedChunkSeqNo)
}

func (c *IncomingConnection) moveWindowPrefix() {
	for c.windowChunks.Len() > 0 && c.windowChunks.Front().received {
		chunk := c.windowChunks.Front()
		message := chunk.message
		c.windowChunks.PopFront()
		c.ackPrefix++

		if chunk.nextParts > 0 {
			// To leave pointer to message
			if c.windowChunks.Len() == 0 {
				c.windowChunks.PushBack(IncomingChunk{})
			}
			nextChunk := c.windowChunks.IndexRef(0)
			nextChunk.message = message
			nextChunk.prevParts = chunk.prevParts + 1
			nextChunk.nextParts = chunk.nextParts - 1
		} else {
			if c.conn.StreamLikeIncoming {
				if !handleUdpMsgIfAny(*message.data, nil) {
					c.conn.MessageHandle(message.data, true)
				}
			}
			var messageSize uint32
			if message == &c.fakeMessage {
				messageSize = uint32(len(chunk.payload))
			} else {
				messageSize = message.size
				c.messagesPool = append(c.messagesPool, message)
			}
			c.firstMessageOffset += int64(messageSize)
			c.messagesTotalSize -= int64(messageSize)
			c.transport.releaseMemory(int64(messageSize))
		}
	}
}

func (c *IncomingConnection) ensureWindowSize(chunk IncomingChunk, seqNo uint32) bool {
	chunkIndex := int(seqNo - c.ackPrefix)
	if chunkIndex < c.windowChunks.Len() {
		return true
	}

	if chunkIndex >= c.maxWindowSize {
		return false
	}

	messageEndOffset := chunk.chunkOffset + int64(len(chunk.payload)) + int64(chunk.nextLength)
	needMessagesTotalSize := messageEndOffset - c.firstMessageOffset
	if needMessagesTotalSize < 0 {
		log.Panicf("incorrect chunk.chunkOffset or chunk.nextLength: new message end offset %d < first message offset %d", messageEndOffset, c.firstMessageOffset)
	}
	needMemoryDelta := needMessagesTotalSize - c.messagesTotalSize
	if needMemoryDelta == 0 {
		// no new messages
		c.extendWindow(chunkIndex + 1)
		return true
	}

	if c.inMemoryWaitersQueue {
		if needMemoryDelta >= c.requestedMemorySize {
			// we will better not request more memory than we wanted earlier
			// TODO window control ?????
			return false
		}
		// otherwise we will reduce our request
	}

	c.requestedMemorySize = needMemoryDelta
	c.requestedChunk = chunk
	c.requestedChunkSeqNo = seqNo
	c.requestedChunkDataReused = algo.ResizeSlice(c.requestedChunkDataReused, len(chunk.payload))
	copy(c.requestedChunkDataReused, chunk.payload)
	c.requestedChunk.payload = c.requestedChunkDataReused
	c.requestedChunkDataReused = c.requestedChunkDataReused[:0]

	if !c.transport.tryAcquireMemory(c) {
		// window control just at the beginning of this chunk message
		c.windowControl = int64(seqNo - chunk.prevParts)
		return false
	}

	c.extendWindow(chunkIndex + 1)
	c.messagesTotalSize += c.requestedMemorySize

	c.requestedMemorySize = 0
	c.requestedChunk = IncomingChunk{}
	c.requestedChunkSeqNo = 0
	return true
}

func (c *IncomingConnection) extendWindow(size int) {
	c.windowControl = -1
	if c.windowChunks.Cap() < size {
		c.windowChunks.Reserve(max(size, c.windowChunks.Cap()*2)) // to amortize reallocate overhead
	}
	// create all chunks before current
	for i := c.windowChunks.Len(); i < size; i++ {
		thisChunk := IncomingChunk{}
		if i > 0 {
			prevChunk := c.windowChunks.Index(i - 1)
			// if previous chunk is part of our message and has message pointer
			// then we can set pointer to existing message
			if prevChunk.message != nil && prevChunk.nextParts > 0 {
				thisChunk.message = prevChunk.message
				thisChunk.prevParts = prevChunk.prevParts + 1
				thisChunk.nextParts = prevChunk.nextParts - 1
			}
		}
		c.windowChunks.PushBack(thisChunk)
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

		c.chunk = IncomingChunk{
			message:     nil,
			payload:     packet[:partSize],
			prevParts:   prev,
			nextParts:   next,
			chunkOffset: offset,
			prevLength:  prevLength,
			nextLength:  nextLength,
			received:    false,
		}
		received := c.ReceiveMessageChunk(
			resendReq,
			seqNo,
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

func (c *IncomingConnection) haveHoles() bool {
	// first chunk in window is never received (invariant)
	// when windowChunks.Len() > 1, then last chunk in window is always received (invariant)
	return c.windowChunks.Len() > 1
}

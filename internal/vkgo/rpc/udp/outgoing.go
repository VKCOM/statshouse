// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"fmt"

	"github.com/VKCOM/statshouse/internal/vkgo/algo"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tlnetUdpPacket"
)

// TODO replace with OutgoingConnectionConfig
var MaxChunkSize = 1000 // assuming minimal MTU for UDP is 508 bytes

type OutgoingMessage struct {
	payload  *[]byte
	seqNo    uint32
	offset   int64
	parts    uint32
	refCount int
}

type OutgoingChunk struct {
	message   *OutgoingMessage
	payload   []byte // `payload == nil` means chunk is acked
	prevParts uint32
}

func (c OutgoingChunk) nextParts() uint32 {
	return c.message.parts - c.prevParts - 1
}

func (c OutgoingChunk) offset() int64 {
	return c.message.offset + int64(c.prevOffset())
}

func (c OutgoingChunk) prevOffset() uint32 {
	return uint32(cap(*c.message.payload) - cap(c.payload))
}

func (c OutgoingChunk) nextOffset() uint32 {
	return uint32(len(*c.message.payload)-len(c.payload)) - c.prevOffset()
}

func (c OutgoingChunk) acked() bool {
	return c.payload == nil
}

type OutgoingConnection struct {
	window             algo.TreeMap[uint32, OutgoingChunk, seqNumCompT]
	timeoutedSeqNum    uint32
	nonTimeoutedSeqNum uint32
	notSendedSeqNum    uint32
	chunkToSendSeqNum  uint32 // points to some chunk in un-acked suffix

	windowControlSeqNum int64
	messageQueue        algo.CircularSlice[*OutgoingMessage]
	ackSeqNoPrefix      uint32 // seqNo of first chunk in window
	nextSeqNo           uint32 // seqNo of first chunk after window
	totalMessagesOffset int64

	resendRanges    tlnetUdpPacket.ResendRequest // outgoing seqNum ranges, requested to resend
	resendIndex     int
	rangeInnerIndex int // != 0 when we didn't send whole range (when part of range was already acked)
}

func (o *OutgoingConnection) unrefMessage(t *Transport, s *OutgoingMessage) {
	s.refCount--
	if s.refCount < 0 {
		panic("message RefCount < 0")
	}
	if s.refCount == 0 {
		t.outgoingMessagesPool = append(t.outgoingMessagesPool, s)
		t.stats.OutgoingMessagesInflight.Add(-1)

		*s.payload = (*s.payload)[:0]
		t.messageDeallocator(s.payload)
		s.payload = nil
	}
}

func (o *OutgoingConnection) checkAck(seqNum uint32) (ok bool, err error) {
	index, length := seqNum-o.ackSeqNoPrefix, o.nextSeqNo-o.ackSeqNoPrefix
	if index >= length {
		if index < (1<<31)+(length>>1) {
			return false, fmt.Errorf("ack from future: %d", seqNum)
		} else {
			return false, nil
		}
	}
	return true, nil
}

func (o *OutgoingConnection) ackFrontChunk(t *Transport) {
	if o.window.Empty() {
		panic("empty window")
	}
	frontEntry := o.window.GetPtr(o.ackSeqNoPrefix)
	frontEntry.payload = nil
	chunk := *frontEntry
	for {
		message := chunk.message
		o.window.Delete(o.ackSeqNoPrefix)
		o.unrefMessage(t, message)
		o.ackSeqNoPrefix++

		if o.window.Empty() {
			return
		}
		chunk = o.window.Front().V
		if !chunk.acked() {
			return
		}
	}
}

func (o *OutgoingConnection) updatedSeqNums(ackSeqNum uint32) {
	if ackSeqNum >= o.timeoutedSeqNum {
		o.timeoutedSeqNum = ackSeqNum + 1
		if ackSeqNum >= o.nonTimeoutedSeqNum {
			o.nonTimeoutedSeqNum = ackSeqNum + 1
			if ackSeqNum >= o.notSendedSeqNum {
				o.notSendedSeqNum = ackSeqNum + 1
			}
		}
		if ackSeqNum >= o.chunkToSendSeqNum {
			o.chunkToSendSeqNum = ackSeqNum + 1
			if o.nonTimeoutedSeqNum <= o.chunkToSendSeqNum && o.chunkToSendSeqNum < o.notSendedSeqNum {
				o.chunkToSendSeqNum = o.notSendedSeqNum
			}
		}
	}
}

// TODO: clarify semantic
func (o *OutgoingConnection) AckPrefix(t *Transport, prefixSeqNum uint32) error {
	if prefixSeqNum == 0 {
		return nil
	}
	if ok, err := o.checkAck(prefixSeqNum - 1); !ok {
		return err
	}
	o.updatedSeqNums(prefixSeqNum - 1)
	oldAckSeqNoPrefix := o.ackSeqNoPrefix
	targetIndex := prefixSeqNum - oldAckSeqNoPrefix // TODO: clarify semantic
	for o.ackSeqNoPrefix-oldAckSeqNoPrefix < targetIndex {
		o.ackFrontChunk(t)
	}
	return nil
}

func (o *OutgoingConnection) AckChunk(t *Transport, seqNum uint32) error {
	if ok, err := o.checkAck(seqNum); !ok {
		return err
	}
	o.updatedSeqNums(seqNum)
	if seqNum == o.ackSeqNoPrefix {
		o.ackFrontChunk(t)
		return nil
	}
	o.window.GetPtr(seqNum).payload = nil
	return nil
}

func (o *OutgoingConnection) sliceNextMessage(t *Transport, datagramOffset int) {
	message := o.messageQueue.PopFront()
	if message == nil {
		panic("message queue is empty")
	}
	message.seqNo = o.nextSeqNo

	chunksCount := 0
	for offset := 0; offset < len(*message.payload); {
		if datagramOffset+4 >= t.maxOutgoingPayloadSize {
			datagramOffset = 0
			continue
		}
		chunkSize := min(t.maxOutgoingPayloadSize-datagramOffset-4, len(*message.payload)-offset)

		chunk := OutgoingChunk{
			message:   message,
			payload:   (*message.payload)[offset : offset+chunkSize],
			prevParts: uint32(chunksCount),
		}
		o.window.Set(o.nextSeqNo, chunk)
		o.nextSeqNo++

		offset += chunkSize
		datagramOffset += 4 + chunkSize
		if 4+datagramOffset >= t.maxOutgoingPayloadSize {
			datagramOffset = 0
		}
		chunksCount++
	}

	message.parts = uint32(chunksCount)
	message.refCount += chunksCount - 1
}

func (o *OutgoingConnection) SetFlowControl(seqNo uint32) {
	o.windowControlSeqNum = int64(seqNo)
}

func (o *OutgoingConnection) ResetFlowControl() {
	o.windowControlSeqNum = -1
}

func (o *OutgoingConnection) haveReliableChunksToSendNow(t *Transport) bool {
	index := int(o.chunkToSendSeqNum - o.ackSeqNoPrefix)
	return o.resendIndex < len(o.resendRanges.Ranges) ||
		(o.chunkToSendSeqNum < o.nextSeqNo || o.messageQueue.Len() != 0) && index < t.maxOutgoingWindowSize
}

func (o *OutgoingConnection) haveChunksToSendNow(t *Transport) bool {
	return o.haveReliableChunksToSendNow(t)
}

/*
GetChunksToSend
TODO draw data stream with acked/un-acked, timeout-ed, resended, ... chunks to explain algorithm how to choose chunks to send
*/
func (o *OutgoingConnection) GetChunksToSend(t *Transport, chunks [][]byte) (_ [][]byte, firstSeqNum uint32, singleMessage bool) {
	payloadSize := 0
	firstSeqNum = 0
	singleMessage = true
	var prevMessage *OutgoingMessage = nil

	for o.resendIndex < len(o.resendRanges.Ranges) {
		r := o.resendRanges.Ranges[o.resendIndex]
		if r.PacketNumTo < o.ackSeqNoPrefix {
			// already in ack prefix
			o.resendIndex++
			o.rangeInnerIndex = 0
			continue
		}

		for seqNum := r.PacketNumFrom + uint32(o.rangeInnerIndex); seqNum <= r.PacketNumTo; seqNum++ {
			if seqNum < o.ackSeqNoPrefix {
				o.rangeInnerIndex++
				continue
			}
			chunk := o.window.GetPtr(seqNum)
			if chunk.acked() {
				if prevMessage != nil {
					break
				}
				o.rangeInnerIndex++
				continue
			}

			// TODO remove copy paste code
			if payloadSize+4+len(chunk.payload) > t.maxOutgoingPayloadSize {
				break
			}

			if prevMessage == nil {
				firstSeqNum = seqNum
			} else if prevMessage != chunk.message {
				singleMessage = false
			} else {
				// we don't allow several chunks of one message in a datagram
				// prevMessage == chunk.message
				t.stats.HoleSeqNumsSent.Add(int64(len(chunks)))
				return chunks, firstSeqNum, singleMessage
			}
			prevMessage = chunk.message

			chunks = append(chunks, chunk.payload)
			payloadSize += 4 + len(chunk.payload)
			o.rangeInnerIndex++
		}

		if prevMessage != nil {
			t.stats.HoleSeqNumsSent.Add(int64(len(chunks)))
			return chunks, firstSeqNum, singleMessage
		}

		o.resendIndex++
		o.rangeInnerIndex = 0
	}

	for {
		seqNum := o.chunkToSendSeqNum
		if seqNum < o.nextSeqNo {
			if int(seqNum-o.ackSeqNoPrefix) == t.maxOutgoingWindowSize {
				break
			}
			chunk := o.window.GetPtr(seqNum)
			// we do not check chunk.acked, because all chunks from seq num `o.chunkToSendSeqNum` are not acked (invariant)

			// TODO remove copy paste code
			if payloadSize+4+len(chunk.payload) > t.maxOutgoingPayloadSize {
				break
			}

			if prevMessage == nil {
				firstSeqNum = seqNum
			} else if prevMessage != chunk.message {
				singleMessage = false
			} else {
				// prevMessage == chunk.message
				// we don't allow several chunks of one message in a datagram
				break
			}
			prevMessage = chunk.message
			chunks = append(chunks, chunk.payload)
			payloadSize += 4 + len(chunk.payload)

			o.chunkToSendSeqNum++
			if o.nonTimeoutedSeqNum <= o.chunkToSendSeqNum && o.chunkToSendSeqNum < o.notSendedSeqNum {
				// seqNum in interval [nonTimeouted ... notSended), chunks from which we must not send before next timeout
				o.chunkToSendSeqNum = o.notSendedSeqNum
				if prevMessage != nil {
					// have already taken some chunks
					break
				}
				// haven't taken any chunk yet
			}
			continue
		}
		if o.messageQueue.Len() != 0 {
			o.sliceNextMessage(t, payloadSize)
			continue
		}
		break
	}

	return chunks, firstSeqNum, singleMessage
}

func (o *OutgoingConnection) OnResendTimeout() {
	// TODO document this interesting code
	if o.nonTimeoutedSeqNum > o.timeoutedSeqNum {
		o.nonTimeoutedSeqNum = o.timeoutedSeqNum
	} else {
		o.nonTimeoutedSeqNum = o.notSendedSeqNum
	}
	o.notSendedSeqNum = o.chunkToSendSeqNum
	if o.timeoutedSeqNum < o.nonTimeoutedSeqNum {
		o.chunkToSendSeqNum = o.timeoutedSeqNum
	}
}

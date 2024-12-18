// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"fmt"

	"github.com/vkcom/statshouse/internal/vkgo/algo"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tlnetUdpPacket"
)

// TODO replace with OutgoingConnectionConfig
var MaxChunkSize = 348 // assuming minimal MTU for UDP is 508 bytes

type OutgoingMessage struct {
	payload  *[]byte
	seqNo    uint32
	offset   int64
	refCount int
}

type OutgoingChunk struct {
	message   *OutgoingMessage
	payload   []byte
	prevParts uint32
	nextParts uint32

	acked bool
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

type OutgoingConnection struct {
	transport *Transport
	conn      *Connection

	Window             algo.CircularSlice[OutgoingChunk]
	timeoutedSeqNum    uint32
	nonTimeoutedSeqNum uint32
	notSendedSeqNum    uint32
	chunkToSendSeqNum  uint32 // points to some chunk in un-acked suffix

	MaxWindowSize          int
	WindowControlSeqNum    int64
	MaxPayloadSize         int
	MessageQueue           algo.CircularSlice[*OutgoingMessage]
	AckSeqNoPrefix         uint32
	LastAckedMessageOffset int64
	TotalMessagesSize      int64

	resendRanges    tlnetUdpPacket.ResendRequest // outgoing seqNum ranges, requested to resend
	resendIndex     int
	rangeInnerIndex int // != 0 when we didn't send whole range (when part of range was already acked)
}

func (o *OutgoingConnection) unrefMessage(s *OutgoingMessage) {
	s.refCount--
	if s.refCount < 0 {
		panic("message RefCount < 0")
	}
	if s.refCount == 0 {
		o.LastAckedMessageOffset += int64(len(*s.payload))
		o.TotalMessagesSize -= int64(len(*s.payload))
		o.transport.messagesPool = append(o.transport.messagesPool, s)

		*s.payload = (*s.payload)[:0]
		o.transport.messageDeallocator(s.payload)
		s.payload = nil
	}
}

func (o *OutgoingConnection) checkAck(seqNum uint32) (ok bool, err error) {
	index, length := seqNum-o.AckSeqNoPrefix, uint32(o.Window.Len())
	if index >= length {
		if index < (1<<31)+(length>>1) {
			return false, fmt.Errorf("ack from future: %d", seqNum)
		} else {
			return false, nil
		}
	}
	return true, nil
}

func (o *OutgoingConnection) ackFrontChunk() {
	if o.Window.Len() == 0 {
		panic("empty window")
	}
	chunk := o.Window.IndexRef(0)
	chunk.acked = true
	for o.Window.Len() > 0 && o.Window.Front().acked {
		message := o.Window.Front().message
		o.Window.PopFront()
		o.unrefMessage(message)
		o.AckSeqNoPrefix++
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
func (o *OutgoingConnection) AckPrefix(prefixSeqNum uint32) error {
	if prefixSeqNum == 0 {
		return nil
	}
	if ok, err := o.checkAck(prefixSeqNum - 1); !ok {
		return err
	}
	o.updatedSeqNums(prefixSeqNum - 1)
	oldAckSeqNoPrefix := o.AckSeqNoPrefix
	targetIndex := prefixSeqNum - oldAckSeqNoPrefix // TODO: clarify semantic
	for o.AckSeqNoPrefix-oldAckSeqNoPrefix < targetIndex {
		o.ackFrontChunk()
	}
	return nil
}

func (o *OutgoingConnection) AckChunk(seqNum uint32) error {
	if ok, err := o.checkAck(seqNum); !ok {
		return err
	}
	o.updatedSeqNums(seqNum)
	index := int(seqNum - o.AckSeqNoPrefix)
	if index == 0 {
		o.ackFrontChunk()
		return nil
	}
	o.Window.IndexRef(index).acked = true
	return nil
}

func (o *OutgoingConnection) sliceNextMessage(datagramOffset int) {
	message := o.MessageQueue.PopFront()
	if message == nil {
		panic("message queue is empty")
	}
	message.seqNo = o.AckSeqNoPrefix + uint32(o.Window.Len())

	chunksCount := 0
	for offset := 0; offset < len(*message.payload); {
		if datagramOffset+4 >= o.MaxPayloadSize {
			datagramOffset = 0
			continue
		}
		chunkSize := min(MaxChunkSize, min(o.MaxPayloadSize-datagramOffset-4, len(*message.payload)-offset))

		chunk := OutgoingChunk{
			message:   message,
			payload:   (*message.payload)[offset : offset+chunkSize],
			prevParts: uint32(chunksCount),
			acked:     false,
		}
		o.Window.PushBack(chunk)

		offset += chunkSize
		datagramOffset += 4 + chunkSize
		if 4+datagramOffset >= o.MaxPayloadSize {
			datagramOffset = 0
		}
		chunksCount++
	}

	j := 0
	for i := o.Window.Len() - 1; j < chunksCount; i-- {
		o.Window.IndexRef(i).nextParts = uint32(j)
		j++
	}

	message.refCount += chunksCount - 1
}

func (o *OutgoingConnection) SetFlowControl(seqNo uint32) {
	o.WindowControlSeqNum = int64(seqNo)
}

func (o *OutgoingConnection) ResetFlowControl() {
	o.WindowControlSeqNum = -1
}

func (o *OutgoingConnection) haveChunksToSendNow() bool {
	index := int(o.chunkToSendSeqNum - o.AckSeqNoPrefix)
	return o.resendIndex < len(o.resendRanges.Ranges) ||
		(index < o.Window.Len() || o.MessageQueue.Len() != 0) && index < o.MaxWindowSize
}

/*
GetChunksToSend
TODO draw data stream with acked/un-acked, timeout-ed, resended, ... chunks to explain algorithm how to choose chunks to send
*/
func (o *OutgoingConnection) GetChunksToSend(chunks [][]byte) (_ [][]byte, firstSeqNum uint32, singleMessage bool) {
	if o.MaxPayloadSize < MaxChunkSize {
		panic("maximal payload size is less than maximal chunk size")
	}

	payloadSize := 0
	firstSeqNum = 0
	singleMessage = true
	var prevMessage *OutgoingMessage = nil

	for o.resendIndex < len(o.resendRanges.Ranges) {
		r := o.resendRanges.Ranges[o.resendIndex]
		if r.PacketNumTo < o.AckSeqNoPrefix {
			// already in ack prefix
			o.resendIndex++
			o.rangeInnerIndex = 0
			continue
		}

		for seqNum := r.PacketNumFrom + uint32(o.rangeInnerIndex); seqNum <= r.PacketNumTo; seqNum++ {
			if seqNum < o.AckSeqNoPrefix {
				o.rangeInnerIndex++
				continue
			}
			index := int(seqNum - o.AckSeqNoPrefix)
			chunk := o.Window.IndexRef(index)
			if chunk.acked {
				if prevMessage != nil {
					break
				}
				o.rangeInnerIndex++
				continue
			}

			// TODO remove copy paste code
			if payloadSize+4+len(chunk.payload) > o.MaxPayloadSize {
				break
			}

			if prevMessage == nil {
				firstSeqNum = seqNum
			} else if prevMessage != chunk.message {
				singleMessage = false
			} else {
				// we don't allow several chunks of one message in a datagram
				// prevMessage == chunk.message
				return chunks, firstSeqNum, singleMessage
			}
			prevMessage = chunk.message

			chunks = append(chunks, chunk.payload)
			payloadSize += 4 + len(chunk.payload)
			o.rangeInnerIndex++
		}

		if prevMessage != nil {
			return chunks, firstSeqNum, singleMessage
		}

		o.resendIndex++
		o.rangeInnerIndex = 0
	}

	for {
		seqNum := o.chunkToSendSeqNum
		index := int(seqNum - o.AckSeqNoPrefix)
		if index < o.Window.Len() {
			if index == o.MaxWindowSize {
				break
			}
			chunk := o.Window.IndexRef(index)

			// TODO remove copy paste code
			if payloadSize+4+len(chunk.payload) > o.MaxPayloadSize {
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
		if o.MessageQueue.Len() != 0 {
			o.sliceNextMessage(payloadSize)
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

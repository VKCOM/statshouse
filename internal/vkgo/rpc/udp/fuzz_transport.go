// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"fmt"
	"log"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tlnetUdpPacket"
)

/*
UDP Transport Fuzzing

8 agents - every is a single Transport

Events:
  1) new message
  2) one goWrite()'s iteration - all events handling and one datagram send (if any)
  3) one goRead()'s iteration - datagram receive, handling and enc header push to Transport::newHdrRcvs
  4) enc header adding to goWrite()'s queue
  5) [Resend / AckSend / ResendRequest] timer expiration
  6) datagram duplication
  7) datagram loss
*/

// transports must be degree of 2 !!!
const transportsDegree = 4
const transports = 1 << transportsDegree

const MaxFuzzChunkSize = 1 << 5
const MaxFuzzPayloadSize = MaxFuzzChunkSize
const MaxFuzzMessageSize = (1 << 8) - 1
const MaxFuzzTransportMemory = 2 * MaxFuzzMessageSize
const testStreamLikeIncoming = true

type RandomMessage struct {
	src, dst int
	message  string
}

type TestDatagram struct {
	addr     netip.AddrPort
	datagram []byte
}

type FuzzTransportContext struct {
	ts [transports]*Transport

	network [transports][]TestDatagram     // incoming datagrams
	encHdrs [transports][]ConnectionHeader // enc headers sent by goRead()

	sentMessages     map[RandomMessage]int
	receivedMessages map[RandomMessage]int
}

func receiveMessageHandler(fctx *FuzzTransportContext, srcId, dstId int) MessageHandler {
	return func(message *[]byte, canSave bool) {
		if !canSave {
			m := make([]byte, len(*message))
			copy(m, *message)
			message = &m
		}

		fctx.receivedMessages[RandomMessage{
			src:     srcId,
			dst:     dstId,
			message: string(*message),
		}] += 1
	}
}

func FuzzTransport(fuzz []byte) int {
	MaxChunkSize = MaxFuzzChunkSize

	fctx := &FuzzTransportContext{
		sentMessages:     make(map[RandomMessage]int),
		receivedMessages: make(map[RandomMessage]int),
	}

	for tId := 0; tId < transports; tId++ {
		udpAddr, err := net.ResolveUDPAddr("udp", transportIdToAddress(tId))
		if err != nil {
			panic(err)
		}
		conn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			panic(err)
		}
		tIdCopy := tId
		fctx.ts[tId], err = NewTransport(
			MaxFuzzTransportMemory,
			[]string{
				"abacaba",
			},
			conn,
			uint32(time.Now().Unix()),
			func(conn *Connection) {
				conn.MessageHandle = receiveMessageHandler(fctx, addressToTransportId(conn.remoteAddr.String()), tIdCopy)
				conn.StreamLikeIncoming = testStreamLikeIncoming
			},
			func(_ *Connection) {},
			nil,
			nil,
			0,
		)
		if err != nil {
			panic(err)
		}
		//goland:noinspection GoDeferInLoop
		defer func() {
			err := fctx.ts[tIdCopy].Close()
			if err != nil {
				panic(err)
			}
		}()
	}

	// for unique messages
	// after overflow, messages have small possibility to repeat, it is good enough for us
	var nonce byte = 0
	for i := 0; i+2 < len(fuzz); {
		switch fuzz[i] {
		case 'n': // new message

			transportId := fuzzVerbToTransportId(fuzz[i+1])
			dstId := fuzzVerbToDstId(fuzz[i+1])
			if dstId <= transportId {
				// Out protocol currently doesn't support 2 active connection sides
				return -1
			}
			messageSize := fuzzVerbToMessageSize(fuzz[i+2])
			if messageSize == 0 {
				// Out protocol doesn't support empty messages
				return -1
			}

			doNewMessage(fctx, transportId, dstId, messageSize, &nonce)

			i += 3

		case 'w': // goWrite step

			transportId := fuzzVerbToTransportId(fuzz[i+1])

			doGoWriteStep(fctx, transportId)

			i += 2

		case 'r': // goRead step

			transportId := fuzzVerbToTransportId(fuzz[i+1])
			dgrmId := int(fuzz[i+2]) // make dgrmId 2-byte ?

			doGoReadStep(fctx, transportId, dgrmId)

			i += 3

		case 'e': // enc header adding to goWrite() queue

			transportId := fuzzVerbToTransportId(fuzz[i+1])

			doEncHdrRcv(fctx, transportId)

			i += 2

		case 't': // timer expiration

			transportId := fuzzVerbToTransportId(fuzz[i+1])
			timerId := fuzzVerbToDstId(fuzz[i+1])

			if timerId == 0 {
				// resend timer
				doResendTimerBurn(fctx, transportId)
			} else if timerId == 1 {
				// ack timer
				doAckTimerBurn(fctx, transportId)
			} else if timerId == 2 {
				// resend request timer
				doResendRequestTimerBurn(fctx, transportId)
			} else {
				return -1
			}

			i += 2

		case 'd': // datagram duplication

			transportId := fuzzVerbToTransportId(fuzz[i+1])
			dgrmId := int(fuzz[i+2]) // TODO make dgrmId 2-byte ???

			// copy this datagram to the end of transport network
			l := len(fctx.network[transportId])
			if l > 0 {
				datagram := fctx.network[transportId][dgrmId%l]
				fctx.network[transportId] = append(fctx.network[transportId], datagram)
			}

			checkInvariants(fctx)

			i += 3

		case 'l': // datagram loss

			transportId := fuzzVerbToTransportId(fuzz[i+1])
			dgrmId := int(fuzz[i+2]) // TODO make dgrmId 2-byte ???

			// replace this datagram by the last one in transport network
			l := len(fctx.network[transportId])
			if l > 0 {
				fctx.network[transportId][dgrmId%l] = fctx.network[transportId][l-1]
				fctx.network[transportId] = fctx.network[transportId][:l-1]
			}

			checkInvariants(fctx)

			i += 3

		default:
			return -1
		}
	}

	// at every step we will check this status in every connection
	// at least one connection must increase it
	type ConnProgressStatus struct {
		// incoming part
		receivedPrefix   uint32
		receivedInWindow int

		// outgoing part
		AckedPrefix  uint32
		AcksInWindow int
	}
	var progressStatuses [transports][transports]ConnProgressStatus

	// Now we should "repair" network, let protocol run its state machines to have progress and finally receive all messages
	step := 0
	for ; ; step++ {
		//log.Printf("step %d", step)

		// TODO with window control enabled we must pass datagram with last actual windowControl before resend timers,
		// otherwise OutgoingConnections can be blocked because of an old windowControl value.

		// fire resend request timers
		for tId, t := range fctx.ts {
			for t.resendRequestTimers.Len() > 0 {
				doResendRequestTimerBurn(fctx, tId)
			}
		}

		// send datagrams with resend request
		for tId := range fctx.ts {
			doGoWriteStep(fctx, tId)
		}

		// receive datagrams with resend request
		for tId := range fctx.ts {
			for len(fctx.network[tId]) > 0 {
				doGoReadStep(fctx, tId, 0)
			}
		}

		// receive resend requests
		for tId := range fctx.ts {
			for len(fctx.encHdrs[tId]) > 0 {
				doEncHdrRcv(fctx, tId)
			}
		}

		// sometimes to resend chunk we need 2 timer burns...
		for i := 0; i < 2; i++ {
			// fire resend timers
			for tId, t := range fctx.ts {
				for t.resendTimers.Len() > 0 {
					doResendTimerBurn(fctx, tId)
				}
			}

			// send datagram with payload
			for tId := range fctx.ts {
				doGoWriteStep(fctx, tId)
			}
		}

		// receive datagrams with payload
		for tId := range fctx.ts {
			for len(fctx.network[tId]) > 0 {
				doGoReadStep(fctx, tId, 0)
			}
		}

		// receive enc headers with seq nums
		for tId := range fctx.ts {
			for len(fctx.encHdrs[tId]) > 0 {
				doEncHdrRcv(fctx, tId)
			}
		}

		// fire ack timers
		for tId, t := range fctx.ts {
			for t.ackTimers.Len() > 0 {
				doAckTimerBurn(fctx, tId)
			}
		}

		// send datagram with acks
		for tId := range fctx.ts {
			doGoWriteStep(fctx, tId)
		}

		// receive datagrams with acks
		for tId := range fctx.ts {
			for len(fctx.network[tId]) > 0 {
				doGoReadStep(fctx, tId, 0)
			}
		}

		// receive enc headers with ack nums
		for tId := range fctx.ts {
			for len(fctx.encHdrs[tId]) > 0 {
				doEncHdrRcv(fctx, tId)
			}
		}

		// handle enc headers with ack nums
		for tId := range fctx.ts {
			doGoWriteStep(fctx, tId)
		}

		var connHaveDataToSend *Connection = nil
		newReceivedOrAckedChunks := false
		for tId, t := range fctx.ts {
			for _, conn := range t.handshakeByPid {

				srcId := portToTransportId(int(conn.remoteAddr.Port()))

				receivedChunks := 0
				for i := 0; i < conn.incoming.windowChunks.Len(); i++ {
					if conn.incoming.windowChunks.Index(i).received {
						receivedChunks++
					}
				}
				ackedChunks := 0
				for i := 0; i < conn.outgoing.Window.Len(); i++ {
					if conn.outgoing.Window.Index(i).acked {
						ackedChunks++
					}
				}

				// check incoming progress
				statusWas := progressStatuses[srcId][tId]
				if statusWas.receivedPrefix > conn.incoming.ackPrefix {
					panic("connection received prefix decreased")
				} else if statusWas.receivedPrefix == conn.incoming.ackPrefix {
					if statusWas.receivedInWindow > receivedChunks {
						panic("connection received chunks in window decreased")
					}
					if statusWas.receivedInWindow < receivedChunks {
						newReceivedOrAckedChunks = true
					}
				} else {
					newReceivedOrAckedChunks = true
				}

				// check outgoing progress
				if statusWas.AckedPrefix > conn.outgoing.AckSeqNoPrefix {
					panic("connection acked prefix decreased")
				} else if statusWas.AckedPrefix == conn.outgoing.AckSeqNoPrefix {
					if statusWas.AcksInWindow > ackedChunks {
						panic("connection acked chunks in window decreased")
					}
					if statusWas.AcksInWindow < ackedChunks {
						newReceivedOrAckedChunks = true
					}
				} else {
					newReceivedOrAckedChunks = true
				}

				progressStatuses[srcId][tId] = ConnProgressStatus{
					receivedPrefix:   conn.incoming.ackPrefix,
					receivedInWindow: receivedChunks,

					AckedPrefix:  conn.outgoing.AckSeqNoPrefix,
					AcksInWindow: ackedChunks,
				}

				if conn.outgoing.haveChunksToSendNow() || conn.inResendQueue || conn.inAckQueue {
					connHaveDataToSend = conn
				}
			}
		}

		if connHaveDataToSend == nil || len(fctx.receivedMessages) == len(fctx.sentMessages) {
			break
		}

		if !newReceivedOrAckedChunks {
			log.Println("Last progress state:")
			for tId, t := range fctx.ts {
				for _, conn := range t.handshakeByPid {
					srcId := portToTransportId(int(conn.remoteAddr.Port()))
					log.Printf("progress %d -> %d: %+v", srcId, tId, progressStatuses[tId][srcId])
				}
			}
			log.Printf(
				"Connection %d -> %d have data (chunks or acks) to send",
				portToTransportId(int(connHaveDataToSend.incoming.transport.localPid.PortPid&0xffff)),
				portToTransportId(int(connHaveDataToSend.remoteAddr.Port())),
			)
			panic("But no new received or acked chunks in any connection")
		}
	}

	for m := range fctx.sentMessages {
		if _, exists := fctx.receivedMessages[m]; !exists {
			log.Panicf(
				"message from %s to %s sent but not received",
				transportIdToAddress(m.src),
				transportIdToAddress(m.dst),
			)
		}
	}

	return 1
}

func transportIdToAddress(tId int) string {
	return strings.Replace(fmt.Sprintf("127.0.0.1:228%2d", tId), " ", "0", 1)
}

func portToTransportId(port int) int {
	return port % 100
}

func checkInvariants(fctx *FuzzTransportContext) {
	for _, t := range fctx.ts {
		t.checkInvariants()
		for _, conn := range t.handshakeByPid {
			conn.checkInvariants()
		}
	}
}

func (t *Transport) checkInvariants() {
	if t.acquiredMemory > t.incomingMessagesMemoryLimit {
		panic("Transport acquiredMemory > incomingMessagesMemoryLimit")
	}
	if t.memoryWaiters.Len() > 0 {
		if t.acquiredMemory+t.memoryWaiters.Front().requestedMemorySize <= t.incomingMessagesMemoryLimit {
			panic("Transport can acquire memory for the first IncomingConnection in Transport queue")
		}
	}
	conns := make(map[*IncomingConnection]int)
	for i := 0; i < t.memoryWaiters.Len(); i++ {
		conn := t.memoryWaiters.Index(i)
		if j, exists := conns[conn]; exists {
			panic(fmt.Sprintf("IncomingConnection attends twice in Transport queue (indices %d and %d)", j, i))
		}
		if conn.transport != t {
			panic(fmt.Sprintf("IncomingConnection in Transport queue (%d index) has different transport pointer", i))
		}
		if !conn.inMemoryWaitersQueue {
			panic(fmt.Sprintf("IncomingConnection is in Transport queue (%d index), but has no flag inMemoryWaitersQueue", i))
		}
	}
}

func (c *Connection) checkInvariants() {
	c.incoming.checkInvariants()
	c.outgoing.checkInvariants()
	c.acks.checkInvariantsFuzz()

	// all acked seqNums in c.acks must be in c.incoming
	if c.acks.ackPrefix > c.incoming.ackPrefix {
		log.Panic("AcksToSend::ackPrefix > IncomingConnection::ackPrefix")
	}
	r := c.acks.firstRange
	for r != nil {
		for ackNum := r.ackFrom; ackNum <= r.ackTo; ackNum++ {
			if ackNum < c.incoming.ackPrefix {
				continue
			}

			index := int(ackNum - c.incoming.ackPrefix)
			chunks := c.incoming.windowChunks
			if index < chunks.Len() && chunks.Index(index).received {
				continue
			}
			log.Panicf(
				"AcksToSend has acked range [%d...%d],"+
					" but seqNum %d isn't received in IncomingConnection",
				r.ackFrom,
				r.ackTo,
				ackNum,
			)
		}
		r = r.next
	}
}

func (o *OutgoingConnection) checkInvariants() {
	// TODO implement most part of validation
	if o.timeoutedSeqNum > o.nonTimeoutedSeqNum {
		panic("o.timeoutedSeqNum > o.nonTimeoutedSeqNum")
	}
	if o.nonTimeoutedSeqNum > o.notSendedSeqNum {
		panic("o.nonTimeoutedSeqNum > o.notSendedSeqNum")
	}
	if o.nonTimeoutedSeqNum <= o.chunkToSendSeqNum && o.chunkToSendSeqNum < o.notSendedSeqNum {
		panic("o.nonTimeoutedSeqNum <= o.chunkToSendSeqNum && o.chunkToSendSeqNum < o.notSendedSeqNum")
	}
}

func (c *IncomingConnection) checkInvariants() {
	if c.windowChunks.Len() > 0 && c.windowChunks.Front().received {
		panic("IncomingConnection first chunk in window is received")
	}
	if c.windowChunks.Len() > 1 && !c.windowChunks.Index(c.windowChunks.Len()-1).received {
		panic("IncomingConnection last chunk in window (with length > 1) is not received")
	}

	lastMessageOffset := c.firstMessageOffset

	for i := 0; i < c.windowChunks.Len(); i++ {
		chunk := c.windowChunks.Index(i)
		if chunk.message != nil {
			if chunk.received {
				lastMessageOffset = max(chunk.chunkOffset+int64(len(chunk.payload))+int64(chunk.nextLength), lastMessageOffset)
			} else {
				// This code is for the case when we don't have any received chunk of this message
				// and thus don't know chunk.chunkOffset and chunk.nextLength
				if chunk.prevParts >= uint32(i) {
					// this message is first in window
					lastMessageOffset = c.firstMessageOffset + int64(len(*chunk.message.data))
				}
			}
		}
	}
	if lastMessageOffset-c.firstMessageOffset != c.messagesTotalSize {
		panic("realTotalMessagesSize != c.messagesTotalSize")
	}
}

func (a *AcksToSend) checkInvariantsFuzz() {
	a.checkInvariantsCommon(
		func(err string) { panic(err) },
	)
}

// transport writeMu must be unlocked
func doNewMessage(fctx *FuzzTransportContext, transportId int, dstId int, messageSize int, nonce *byte) {
	defer checkInvariants(fctx)

	message := make([]byte, messageSize)
	if messageSize > 0 {
		message[0] = *nonce
		*nonce++
	}

	conn, err := fctx.ts[transportId].ConnectTo(
		netip.MustParseAddrPort(fctx.ts[dstId].socket.LocalAddr().(*net.UDPAddr).String()),
		// note: for incoming message handler source is dstId and destination is our transportId
		receiveMessageHandler(fctx, dstId, transportId),
		testStreamLikeIncoming,
		nil,
	)
	if err != nil {
		panic(err)
	}
	err = conn.SendMessage(&message)
	if err != nil {
		panic(err)
	}

	fctx.sentMessages[RandomMessage{
		src:     transportId,
		dst:     dstId,
		message: string(message),
	}] += 1
}

// transport writeMu must be locked
func doGoWriteStep(fctx *FuzzTransportContext, transportId int) {
	defer checkInvariants(fctx)

	fctx.ts[transportId].writeMu.Lock()
	datagram, conn, newResendTimer, _ := fctx.ts[transportId].goWriteStep()
	if datagram != nil {
		if conn.outgoing.haveChunksToSendNow() {
			fctx.ts[transportId].addConnectionToSendQueueLocked(conn)
		}

		// datagram send
		dstId := addressToTransportId(conn.remoteAddr.String())
		datagramCopy := make([]byte, len(datagram))
		copy(datagramCopy, datagram)
		fctx.network[dstId] = append(fctx.network[dstId], TestDatagram{
			datagram: datagramCopy,
			addr:     netip.MustParseAddrPort(fctx.ts[transportId].socket.LocalAddr().(*net.UDPAddr).String()),
		})

		if newResendTimer {
			// resend timer activation
			fctx.ts[transportId].resendTimers.PushBack(conn)
			conn.inResendQueue = true
		}
	} else {
		fctx.ts[transportId].writeMu.Unlock()
	}
}

// transport writeMu must be unlocked
func doGoReadStep(fctx *FuzzTransportContext, transportId int, dgrmId int) {
	defer checkInvariants(fctx)

	l := len(fctx.network[transportId])
	if l == 0 {
		return
	}
	fuzzDatagram := fctx.network[transportId][dgrmId%l]
	// remove datagram from network
	fctx.network[transportId][dgrmId%l] = fctx.network[transportId][l-1]
	fctx.network[transportId] = fctx.network[transportId][:l-1]

	var enc tlnetUdpPacket.EncHeader
	var closed bool
	var resendReq tlnetUdpPacket.ResendRequest
	conn, closed, err := fctx.ts[transportId].processIncomingDatagram(fuzzDatagram.addr, fctx.ts[transportId].localPid.Ip, fuzzDatagram.datagram, &enc, &resendReq)
	if err != nil {
		panic(err)
	}

	if closed {
		conn.closed = true
	}

	goReadHandleEncHdr(fctx, transportId, conn, enc)

	// send enc header with acquired chunks
	if len(fctx.ts[transportId].acquiredMemoryEvents) > 0 {
		for _, e := range fctx.ts[transportId].acquiredMemoryEvents {
			var encHdrAcquired tlnetUdpPacket.EncHeader
			encHdrAcquired.SetPacketNum(e.seqNum)

			goReadHandleEncHdr(fctx, transportId, e.conn, encHdrAcquired)
		}
		fctx.ts[transportId].acquiredMemoryEvents = fctx.ts[transportId].acquiredMemoryEvents[:0]
	}

	if len(resendReq.Ranges) > 0 {
		fctx.ts[transportId].newResendRequestsRcvs.PushBack(ConnResendRequest{
			conn: conn,
			req:  resendReq,
		})
	}
}

func goReadHandleEncHdr(fctx *FuzzTransportContext, transportId int, conn *Connection, enc tlnetUdpPacket.EncHeader) {
	// send enc header to goWrite()
	fctx.encHdrs[transportId] = append(fctx.encHdrs[transportId], ConnectionHeader{
		conn: conn,
		enc:  enc,
	})

	if enc.IsSetPacketNum() || enc.IsSetPacketsFrom() {
		if !conn.inAckQueue && !conn.inSendQueue {
			// start ack timer
			fctx.ts[transportId].ackTimers.PushBack(conn)
			conn.inAckQueue = true
		}

		// start resend request timer if have holes
		if conn.incoming.haveHoles() && !conn.inResendRequestQueue {
			fctx.ts[transportId].resendRequestTimers.PushBack(conn)
			conn.inResendRequestQueue = true
		}
	}
}

func doEncHdrRcv(fctx *FuzzTransportContext, transportId int) {
	defer checkInvariants(fctx)

	if len(fctx.encHdrs[transportId]) == 0 {
		return
	}
	fctx.ts[transportId].newHdrRcvs.PushBack(fctx.encHdrs[transportId][0])
	fctx.encHdrs[transportId] = fctx.encHdrs[transportId][1:]
}

func doResendTimerBurn(fctx *FuzzTransportContext, transportId int) {
	defer checkInvariants(fctx)

	if fctx.ts[transportId].resendTimers.Len() == 0 {
		return
	}
	connResendTimeout := fctx.ts[transportId].resendTimers.PopFront()
	fctx.ts[transportId].newResends.PushBack(connResendTimeout)
}

func doAckTimerBurn(fctx *FuzzTransportContext, transportId int) {
	defer checkInvariants(fctx)

	if fctx.ts[transportId].ackTimers.Len() == 0 {
		return
	}
	conn := fctx.ts[transportId].ackTimers.PopFront()
	conn.inAckQueue = false
	fctx.ts[transportId].newAckSnds.PushBack(conn)
}

func doResendRequestTimerBurn(fctx *FuzzTransportContext, transportId int) {
	defer checkInvariants(fctx)

	if fctx.ts[transportId].resendRequestTimers.Len() == 0 {
		return
	}
	conn := fctx.ts[transportId].resendRequestTimers.PopFront()
	conn.inResendRequestQueue = false
	fctx.ts[transportId].newResendRequestSnds.PushBack(conn)
}

// util functions to extract fuzzing arguments
func fuzzVerbToTransportId(b byte) int {
	// transports must be degree of 2 !!!
	return int(b) & (transports - 1)
}

// also used as timerId
func fuzzVerbToDstId(b byte) int {
	return int(b) >> transportsDegree
}

func fuzzVerbToMessageSize(b byte) int {
	// message size must be aligned by 4
	return int(b) & ^0b11
}

func addressToTransportId(address string) int {
	ending := address[len(address)-2:]
	tid, err := strconv.Atoi(ending)
	if err != nil {
		log.Panicf("address ending \"%s\" must be id of a transport, %+v", ending, err)
	}
	return tid
}

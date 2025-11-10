// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"container/heap"
	"fmt"
	"log"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tlnetUdpPacket"
)

/*
UDP Transport Fuzzing

4 agents - every is a single Transport

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
const transportsDegree = 2
const transports = 1 << transportsDegree

const MaxFuzzChunkSize = 1 << 5
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

// for https://github.com/dvyukov/go-fuzz
func FuzzDyukov(fuzz []byte) int {
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
		tIdCopy := tId
		fctx.ts[tId], err = NewTransport(
			MaxFuzzTransportMemory,
			[]string{
				"01234567890123456789012345678901",
			},
			nil,
			udpAddr,
			uint32(time.Now().Unix()),
			func(conn *Connection) {
				conn.MessageHandle = receiveMessageHandler(fctx, addressToTransportId(conn.remoteAddr().String()), tIdCopy)
				conn.StreamLikeIncoming = testStreamLikeIncoming
			},
			func(_ *Connection) {},
			nil,
			nil,
			0,
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
				i += 3
				continue
			}
			messageSize := fuzzVerbToMessageSize(fuzz[i+2])
			if messageSize == 0 {
				// Out protocol doesn't support empty messages
				i += 3
				continue
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
				i += 2
				continue
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
			i += 1
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

				srcId := portToTransportId(int(portFromNetPid(conn.remotePid())))

				receivedChunks := 0
				for s := conn.incoming.ackPrefix; s < conn.incoming.nextSeqNo; s++ {
					ch, _ := conn.incoming.windowChunks.Get(s)
					if ch.received() {
						receivedChunks++
					}
				}
				ackedChunks := 0
				for s := conn.outgoing.ackSeqNoPrefix; s < conn.outgoing.nextSeqNo; s++ {
					if conn.outgoing.window.GetPtr(s).acked() {
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
				if statusWas.AckedPrefix > conn.outgoing.ackSeqNoPrefix {
					panic("connection acked prefix decreased")
				} else if statusWas.AckedPrefix == conn.outgoing.ackSeqNoPrefix {
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

					AckedPrefix:  conn.outgoing.ackSeqNoPrefix,
					AcksInWindow: ackedChunks,
				}

				if conn.outgoing.haveChunksToSendNow(t) || conn.GetFlag(inResendQueueFlag) || conn.GetFlag(inAckQueueFlag) {
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
					srcId := portToTransportId(int(portFromNetPid(conn.remotePid())))
					log.Printf("progress %d -> %d: %+v", srcId, tId, progressStatuses[tId][srcId])
				}
			}
			log.Printf(
				"Connection %d -> %d have data (chunks or acks) to send",
				portToTransportId(int(connHaveDataToSend.incoming.transport.localPid.PortPid&0xffff)),
				portToTransportId(int(portFromNetPid(connHaveDataToSend.remotePid()))),
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
		if t.acquiredMemory+t.memoryWaiters.Front().incoming.requestedMemorySize <= t.incomingMessagesMemoryLimit {
			panic("Transport can acquire memory for the first IncomingConnection in Transport queue")
		}
	}

	realMemoryWaiters := make(map[*Connection]struct{})
	for i := 0; i < t.memoryWaiters.Len(); i++ {
		memoryWaiter := t.memoryWaiters.Index(i)
		if j, exists := realMemoryWaiters[memoryWaiter]; exists {
			panic(fmt.Sprintf("IncomingConnection attends twice in Transport queue (indices %d and %d)", j, i))
		}
		realMemoryWaiters[memoryWaiter] = struct{}{}
		if memoryWaiter.incoming.transport != t {
			panic(fmt.Sprintf("IncomingConnection in Transport queue (%d index) has different transport pointer", i))
		}
		if !memoryWaiter.incoming.inMemoryWaitersQueue {
			panic(fmt.Sprintf("IncomingConnection is in Transport queue (%d index), but has no flag inMemoryWaitersQueue", i))
		}
	}

	for _, conn := range t.handshakeByPid {
		if conn.incoming.inMemoryWaitersQueue {
			if _, exists := realMemoryWaiters[conn]; !exists {
				panic("IncomingConnection has flag inMemoryWaitersQueue, but doesn't exists in memory waiters queue")
			}
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

			chunk, _ := c.incoming.windowChunks.Get(ackNum)
			if chunk.received() {
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
	if !c.windowChunks.Empty() {
		ch, _ := c.windowChunks.Get(c.ackPrefix)
		if ch.received() {
			panic("IncomingConnection first chunk in window is received")
		}
	}
	if c.windowChunks.LenMoreThan1() && !c.windowChunks.Back().V.received() {
		panic("IncomingConnection last chunk in window (with length > 1) is not received")
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
		netip.MustParseAddrPort(fctx.ts[dstId].socketAddr.String()),
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
	datagram, conn, newResendTimer, _, _, _ := fctx.ts[transportId].goWriteStep()
	if datagram != nil {
		if conn.outgoing.haveChunksToSendNow(fctx.ts[transportId]) {
			fctx.ts[transportId].addConnectionToSendQueueLocked(conn)
		}

		// datagram send
		dstId := addressToTransportId(conn.remoteAddr().String())
		datagramCopy := make([]byte, len(datagram))
		copy(datagramCopy, datagram)
		fctx.network[dstId] = append(fctx.network[dstId], TestDatagram{
			datagram: datagramCopy,
			addr:     netip.MustParseAddrPort(fctx.ts[transportId].socketAddr.String()),
		})

		if newResendTimer {
			// resend timer activation
			heap.Push(&fctx.ts[transportId].resendTimers, conn)
			conn.SetFlag(inResendQueueFlag, true)
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
		conn.SetFlag(closedFlag, true)
	}

	goReadHandleEncHdr(fctx, transportId, conn, enc)

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
		if !conn.GetFlag(inAckQueueFlag) && !conn.GetFlag(inSendQueueFlag) {
			// start ack timer
			fctx.ts[transportId].ackTimers.PushBack(conn)
			conn.SetFlag(inAckQueueFlag, true)
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
	connResendTimeout := heap.Pop(&fctx.ts[transportId].resendTimers).(*Connection)
	fctx.ts[transportId].newResends.PushBack(connResendTimeout)
}

func doAckTimerBurn(fctx *FuzzTransportContext, transportId int) {
	defer checkInvariants(fctx)

	if fctx.ts[transportId].ackTimers.Len() == 0 {
		return
	}
	conn := fctx.ts[transportId].ackTimers.PopFront()
	conn.SetFlag(inAckQueueFlag, false)
	fctx.ts[transportId].newAckSnds.PushBack(conn)
}

func doResendRequestTimerBurn(fctx *FuzzTransportContext, transportId int) {
	defer checkInvariants(fctx)

	if fctx.ts[transportId].resendRequestTimers.Len() == 0 {
		return
	}
	conn := fctx.ts[transportId].resendRequestTimers.PopFront()
	conn.SetFlag(inResendRequestQueueFlag, false)
	fctx.ts[transportId].newResendRequestSnds.PushBack(conn)
}

// util functions to extract fuzzing arguments
func fuzzVerbToTransportId(b byte) int {
	// transports must be degree of 2 !!!
	return int(b) & (transports - 1)
}

// also used as timerId
func fuzzVerbToDstId(b byte) int {
	return int(b) >> (8 - transportsDegree)
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

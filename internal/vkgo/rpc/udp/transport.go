// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"crypto/cipher"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/ige"

	"github.com/VKCOM/statshouse/internal/vkgo/algo"
	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tlnet"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tlnetUdpPacket"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/udp/centaur"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

const MinCryptoKeyLen = 32

const MinResendTimeoutMillis = int64(30)
const MaxResendTimeoutMillis = int64(60_000)
const ResendTimeoutCoef = float64(1.5)
const MinResendRequestTimeoutMillis = int64(10)
const RegenerateTimeoutMillis = 5 * 60 * 1000
const DefaultMaxWindowSize = 1000

const TransportVersion = 2

type ConnectionID struct {
	IP   uint32 // Actual remote address. Motivation - prevent collisions of hash between clients
	Port uint16 // Actual remote port. Motivation - prevent collisions of hash between clients
	Hash int64  // common connection identifier, defined in VK UDP Protocol. TODO - each side must be able to select its own hash
}

type ConnectionStatus uint8

const (
	ConnectionStatusWaitingForRemotePid ConnectionStatus = iota
	ConnectionStatusWaitingForHash
	ConnectionStatusEstablished
	ConnectionSentObsoleteHash
	ConnectionSentObsoleteGeneration
)

// MessageHandler is called when full message is received by transport.
// message - pointer to slice with received message
// canSave - whether handler owns message pointer and can do with it whatever it wants (usually return it to pool after use).
// canSave == false means handler can only read message and must copy it in case he wants to use it after return.
type MessageHandler func(message *[]byte, canSave bool)

// Connection - Every Connection is created
// 1) either in Transport::ConnectTo() method and has fields MessageHandle, StreamLikeIncoming and UserData set to method parameters,
// 2) or in Transport::goRead() goroutine after datagram receive from the other endpoint.
// In second case fields MessageHandle, StreamLikeIncoming and UserData must be set by Transport::acceptHandler,
// obtained in NewTransport() method.
type Connection struct {
	id     ConnectionID
	status ConnectionStatus

	outgoing OutgoingConnection // outgoing messages, accessed only by Transport::goWrite
	acks     AcksToSend         // data structure to send acks, accessed only by Transport::goWrite
	incoming IncomingConnection // incoming messages, accessed only by Transport::goRead

	// These fields are public, because higher protocol should set then in acceptHandler and can use them in closeHandler
	MessageHandle      MessageHandler
	StreamLikeIncoming bool
	UserData           any

	remoteIp        uint32
	remotePort      uint16
	remoteProcessId uint16
	remoteUtime     uint32
	generation      uint32
	localIp         uint32
	activeSide      bool

	randomKey [8]uint32
	// readOld and writeOld ciphers are derived from full client and server pids
	readNew  cipher.Block
	writeNew cipher.Block
	keyID    [4]byte

	flags        uint32 // accessed only under writeMu lock
	goWriteFlags uint32 // accessed only by goWrite

	resendTimeout int64
	resendTimeMs  int64

	ackTimeMs int64

	resendRequestTimeout int64
	resendRequestTimeMs  int64

	regenerateTimeMs int64

	receivedObsoleteHash int64
}

const (
	// accessed under writeMu
	closedFlag               = iota
	inSendQueueFlag          = iota
	inAckQueueFlag           = iota
	inResendRequestQueueFlag = iota
	inRegenerateQueue        = iota
	stopRegenerateTimerFlag  = iota

	// TODO separate goWrite flags from others
	// accessed by goWrite only
	inResendQueueFlag      = iota
	ackSentFlag            = iota // when true - means that acks were sent with payload before ack timer burned - so no need to send acks again this time
	forceResendRequestFlag = iota
)

func (c *Connection) flagPtr(flag int) *uint32 {
	var flagsPtr *uint32
	if flag == inResendQueueFlag || flag == ackSentFlag || flag == forceResendRequestFlag {
		flagsPtr = &c.goWriteFlags
	} else {
		flagsPtr = &c.flags
	}
	return flagsPtr
}

func (c *Connection) SetFlag(flag int, value bool) {
	flagsPtr := c.flagPtr(flag)
	if value {
		*flagsPtr |= 1 << flag
	} else {
		*flagsPtr &= ^(1 << flag)
	}
}

func (c *Connection) GetFlag(flag int) bool {
	flagsPtr := c.flagPtr(flag)
	return (*flagsPtr & (1 << flag)) > 0
}

func (c *Connection) SendMessage(message *[]byte) error {
	return c.sendMessageImpl(message, false)
}

func (c *Connection) SendUnreliableMessage(message *[]byte) error {
	return c.sendMessageImpl(message, true)
}

func (c *Connection) sendMessageImpl(message *[]byte, unreliable bool) error {
	if len(*message) == 0 {
		return fmt.Errorf("message must be non empty")
	}

	c.incoming.transport.writeMu.Lock()
	if unreliable {
		c.incoming.transport.unreliableMessages.PushBack(ConnectionMessage{
			conn:    c,
			message: message,
		})
	} else {
		c.incoming.transport.newMessages.PushBack(ConnectionMessage{
			conn:    c,
			message: message,
		})
	}
	c.incoming.transport.writeMu.Unlock()
	c.incoming.transport.writeCV.Signal()
	if !unreliable {
		c.incoming.transport.stats.OutgoingMessagesInflight.Add(1)
	}
	return nil
}

func (c *Connection) remoteAddr() Addr { return Addr{Ip: c.remoteIp, Port: c.remotePort} }

// TODO - can we track this correctly?
func (c *Connection) ListenAddr() net.Addr {
	return Addr{Ip: c.incoming.transport.localPid.Ip, Port: portFromNetPid(c.incoming.transport.localPid)}
}

func (c *Connection) LocalAddr() net.Addr {
	return Addr{Ip: c.localIp, Port: portFromNetPid(c.incoming.transport.localPid)}
}

func (c *Connection) RemoteAddr() net.Addr { return Addr{Ip: c.remoteIp, Port: c.remotePort} }

func (c *Connection) KeyID() [4]byte {
	return c.keyID
}

func (c *Connection) localPid() tlnet.Pid {
	pid := c.incoming.transport.localPid
	pid.Ip = c.localIp
	return pid
}

// goRead() can update c.remoteUtime and c.remoteProcessId, so others must call this method under locked writeMu !!!
func (c *Connection) remotePid() tlnet.Pid {
	return tlnet.Pid{
		Ip:      c.remoteIp,
		PortPid: uint32(c.remotePort) | (uint32(c.remoteProcessId) << 16),
		Utime:   c.remoteUtime,
	}
}

func (c *Connection) resetLockedState() {
	// nothing to do
}

// TODO test close connection algorithm and check all properties
func (c *Connection) resetGoReadUnlockedState() {
	var lastMessage *IncomingMessage
	for !c.incoming.windowChunks.Empty() {
		entry := c.incoming.windowChunks.Front()
		message := entry.V.message
		if !entry.V.received() && message != nil && message != &fakeMessage {
			// unreceived chunk with allocated message

			message.remaining--
			if message.remaining == 0 {
				// no more chunks

				if message.data != nil {
					*message.data = (*message.data)[:0]
					c.incoming.transport.messageDeallocator(message.data)
					message.data = nil
					c.incoming.transport.incomingMessagesPool = append(c.incoming.transport.incomingMessagesPool, message)
					c.incoming.transport.stats.MessageReleased.Add(1)
				}
			} else {
				lastMessage = message
			}
		}
		c.incoming.windowChunks.Delete(entry.K)
	}
	if lastMessage != nil && lastMessage.remaining > 0 {
		// only last message may have not all chunks in window
		if lastMessage.data != nil {
			*lastMessage.data = (*lastMessage.data)[:0]
			c.incoming.transport.messageDeallocator(lastMessage.data)
			lastMessage.data = nil
			c.incoming.transport.incomingMessagesPool = append(c.incoming.transport.incomingMessagesPool, lastMessage)
			c.incoming.transport.stats.MessageReleased.Add(1)
		}
	}
	c.incoming.windowChunks = algo.TreeMap[uint32, IncomingChunk, seqNumCompT]{}

	c.incoming.requestedMemorySize = 0
	c.incoming.transport.tryAcquireMemory(c)
	c.incoming.transport.releaseMemory(c.incoming.messagesTotalOffset - c.incoming.messagesBeginOffset)

	c.incoming.transport = nil
	c.incoming.conn = nil
	c.incoming.chunkPayloadForMyFriendEscapeAnalyzer = nil
	c.incoming = IncomingConnection{}

	c.UserData = nil
	c.StreamLikeIncoming = false
	c.MessageHandle = nil
	c.readNew = nil
}

func (c *Connection) resetGoWriteUnlockedState(t *Transport) {
	_ = c.outgoing.AckPrefix(t, c.outgoing.nextSeqNo)
	for c.outgoing.messageQueue.Len() > 0 {
		message := c.outgoing.messageQueue.PopFront()
		*message.payload = (*message.payload)[:0]
		t.messageDeallocator(message.payload)
		t.stats.OutgoingMessagesInflight.Add(-1)
		message.payload = nil
	}
	c.outgoing.window = algo.TreeMap[uint32, OutgoingChunk, seqNumCompT]{}
	c.outgoing.messageQueue = algo.CircularSlice[*OutgoingMessage]{}
	c.outgoing.resendRanges = tlnetUdpPacket.ResendRequest{}
	c.outgoing = OutgoingConnection{}

	c.acks.ackSetReused = nil
	c.acks.nackSetReused = nil
	r := c.acks.firstRange
	c.acks.firstRange = nil
	for r != nil {
		n := r.next
		r.next = nil
		r = n
	}
	c.acks = AcksToSend{}

	c.writeNew = nil
}

// acts like bump_generation in engine repo
func (t *Transport) goReadOnRegenerate(oldConn *Connection) {
	if oldConn.id.Hash != 0 {
		delete(t.connectionById, oldConn.id)
		t.stats.ConnectionsMapSize.Store(int64(len(t.connectionById)))
	}
	handshakeCid := ConnectionID{IP: oldConn.remoteIp, Port: oldConn.remotePort}
	t.handshakeByPidMu.Lock()
	delete(t.handshakeByPid, handshakeCid)
	t.handshakeByPidMu.Unlock()
	t.closeHandler(oldConn)

	localPid := oldConn.localPid()
	remotePid := oldConn.remotePid()
	if oldConn.activeSide {
		remotePid = tlnet.Pid{
			Ip:      oldConn.remoteIp,
			PortPid: uint32(oldConn.remotePort),
		}
	}
	newGeneration := oldConn.generation + 1
	keyID := oldConn.keyID

	activeSide := oldConn.activeSide
	var status ConnectionStatus
	if activeSide {
		status = ConnectionStatusWaitingForRemotePid
	} else {
		status = ConnectionStatusWaitingForHash
	}

	incomingMessageHandle := oldConn.MessageHandle
	streamLikeIncoming := oldConn.StreamLikeIncoming
	userData := oldConn.UserData

	oldConn.resetGoReadUnlockedState()

	// TODO remove copy paste from here and processIncomingDatagramPid
	conn := t.createConnection(status, localPid.Ip, remotePid, newGeneration, activeSide)

	if activeSide {
		conn.MessageHandle = incomingMessageHandle
		conn.StreamLikeIncoming = streamLikeIncoming
		conn.UserData = userData

		conn.keyID = keyID

		t.handshakeByPidMu.Lock()
		if _, exists := t.handshakeByPid[handshakeCid]; exists {
			// While we were creating new connection, somebody has already created it
			// (either goRead() read datagram from that endpoint, or somebody also called ConnectTo in parallel)
			t.handshakeByPidMu.Unlock()
			return
		}
		if t.shutdown {
			t.handshakeByPidMu.Unlock()
			return
		}
		t.handshakeByPid[handshakeCid] = conn
		t.handshakeByPidMu.Unlock()

		return
	}

	connectionHash := int64(CalcHash(localPid, remotePid, newGeneration))
	if t.debugUdpRPC >= 1 {
		log.Printf("calculated hash(local(%s) remote(%s) generation(%d)) => %d for %s", printPid(localPid), printPid(remotePid), newGeneration, connectionHash, conn.remoteAddr().String())
	}
	cid := ConnectionID{IP: remotePid.Ip, Port: portFromNetPid(remotePid), Hash: connectionHash}
	conn.id = cid
	t.connectionById[cid] = conn
	t.stats.ConnectionsMapSize.Store(int64(len(t.connectionById)))

	// for next datagram we must generate new keys
	conn.readNew, conn.writeNew = t.generateCryptoKeys(t.cryptoKeys[keyID], localPid, remotePid, newGeneration)

	// create server connection

	// We must update handshakeByPid map and call t.acceptHandler strictly after t.generateCryptoKeysNew(),
	// because otherwise ConnectTo() can catch Connection from handshakeByPid map
	// and then Connection.SendMessage() can access new crypto keys, introducing data race,
	t.handshakeByPidMu.Lock()
	_, exists := t.handshakeByPid[handshakeCid]
	if exists {
		// While we were creating new connection, somebody has already created it
		// (somebody called ConnectTo in parallel)
	} else {
		if t.shutdown {
			// cannot accept new connections
			t.handshakeByPidMu.Unlock()
			return
		}

		t.acceptHandler(conn)
		t.handshakeByPid[handshakeCid] = conn
	}
	t.handshakeByPidMu.Unlock()
}

// TODO normal naming !!!!!!!!!!!!!
type ConnectionMessage struct {
	conn    *Connection
	message *[]byte
}

type ConnResendRequest struct {
	conn *Connection
	req  tlnetUdpPacket.ResendRequest
}

type ConnectionHeader struct {
	conn *Connection
	enc  tlnetUdpPacket.EncHeader
}

type Transport struct {
	stats TransportStats

	// accessed from goRead and ConnectTo
	handshakeByPidMu sync.RWMutex
	handshakeByPid   map[ConnectionID]*Connection // here all CID have zero Hash
	shutdown         bool

	debugUdpRPC int // 0 - nothing; 1 - prints key udp events; 2 - prints all udp activities (<0 equals to 0; >2 equals to 2)

	// accessed only from goRead
	connectionById map[ConnectionID]*Connection

	// goWrite waits for changes in newMessages, newHdrRcvs, newAckSnds, connectionSendQueue...,
	// then after wake up moves them to local copies
	// and updates state of OutgoingConnection and AcksToSend exclusively without locking
	writeMu               sync.Mutex
	writeCV               *sync.Cond // signaled when newMessages, newHdrRcvs, newAckSnds or newResends has new value
	newMessages           algo.CircularSlice[ConnectionMessage]
	unreliableMessages    algo.CircularSlice[ConnectionMessage]
	newHdrRcvs            algo.CircularSlice[ConnectionHeader] // goWrite use it to update Connection::AcksToSend and Connection::OutgoingConnection
	newResends            algo.CircularSlice[*Connection]
	newAckSnds            algo.CircularSlice[*Connection]
	newResendRequestsRcvs algo.CircularSlice[ConnResendRequest]
	newResendRequestSnds  algo.CircularSlice[*Connection]
	connectionSendQueue   algo.CircularSlice[*Connection]
	closedConnections     algo.CircularSlice[*Connection]
	newDumpUdpTargets     bool

	// accessed only from goWrite
	// with every step goWrite moves slices above to local copies and iterates over them without locking
	newMessagesLocal           algo.CircularSlice[ConnectionMessage]
	newHdrRcvsLocal            algo.CircularSlice[ConnectionHeader]
	newResendsLocal            algo.CircularSlice[*Connection]
	newAckSndsLocal            algo.CircularSlice[*Connection]
	newResendRequestsRcvsLocal algo.CircularSlice[ConnResendRequest]
	newResendRequestSndsLocal  algo.CircularSlice[*Connection]
	connectionSendQueueLocal   algo.CircularSlice[*Connection]
	resendTimersLocal          algo.CircularSlice[*Connection]
	resendRequestTimersLocal   algo.CircularSlice[*Connection]
	closedConnectionsLocal     algo.CircularSlice[*Connection]

	resendCV     chan struct{} // signaled from goWrite when resendTimers has new min value and from Transport::Close()
	resendTimers TimersPriorityQueue

	ackTimeout int64
	ackCV      *sync.Cond // signaled from goRead when ackTimers has new value
	ackTimers  algo.CircularSlice[*Connection]

	resendRequestCV     chan struct{} // signaled from goWrite when resendRequestTimers has new value and from Transport::Close()
	resendRequestTimers TimersPriorityQueue

	regenerateTimeout int64
	regenerateCV      *sync.Cond // signaled from goWrite when regenerateTimers has new value
	regenerateTimers  algo.CircularSlice[*Connection]

	socket     *net.UDPConn
	socketAddr *net.UDPAddr // separate, for fuzzing when socket is nil
	localPid   tlnet.Pid    // if IP is 0.0.0.0, then Transport forbids ConnectTo() and every accepted connection will have Connection::localIp equal to dstIp of received datagram
	closed     bool

	wg sync.WaitGroup

	// handlers from uplevel protocol
	// acceptHandler is called when transport receives new connection handshake from other side
	// acceptHandler can set Connection::MessageHandle, Connection::StreamLikeIncoming and Connection::UserData
	acceptHandler func(connection *Connection)
	// closeHandler is called when connection is need to be closed.
	// TODO clarify semantic, implement graceful close and Connection.Close() method
	closeHandler func(connection *Connection)

	// Customizers for incoming message allocation and outgoing message deallocation
	// Can be nil, in this case will be default - allocator just allocates message, deallocator does nothing
	// messageAllocator must return pointer on message with len = size
	// messageDeallocator accepts pointer on message with len = 0 (for example, for further reuse)
	messageAllocator   func(size int) (message *[]byte)
	messageDeallocator func(messagePtr *[]byte)

	// IncomingConnection memory management
	incomingMessagesMemoryLimit int64
	acquiredMemory              int64
	memoryWaiters               algo.CircularSlice[*Connection]

	// newGoReadRegenerates accessed by goRead and goRegenerate under writeMu
	newGoReadRegenerates algo.CircularSlice[*Connection]
	// newGoReadRegeneratesLocal accessed exclusively by goRead
	newGoReadRegeneratesLocal algo.CircularSlice[*Connection]

	// memory pools for reuse
	writeChunks          [][]byte
	outgoingMessagesPool []*OutgoingMessage
	outgoingChunksAlloc  algo.SliceCacheAllocator[algo.TreeNode[algo.Entry[uint32, OutgoingChunk]]]
	incomingMessagesPool []*IncomingMessage
	incomingChunksAlloc  algo.SliceCacheAllocator[algo.TreeNode[algo.Entry[uint32, IncomingChunk]]]

	// encrypting and decrypting
	cryptoKeys           map[[4]byte]string
	readByteCache        []byte
	writeBuffer          []byte
	writeEncryptedBuffer []byte

	// prevWasReliable helps to achieve fairness guaranties between reliable and unreliable messages
	prevWasReliable bool

	// window options
	maxIncomingWindowSize  int
	maxOutgoingWindowSize  int
	maxOutgoingPayloadSize int // TODO make maxOutgoingPayloadSize equal corresponding value in engine repo
}

type TransportStats struct {
	// these metrics are valuable in per second calculation
	NewIncomingMessages           atomic.Int64
	MessageHandlerCalled          atomic.Int64 // only for reliable messages
	MessageReleased               atomic.Int64
	DatagramRead                  atomic.Int64
	DatagramWritten               atomic.Int64
	UnreliableMessagesReceived    atomic.Int64
	ObsoletePidReceived           atomic.Int64
	ObsoleteHashReceived          atomic.Int64
	ObsoleteGenerationReceived    atomic.Int64
	ResendRequestReceived         atomic.Int64
	UnreliableMessagesSent        atomic.Int64
	ObsoletePidSent               atomic.Int64
	ObsoleteHashSent              atomic.Int64
	ObsoleteGenerationSent        atomic.Int64
	ResendRequestSent             atomic.Int64
	AckTimerBurned                atomic.Int64
	ResendTimerBurned             atomic.Int64
	ResendRequestTimerBurned      atomic.Int64
	RegenerateTimerBurned         atomic.Int64
	HoleSeqNumsSent               atomic.Int64
	RequestedSeqNumsOutOfWindow   atomic.Int64
	RequestedSeqNumsActuallyAcked atomic.Int64
	RequestedSeqNumsNotAcked      atomic.Int64

	// the absolute values of these metrics are decent for us
	IncomingMessagesInflight atomic.Int64 // messages, that we started to receive, but didn't pass to handler
	OutgoingMessagesInflight atomic.Int64 // not acked outgoing messages
	ConnectionsMapSize       atomic.Int64
	MemoryWaitersSize        atomic.Int64
	AcquiredMemory           atomic.Int64
}

func (t *Transport) GetStats(res *TransportStats) {
	res.NewIncomingMessages.Add(t.stats.NewIncomingMessages.Load())
	res.MessageHandlerCalled.Add(t.stats.MessageHandlerCalled.Load())
	res.MessageReleased.Add(t.stats.MessageReleased.Load())
	res.DatagramRead.Add(t.stats.DatagramRead.Load())
	res.DatagramWritten.Add(t.stats.DatagramWritten.Load())
	res.UnreliableMessagesReceived.Add(t.stats.UnreliableMessagesReceived.Load())
	res.ObsoletePidReceived.Add(t.stats.ObsoletePidReceived.Load())
	res.ObsoleteHashReceived.Add(t.stats.ObsoleteHashReceived.Load())
	res.ObsoleteGenerationReceived.Add(t.stats.ObsoleteGenerationReceived.Load())
	res.ResendRequestReceived.Add(t.stats.ResendRequestReceived.Load())
	res.UnreliableMessagesSent.Add(t.stats.UnreliableMessagesSent.Load())
	res.ObsoletePidSent.Add(t.stats.ObsoletePidSent.Load())
	res.ObsoleteHashSent.Add(t.stats.ObsoleteHashSent.Load())
	res.ObsoleteGenerationSent.Add(t.stats.ObsoleteGenerationSent.Load())
	res.ResendRequestSent.Add(t.stats.ResendRequestSent.Load())
	res.ResendTimerBurned.Add(t.stats.ResendTimerBurned.Load())
	res.AckTimerBurned.Add(t.stats.AckTimerBurned.Load())
	res.ResendRequestTimerBurned.Add(t.stats.ResendRequestTimerBurned.Load())
	res.RegenerateTimerBurned.Add(t.stats.RegenerateTimerBurned.Load())
	res.HoleSeqNumsSent.Add(t.stats.HoleSeqNumsSent.Load())
	res.RequestedSeqNumsOutOfWindow.Add(t.stats.RequestedSeqNumsOutOfWindow.Load())
	res.RequestedSeqNumsActuallyAcked.Add(t.stats.RequestedSeqNumsActuallyAcked.Load())
	res.RequestedSeqNumsNotAcked.Add(t.stats.RequestedSeqNumsNotAcked.Load())
	res.IncomingMessagesInflight.Add(t.stats.IncomingMessagesInflight.Load())
	res.OutgoingMessagesInflight.Add(t.stats.OutgoingMessagesInflight.Load())
	res.ConnectionsMapSize.Add(t.stats.ConnectionsMapSize.Load())
	res.MemoryWaitersSize.Add(t.stats.MemoryWaitersSize.Load())
	res.AcquiredMemory.Add(t.stats.AcquiredMemory.Load())
}

func (t *Transport) DumpUdpTargets() {
	t.writeMu.Lock()
	t.newDumpUdpTargets = true
	t.writeMu.Unlock()
}

func NewTransport(
	incomingMessagesMemoryLimit int64,
	cryptoKeys []string,
	socket *net.UDPConn,
	socketAddr net.Addr,
	startTime uint32,
	acceptHandler func(*Connection),
	closeHandler func(*Connection),
	messageAllocator func(size int) (message *[]byte),
	messageDeallocator func(message *[]byte),
	resendTimeout time.Duration,
	debugUdpRPC int,
) (*Transport, error) {
	if len(cryptoKeys) == 0 {
		return nil, fmt.Errorf("UDP transport requires at least 1 crypto key")
	}
	resendTimeoutMillis := resendTimeout.Milliseconds()
	if resendTimeoutMillis == 0 {
		resendTimeoutMillis = MinResendTimeoutMillis
	}
	if messageAllocator == nil {
		messageAllocator = func(size int) (message *[]byte) {
			m := make([]byte, size)
			return &m
		}
	}
	if messageDeallocator == nil {
		messageDeallocator = func(messagePtr *[]byte) {
			// just gc message
		}
	}
	// TODO if acceptHandler == nil, then forbid accepting new connections, or pass special flag for this
	t := &Transport{
		debugUdpRPC:                 debugUdpRPC,
		handshakeByPid:              make(map[ConnectionID]*Connection),
		connectionById:              make(map[ConnectionID]*Connection),
		ackTimeout:                  MinResendTimeoutMillis,
		regenerateTimeout:           RegenerateTimeoutMillis,
		acceptHandler:               acceptHandler,
		closeHandler:                closeHandler,
		messageAllocator:            messageAllocator,
		messageDeallocator:          messageDeallocator,
		incomingMessagesMemoryLimit: incomingMessagesMemoryLimit,
		socket:                      socket,
		socketAddr:                  socketAddr.(*net.UDPAddr),
		outgoingChunksAlloc:         algo.NewSliceCacheAllocator[algo.TreeNode[algo.Entry[uint32, OutgoingChunk]]](),
		incomingChunksAlloc:         algo.NewSliceCacheAllocator[algo.TreeNode[algo.Entry[uint32, IncomingChunk]]](),
		cryptoKeys:                  make(map[[4]byte]string),
		maxIncomingWindowSize:       DefaultMaxWindowSize,
		maxOutgoingWindowSize:       DefaultMaxWindowSize,
		maxOutgoingPayloadSize:      MaxChunkSize,
	}
	t.writeCV = sync.NewCond(&t.writeMu)
	t.resendCV = make(chan struct{}, 1)
	t.ackCV = sync.NewCond(&t.writeMu)
	t.resendRequestCV = make(chan struct{}, 1)
	t.regenerateCV = sync.NewCond(&t.writeMu)

	for _, key := range cryptoKeys {
		if len(key) < MinCryptoKeyLen {
			return nil, fmt.Errorf("crypto key is too short (%d bytes), must be at least %d bytes", len(key), MinCryptoKeyLen)
		}
		var keyID [4]byte
		copy(keyID[:], key[:4])
		n := binary.LittleEndian.Uint32(keyID[:])
		n &= CryptoKeyIdMask
		binary.LittleEndian.PutUint32(keyID[:], n&CryptoKeyIdMask)
		if _, exists := t.cryptoKeys[keyID]; exists {
			return nil, errors.New("2 crypto keys with identical keyID")
		}
		t.cryptoKeys[keyID] = key
	}

	// if passed to NewTransport() addr had 0 port, we set port chosen by os
	strAddr := t.socketAddr.String()
	realAddr := netip.MustParseAddrPort(strAddr)

	t.localPid = tlnet.Pid{
		Ip:      getIpUint32(realAddr),
		PortPid: uint32(int(realAddr.Port()) + (os.Getpid() << 16)),
		Utime:   startTime,
	}
	/*
		TODO allow ListenUDP on ":9999"
		if t.localPid.Ip == 0 {
			f, err := socket.File()
			// TODO file fd is different or not?
			if err != nil {
				return nil, err
			}
			err = syscall.SetsockoptInt(int(f.Fd()), syscall.IPPROTO_IP, syscall.IP_PKTINFO, 1)
			if err != nil {
				return nil, err
			}
		}
	*/

	return t, nil
}

func (t *Transport) Run() error {
	t.wg.Add(6)

	go t.goWrite()
	go t.goResend()
	go t.goAck()
	go t.goResendRequest()
	go t.goRegenerate()
	t.goRead()

	t.wg.Wait()

	return nil
}

func (t *Transport) StartTime() uint32 {
	return t.localPid.Utime
}

func (t *Transport) Shutdown() {
	t.handshakeByPidMu.Lock()
	t.shutdown = true
	t.handshakeByPidMu.Unlock()
}

func (t *Transport) Close() (err error) {
	t.writeMu.Lock()
	t.closed = true
	t.writeMu.Unlock()
	if t.socket != nil { // fuzzing
		err = t.socket.Close()
	}
	t.writeCV.Signal()
	t.ackCV.Signal()

	select {
	case t.resendCV <- struct{}{}:
	default:
	}

	select {
	case t.resendRequestCV <- struct{}{}:
	default:
	}

	return err
}

func (t *Transport) setStatusAndAddConnectionToSendQueueUnlocked(conn *Connection, status ConnectionStatus) {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	conn.status = status
	t.addConnectionToSendQueueLocked(conn)
}

func (t *Transport) addConnectionToSendQueueLocked(conn *Connection) {
	if !conn.GetFlag(inSendQueueFlag) {
		// TODO maybe allow several instances of connection in send queue?
		t.connectionSendQueue.PushBack(conn)
		conn.SetFlag(inSendQueueFlag, true)
	}
}

func (t *Transport) addConnectionToSendQueueUnlocked(conn *Connection) {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	t.addConnectionToSendQueueLocked(conn)
}

func (t *Transport) popConnectionFromSendQueueLocked() *Connection {
	conn := t.connectionSendQueue.PopFront()
	conn.SetFlag(inSendQueueFlag, false)
	return conn
}

var ErrTransportClosed = errors.New("udp transport closed")
var ErrTransportShutdown = errors.New("udp transport shutdown")
var ErrOnlyListenTransport = errors.New("udp transport has local address 0.0.0.0, cannot connect as active side")

func (t *Transport) ConnectTo(addr netip.AddrPort, incomingMessageHandle MessageHandler, streamLikeIncoming bool, userData any) (*Connection, error) {
	if t.localPid.Ip == 0 {
		// TODO discuss
		return nil, ErrOnlyListenTransport
	}
	if incomingMessageHandle == nil {
		// TODO why is it panic? Someone may want transport for just sending messages
		panic("nil message handle function")
	}

	ip, port := ipPortFromAddr(addr)
	handshakeID := ConnectionID{
		IP:   ip,
		Port: port,
		Hash: 0,
	}

	// Fast pass
	t.handshakeByPidMu.RLock()
	if t.handshakeByPid == nil {
		t.handshakeByPidMu.RUnlock()
		return nil, ErrTransportClosed
	}
	conn, exists := t.handshakeByPid[handshakeID]
	t.handshakeByPidMu.RUnlock()
	if exists {
		return conn, nil
	}

	// TODO make handshake with remote side
	conn = t.createConnection(ConnectionStatusWaitingForRemotePid, t.localPid.Ip, tlnet.Pid{Ip: ip, PortPid: uint32(port)}, 0, true)
	conn.MessageHandle = incomingMessageHandle
	conn.StreamLikeIncoming = streamLikeIncoming
	conn.UserData = userData

	for keyID := range t.cryptoKeys {
		// choose any key ID
		conn.keyID = keyID
		break
	}

	t.handshakeByPidMu.Lock()
	if t.handshakeByPid == nil {
		t.handshakeByPidMu.Unlock()
		return nil, ErrTransportClosed
	}
	if connOther, exists := t.handshakeByPid[handshakeID]; exists {
		// While we were creating new connection, somebody has already created it
		// (either goRead() read datagram from that endpoint, or somebody also called ConnectTo in parallel)
		t.handshakeByPidMu.Unlock()
		return connOther, nil
	}
	if t.shutdown {
		t.handshakeByPidMu.Unlock()
		return nil, ErrTransportShutdown
	}
	t.handshakeByPid[handshakeID] = conn
	t.handshakeByPidMu.Unlock()

	// TODO design interface of connection error passing
	return conn, nil
}

func commonCloseError(err error) bool {
	// TODO - better classification in some future go version
	s := err.Error()
	return strings.HasSuffix(s, "EOF") ||
		strings.HasSuffix(s, "broken pipe") ||
		strings.HasSuffix(s, "reset by peer") ||
		strings.HasSuffix(s, "use of closed network connection")
}

func (t *Transport) goRead() {
	defer func() {
		t.wg.Done()
	}()

	buf := make([]byte, 65536)
	//oob := make([]byte, 2048) TODO use oob to parse IP_PKT_INFO
	for {
		t.writeMu.Lock()
		for t.newGoReadRegenerates.Len() > 0 {
			conn := t.newGoReadRegenerates.PopFront()
			if conn.GetFlag(closedFlag) {
				continue
			}
			if conn.GetFlag(stopRegenerateTimerFlag) {
				conn.SetFlag(stopRegenerateTimerFlag, false)
				continue
			}
			t.stats.RegenerateTimerBurned.Add(1)

			conn.SetFlag(closedFlag, true)
			conn.resetLockedState()
			t.closedConnections.PushBack(conn)

			t.newGoReadRegeneratesLocal.PushBack(conn)
		}
		t.writeMu.Unlock()
		if t.newGoReadRegeneratesLocal.Len() > 0 {
			t.writeCV.Signal()
		}
		for t.newGoReadRegeneratesLocal.Len() > 0 {
			conn := t.newGoReadRegeneratesLocal.PopFront()
			if t.debugUdpRPC >= 1 {
				log.Printf("bump generation for %s", conn.remoteAddr().String())
			}
			t.goReadOnRegenerate(conn)
		}
		// TODO process n bytes in case when err != nil and n > 0 !!!
		n, addr, err := t.socket.ReadFromUDPAddrPort(buf)
		t.stats.DatagramRead.Add(1)
		if t.debugUdpRPC >= 2 {
			log.Printf("goRead() <- udp datagram from %s (%d bytes)", addr.String(), n)
		}
		if err != nil {
			if commonCloseError(err) {
				var handshakeMapCopy map[ConnectionID]*Connection

				t.handshakeByPidMu.Lock()
				handshakeMapCopy = t.handshakeByPid
				t.handshakeByPid = nil
				t.handshakeByPidMu.Unlock()

				for _, conn := range handshakeMapCopy {
					t.closeHandler(conn)
				}
				if t.debugUdpRPC >= 1 {
					log.Printf("%v goRead() closed", time.Now().String())
				}
				return
			}
			if t.debugUdpRPC >= 2 {
				log.Printf("[ <- goRead ] network error: %v", err) // TODO - rare log. Happens when attaching or detaching adapters.
			}
			continue
		}
		//log.Printf("%s [ <- goRead ] datagram read from %s (%d bytes)", t.socket.LocalAddr().String(), addr.String(), n)

		var localIp = t.localPid.Ip // TODO parse localIp from IP_PKT_INFO
		/*
			TODO allow ListenUDP on ":9999"

			oobBuffer := bytes.NewBuffer(oob)
			msg := syscall.Cmsghdr{}
			err = binary.Read(oobBuffer, binary.LittleEndian, &msg)
			if err != nil {
				log.Printf("goRead processing oob error: %v", err) // TODO - rare log. Broken packets, etc.
				continue
			}
			if msg.Level == syscall.IPPROTO_IP && msg.Type == syscall.IP_PKTINFO {
				packetInfo := syscall.Inet4Pktinfo{}
				err = binary.Read(oobBuffer, binary.LittleEndian, &packetInfo)
				if err != nil {
					log.Printf("goRead processing oob error: %v", err) // TODO - rare log. Broken packets, etc.
					continue
				}
				localIp = binary.BigEndian.Uint32(packetInfo.Addr[:])
			}
		*/

		var conn *Connection
		var closed bool
		var enc tlnetUdpPacket.EncHeader // TODO REUSE
		var resendReq tlnetUdpPacket.ResendRequest
		conn, closed, err = t.processIncomingDatagram(addr, localIp, buf[:n], &enc, &resendReq)
		if err != nil {
			if t.debugUdpRPC >= 1 {
				log.Printf("goRead processing error: %v", err) // TODO - rare log. Broken packets, etc.
			}
			continue
		}

		if conn != nil {
			t.writeMu.Lock()
			if closed {
				conn.SetFlag(closedFlag, true)
				conn.resetLockedState()
				t.closedConnections.PushBack(conn)
				t.writeMu.Unlock()
				t.writeCV.Signal()
				conn.resetGoReadUnlockedState()

				// need to process this datagram again, to create new connection
				// TODO remove this kostil sraniy and create new connection directly in place, where we closed this one !!!!!!
				// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				conn, closed, err = t.processIncomingDatagram(addr, localIp, buf[:n], &enc, &resendReq)
				if err != nil {
					if t.debugUdpRPC >= 1 {
						log.Printf("goRead processing error: %v", err) // TODO - rare log. Broken packets, etc.
					}
					continue
				}
				if closed {
					// unreachable
					log.Printf("unreachable: connection %s closed again. It is bug in udp rpc code. Send this log to https://vk.com/udp2", conn.remoteAddr().String())
					continue
				}
				if conn == nil {
					continue
				}
				t.writeMu.Lock()
			}
			if conn.GetFlag(inRegenerateQueue) {
				conn.SetFlag(stopRegenerateTimerFlag, true)
			}

			startedAckTimer := t.GoReadHandleEncHdr(conn, enc)

			if len(resendReq.Ranges) > 0 {
				t.newResendRequestsRcvs.PushBack(ConnResendRequest{
					conn: conn,
					req:  resendReq,
				})
			}

			t.writeMu.Unlock()
			// TODO may be not wake up goWrite now, but wake up with response
			t.writeCV.Signal()
			if startedAckTimer {
				t.ackCV.Signal()
			}
		}
	}
}

// GoReadHandleEncHdr must be called under t.writeMu lock
func (t *Transport) GoReadHandleEncHdr(conn *Connection, enc tlnetUdpPacket.EncHeader) (startedAckTimer bool) {
	t.newHdrRcvs.PushBack(ConnectionHeader{
		conn: conn,
		enc:  enc,
	})

	packetHasData := enc.IsSetPacketNum() || enc.IsSetPacketsFrom()
	// when inAckQueueFlag - ack timer is already run
	// when inSendQueueFlag - we will send acks with our own payload in near time
	startedAckTimer = packetHasData && !conn.GetFlag(inAckQueueFlag) && !conn.GetFlag(inSendQueueFlag)
	if startedAckTimer {
		// TODO return
		conn.ackTimeMs = time.Now().UnixMilli() + t.ackTimeout

		t.ackTimers.PushBack(conn)
		conn.SetFlag(inAckQueueFlag, true)
	}

	return
}

var ErrNoPayload = errors.New("udp datagram payload size less than 4 bytes")

func (t *Transport) processIncomingDatagram(addr netip.AddrPort, localIp uint32, payload []byte, encRef *tlnetUdpPacket.EncHeader, resendReq *tlnetUdpPacket.ResendRequest) (*Connection, bool, error) {
	if len(payload) < 4 {
		return nil, false, ErrNoPayload
	}
	// TODO: use xxh instead?
	crc32c := binary.LittleEndian.Uint32(payload[len(payload)-4:])
	payload = payload[:len(payload)-4]
	if crc32c != crc32.Checksum(payload, castagnoliTable) {
		return nil, false, fmt.Errorf("crc32c checksum mismatch")
	}

	unencLen := len(payload)
	var unenc tlnetUdpPacket.UnencHeader
	payload, err := unenc.ReadBoxed(payload)
	if err != nil {
		return nil, false, err
	}
	unencLen -= len(payload)

	if !unenc.IsSetCryptoSha() || !unenc.IsSetCryptoRandom() || !unenc.IsSetEncryptedData() {
		return nil, false, fmt.Errorf("no crypto random flag")
	}
	if !unenc.IsSetLocalPid() && !unenc.IsSetPidHash() {
		return nil, false, fmt.Errorf("neither hash nor pid are set")
	}
	if unenc.IsSetLocalPid() {
		// TODO think, if this always will be correct
		localIp = unenc.RemotePid.Ip
		return t.processIncomingDatagramPid(addr, localIp, unenc, encRef, resendReq, payload)
	} else {
		// only hash is set
		return t.processIncomingDatagramHash(addr, localIp, unenc, encRef, resendReq, payload)
	}
}

func getKeyID(cryptoFlags uint32) [4]byte {
	var res [4]byte
	binary.LittleEndian.PutUint32(res[:], (cryptoFlags>>CryptoKeyIdShift)&CryptoKeyIdMask)
	return res
}

func (t *Transport) processIncomingDatagramHash(addr netip.AddrPort, localIp uint32, unenc tlnetUdpPacket.UnencHeader, encRef *tlnetUdpPacket.EncHeader, resendReq *tlnetUdpPacket.ResendRequest, payload []byte) (*Connection, bool, error) {
	if !unenc.IsSetPidHash() {
		panic("hash is not set")
	}
	remoteHash := unenc.PidHash

	cid := ConnectionID{IP: getIpUint32(addr), Port: addr.Port(), Hash: remoteHash}
	conn, ok := t.connectionById[cid]
	if !ok {
		// That side knows our old hash

		// We create connection and send handshake to force other side update our pid to continue communicating.
		// This behaviour has the problem in case WHEN WE WERE ACTIVE CONNECTION SIDE (for example, rpc client),
		// because other side sends responses and next level protocol
		// will get responses on unknown requests from past life.
		// Also, next level protocol will get this connection in acceptHandler (instead of ConnectTo)
		// and we will have 2 rpc servers connected to each other. Both of them will be holding resources
		// with this connection but no one is sending requests (until somebody wants to send requests).
		//
		// But if we won't send handshake, other side will never try to send pid and reconnect,
		// it will only be sending obsolete hash.
		// So it will never reconnect again until restart or our own connect attempt.

		if t.debugUdpRPC >= 2 {
			log.Printf("received unknown hash %d ", remoteHash)
		}
		ip, port := ipPortFromAddr(addr)
		conn = t.createConnection(ConnectionSentObsoleteHash, localIp, tlnet.Pid{Ip: ip, PortPid: uint32(port)}, 0, false)
		conn.receivedObsoleteHash = unenc.PidHash

		for keyID := range t.cryptoKeys {
			// choose any key ID
			conn.keyID = keyID
			break
		}

		t.addConnectionToSendQueueUnlocked(conn)
		t.writeCV.Signal()
		return nil, false, nil
	}

	if conn.status == ConnectionStatusWaitingForHash {
		t.writeMu.Lock()
		conn.status = ConnectionStatusEstablished
		t.writeMu.Unlock()
	}

	conn, err := t.processEncrypted(conn, unenc, encRef, resendReq, payload, false)
	return conn, false, err
}

func (t *Transport) processIncomingDatagramPid(addr netip.AddrPort, localIp uint32, unenc tlnetUdpPacket.UnencHeader, encRef *tlnetUdpPacket.EncHeader, resendReq *tlnetUdpPacket.ResendRequest, payload []byte) (*Connection, bool, error) {
	if !unenc.IsSetLocalPid() {
		panic("pid is not set")
	}

	if (unenc.RemotePid.Utime != 0 || processIdFromNetPid(unenc.RemotePid) != 0) && (unenc.RemotePid.Utime != t.localPid.Utime || processIdFromNetPid(unenc.RemotePid) != processIdFromNetPid(t.localPid)) {
		// TODO send obsolete pid !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		// That side knows our old pid, or it is old message
		// We do not send obsolete pid, because other side will eventually receive our actual pid
		return nil, false, fmt.Errorf("received datagram with incorrect remote pid (received %+v but actual local pid is %+v) from addr %s", unenc.RemotePid, t.localPid, addr.String())
	}

	// TODO may be skip this part ???
	ip, port, err := t.ensureEqualAddrAndPid(addr, unenc.LocalPid)
	if err != nil {
		return nil, false, err
	}

	handshakeCid := ConnectionID{IP: ip, Port: addr.Port()}

	t.handshakeByPidMu.RLock()
	conn, exists := t.handshakeByPid[handshakeCid]
	t.handshakeByPidMu.RUnlock()

	created := false
	if !exists {
		// we don't know this endpoint

		if unenc.RemotePid == (tlnet.Pid{}) {
			// Obsolete Hash message
			if t.debugUdpRPC >= 2 {
				log.Printf("received Obsolete Hash message from unknown endpoint")
			}
			return nil, false, nil
		}

		// new handshake from active side
		if _, keyExists := t.cryptoKeys[getKeyID(unenc.CryptoFlags)]; !keyExists {
			return nil, false, fmt.Errorf("unknown key ID %v", getKeyID(unenc.CryptoFlags))
		}
		conn = t.createConnection(ConnectionStatusWaitingForHash, localIp, unenc.LocalPid, unenc.Generation, false)
		conn.keyID = getKeyID(unenc.CryptoFlags)

		created = true
	}

	if conn.remoteUtime != 0 && conn.remoteProcessId != 0 &&
		(conn.remoteUtime != unenc.LocalPid.Utime || conn.remoteProcessId != processIdFromNetPid(unenc.LocalPid)) {
		// known remote pid and actual datagram pid mismatch

		if unenc.LocalPid.Utime < conn.remoteUtime {
			// old handshake
			return nil, false, nil
		}

		// Remote side has renewed its pid because of restart or close
		if conn.id.Hash != 0 {
			delete(t.connectionById, conn.id)
			t.stats.ConnectionsMapSize.Store(int64(len(t.connectionById)))
		}

		t.handshakeByPidMu.Lock()
		delete(t.handshakeByPid, handshakeCid)
		t.handshakeByPidMu.Unlock()
		t.closeHandler(conn)
		return conn, true, nil
	}

	if unenc.Generation < conn.generation {
		// That side has old generation

		t.setStatusAndAddConnectionToSendQueueUnlocked(conn, ConnectionSentObsoleteGeneration)
		t.writeCV.Signal()
		return nil, false, nil
	}

	if unenc.Generation > conn.generation {
		if conn.id.Hash != 0 {
			delete(t.connectionById, conn.id)
			t.stats.ConnectionsMapSize.Store(int64(len(t.connectionById)))
		}

		t.handshakeByPidMu.Lock()
		delete(t.handshakeByPid, handshakeCid)
		t.handshakeByPidMu.Unlock()
		t.closeHandler(conn)
		return conn, true, nil
	}

	if conn.status == ConnectionStatusWaitingForRemotePid || created {
		// This code is both for server just received new handshake
		// and for client received answer handshake from server.
		// We need to generate full crypto keys and calculate session hash

		localPid := conn.localPid()
		remotePid := unenc.LocalPid
		connectionHash := int64(CalcHash(localPid, remotePid, unenc.Generation))
		if t.debugUdpRPC >= 1 {
			log.Printf("calculated hash(local(%s) remote(%s) generation(%d)) => %d for %s", printPid(localPid), printPid(remotePid), unenc.Generation, connectionHash, conn.remoteAddr().String())
		}
		cid := ConnectionID{IP: ip, Port: port, Hash: connectionHash}
		conn.id = cid
		t.connectionById[cid] = conn
		t.stats.ConnectionsMapSize.Store(int64(len(t.connectionById)))

		// for next datagram we must generate new keys
		if _, keyExists := t.cryptoKeys[getKeyID(unenc.CryptoFlags)]; !keyExists {
			return nil, false, fmt.Errorf("unknown key ID %v", getKeyID(unenc.CryptoFlags))
		}
		conn.readNew, conn.writeNew = t.generateCryptoKeys(t.cryptoKeys[getKeyID(unenc.CryptoFlags)], localPid, remotePid, unenc.Generation)

		if conn.status == ConnectionStatusWaitingForRemotePid {
			// update client connection
			t.writeMu.Lock()
			conn.status = ConnectionStatusEstablished
			conn.remoteProcessId = processIdFromNetPid(remotePid)
			conn.remoteUtime = remotePid.Utime
			t.writeMu.Unlock()
		} else {
			// create server connection

			// We must update handshakeByPid map and call t.acceptHandler strictly after t.generateCryptoKeysNew(),
			// because otherwise ConnectTo() can catch Connection from handshakeByPid map
			// and then Connection.SendMessage() can access new crypto keys, introducing data race,
			t.handshakeByPidMu.Lock()
			connOther, exists := t.handshakeByPid[handshakeCid]
			if exists {
				// While we were creating new connection, somebody has already created it
				// (somebody called ConnectTo in parallel)
				conn = connOther
			} else {
				if t.shutdown {
					// cannot accept new connections
					t.handshakeByPidMu.Unlock()
					return nil, false, nil
				}

				t.acceptHandler(conn)
				t.handshakeByPid[handshakeCid] = conn
			}
			t.handshakeByPidMu.Unlock()
		}
	}

	if unenc.RemotePid == (tlnet.Pid{}) {
		if t.debugUdpRPC >= 2 {
			log.Printf("received obsolete hash message")
		}
		//it is ObsoleteHash message
		return conn, false, err
	}
	// only client encrypts data with an old cipher
	oldCipher := unenc.RemotePid.Utime == 0 && processIdFromNetPid(unenc.RemotePid) == 0
	conn, err = t.processEncrypted(conn, unenc, encRef, resendReq, payload, oldCipher)
	return conn, false, err
}

func (t *Transport) ensureEqualAddrAndPid(addr netip.AddrPort, pid tlnet.Pid) (uint32, uint16, error) {
	ip, port := ipPortFromAddr(addr)
	unencIp, unencPort := ipPortFromNetPid(pid)
	if ip != unencIp || port != unencPort {
		// TODO support NAT networks
		ipBytes := make([]byte, 4)
		unencIpBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(ipBytes, ip)
		binary.BigEndian.PutUint32(unencIpBytes, unencIp)
		return 0, 0, fmt.Errorf(
			"mismatch address and rpc.NetPid ip/port: addr(%s:%d) and rpc.NetPid(%s:%d)",
			net.IP(ipBytes).String(), port, net.IP(unencIpBytes).String(), unencPort,
		)
	}
	return ip, port, nil
}

type seqNumCompT struct {
}

func (seqNumCompT) Cmp(s1 uint32, s2 uint32) bool { return s1 < s2 }

// TODO support stateless handshake in C++ and go rpc code and add capability bit in handshake
func (t *Transport) createConnection(status ConnectionStatus, localIp uint32, remotePid tlnet.Pid, generation uint32, activeSide bool) *Connection {
	conn := &Connection{
		status:  status,
		localIp: localIp,
		outgoing: OutgoingConnection{
			window:              algo.NewTreeMap[uint32, OutgoingChunk, seqNumCompT](&t.outgoingChunksAlloc),
			windowControlSeqNum: -1,
		},
		incoming: IncomingConnection{
			transport:     t,
			windowChunks:  algo.NewTreeMap[uint32, IncomingChunk, seqNumCompT](&t.incomingChunksAlloc),
			windowControl: -1,
		},
		generation:           generation,
		remoteIp:             remotePid.Ip,
		remotePort:           portFromNetPid(remotePid),
		remoteProcessId:      processIdFromNetPid(remotePid),
		remoteUtime:          remotePid.Utime,
		resendTimeout:        MinResendTimeoutMillis,
		resendRequestTimeout: MinResendRequestTimeoutMillis,
		activeSide:           activeSide,
	}
	conn.incoming.conn = conn
	if t.debugUdpRPC >= 1 {
		log.Printf("created udp transport connection: localPid(%+v) remotePid(%+v) generation(%d) keyId(%+x)", printPid(conn.localPid()), printPid(remotePid), generation, conn.keyID)
	}
	var randomKey [32]byte
	_, _ = cryptorand.Read(randomKey[:])
	CopyIVFrom(&conn.randomKey, randomKey)

	return conn
}

func (t *Transport) generateCryptoKeysOld(keyID [4]byte, localPid tlnet.Pid, remotePid tlnet.Pid, generation uint32) (readOld cipher.Block, writeOld cipher.Block) {
	return t.generateCryptoKeys(t.cryptoKeys[keyID], localPid, remotePid, generation)
}

func (t *Transport) generateCryptoKeys(cryptoKey string, localPid tlnet.Pid, remotePid tlnet.Pid, generation uint32) (read cipher.Block, write cipher.Block) {
	keys := DeriveCryptoKeysUdp(cryptoKey, &localPid, &remotePid, generation)
	var err error
	read, write, err = centaur.AESCentaur(keys.ReadKey[:], keys.WriteKey[:])
	if err != nil {
		log.Panicf("error while generating centaur keys: %+v", err)
	}
	return read, write
}

// TODO move to better place
const EncryptedFlagsMask uint32 = (1 << 7) - 1

func (t *Transport) readEncryptedUnencHeader(payload []byte, firstUnenc tlnetUdpPacket.UnencHeader, secondUnenc *tlnetUdpPacket.UnencHeader) ([]byte, error) {
	var err error
	if firstUnenc.IsSetPidHash() {
		var hash int64
		payload, err = basictl.LongRead(payload, &hash)
		if err != nil {
			return nil, err
		}
		secondUnenc.SetPidHash(hash)
	}
	if firstUnenc.IsSetLocalPid() {
		payload, err = secondUnenc.LocalPid.Read(payload)
		if err != nil {
			return nil, err
		}
		payload, err = secondUnenc.RemotePid.Read(payload)
		if err != nil {
			return nil, err
		}
		var generation uint32
		payload, err = basictl.NatRead(payload, &generation)
		if err != nil {
			return nil, err
		}
		secondUnenc.SetGeneration(generation)
	}
	if firstUnenc.IsSetCryptoFlags() {
		var cryptoFlags uint32
		payload, err = basictl.NatRead(payload, &cryptoFlags)
		if err != nil {
			return nil, err
		}
		secondUnenc.SetCryptoFlags(cryptoFlags)
	}

	return payload, err
}

func (t *Transport) processEncrypted(conn *Connection, unenc tlnetUdpPacket.UnencHeader, enc *tlnetUdpPacket.EncHeader, resendReq *tlnetUdpPacket.ResendRequest, payload []byte, oldCipher bool) (*Connection, error) {
	if !unenc.IsSetCryptoSha() || !unenc.IsSetCryptoRandom() || !unenc.IsSetEncryptedData() {
		return nil, fmt.Errorf("no crypto random flag")
	}

	if cap(t.readByteCache) >= len(payload) {
		t.readByteCache = t.readByteCache[:len(payload)]
	} else {
		t.readByteCache = make([]byte, len(payload))
	}

	var readCipher cipher.Block
	if oldCipher {
		readCipher, _ = t.generateCryptoKeysOld(getKeyID(unenc.CryptoFlags), unenc.RemotePid, unenc.LocalPid, conn.generation)
	} else {
		readCipher = conn.readNew
	}
	var ivb [32]byte
	CopyIVTo(&ivb, unenc.CryptoRandom)
	ige.DecryptBlocks(readCipher, ivb[:], t.readByteCache, payload)

	payload = t.readByteCache

	payload, err := enc.Read(payload)
	if err != nil {
		return nil, err
	}
	if unenc.Flags&EncryptedFlagsMask != enc.Flags&EncryptedFlagsMask {
		return nil, fmt.Errorf("encrypted and decrypted flags do not match")
	}

	receivedSupportedVersion := (enc.Version >> 16) & 0x0000ffff
	seqNum := ^uint32(0)
	if enc.IsSetPacketNum() {
		seqNum = enc.PacketNum
	}
	if enc.IsSetPacketsFrom() {
		seqNum = enc.PacketsFrom
	}
	if enc.IsSetVersion() && receivedSupportedVersion < TransportVersion && seqNum != ^uint32(0) {
		return nil, fmt.Errorf("received supported version is %d but our minimal supported version is %d", receivedSupportedVersion, TransportVersion)
	}

	paddingSize := 0
	if enc.IsSetZeroPadding1Byte() {
		paddingSize += 1
	}
	if enc.IsSetZeroPadding2Bytes() {
		paddingSize += 2
	}
	if enc.IsSetZeroPadding4Bytes() {
		paddingSize += 4
	}
	if enc.IsSetZeroPadding8Bytes() {
		paddingSize += 8
	}

	var unenc2 tlnetUdpPacket.UnencHeader
	unenc2Size := encryptedUnencHeaderSize(unenc)
	if _, err = t.readEncryptedUnencHeader(payload[len(payload)-unenc2Size:], unenc, &unenc2); err != nil {
		return nil, err
	}
	if unenc.PidHash != unenc2.PidHash {
		return nil, fmt.Errorf("unencoded header hash mismatch")
	}
	if unenc.LocalPid != unenc2.LocalPid {
		return nil, fmt.Errorf("unencoded header LocalPid mismatch")
	}
	if unenc.RemotePid != unenc2.RemotePid {
		return nil, fmt.Errorf("unencoded header RemotePid mismatch")
	}
	if unenc.Generation != unenc2.Generation {
		return nil, fmt.Errorf("unencoded header Generation mismatch")
	}
	if unenc.CryptoFlags != unenc2.CryptoFlags {
		return nil, fmt.Errorf("unencoded header CryptoFlags mismatch")
	}
	paddingStartIndex := len(payload) - unenc2Size - paddingSize
	for i := 0; i < paddingSize; i++ {
		if payload[paddingStartIndex+i] != 0 {
			return nil, fmt.Errorf("non zero padding")
		}
	}
	payload = payload[:paddingStartIndex]

	if enc.IsSetPacketNum() || enc.IsSetPacketsFrom() {
		err = conn.incoming.ReceiveDatagram(enc, resendReq, payload)
	}

	return conn, err
}

func encryptedUnencHeaderSize(unenc tlnetUdpPacket.UnencHeader) int {
	size := 0
	if unenc.IsSetPidHash() {
		size += 8
	}
	if unenc.IsSetLocalPid() {
		size += 4*3*2 + 4
	}
	if unenc.IsSetCryptoFlags() {
		size += 4
	}
	return size
}

func (t *Transport) noGoWriteEvents() bool {
	return t.newMessages.Len() == 0 && t.unreliableMessages.Len() == 0 &&
		t.newHdrRcvs.Len() == 0 && t.newResends.Len() == 0 &&
		t.newAckSnds.Len() == 0 && t.newResendRequestsRcvs.Len() == 0 &&
		t.newResendRequestSnds.Len() == 0 && t.connectionSendQueue.Len() == 0 &&
		t.closedConnections.Len() == 0 && !t.newDumpUdpTargets
}

func (t *Transport) goWrite() {
	t.writeMu.Lock()

	defer func() {
		t.writeMu.Unlock()
		t.wg.Done()
	}()

	for {
		for t.noGoWriteEvents() && !t.closed {
			t.writeCV.Wait()
		}
		if t.closed {
			return
		}

		data, conn, needResendTimer, needRegenerateTimer, needToSignalGoResend, needToSignalGoResendRequest := t.goWriteStep()
		if needToSignalGoResendRequest {
			select {
			case t.resendRequestCV <- struct{}{}:
			default:
			}
		}

		if data != nil {
			remoteAddr := conn.remoteAddr()
			var err error
			_, err = t.socket.WriteToUDPAddrPort(data, netip.AddrPortFrom(netip.AddrFrom4(ipToBytes(remoteAddr.Ip)), remoteAddr.Port))
			t.stats.DatagramWritten.Add(1)
			if t.debugUdpRPC >= 2 {
				log.Printf("goWrite() -> datagram to %s (%d bytes)", remoteAddr.String(), len(data))
			}

			t.writeMu.Lock()
			if conn.outgoing.haveChunksToSendNow(t) {
				t.addConnectionToSendQueueLocked(conn)
			}
			if err != nil {
				if commonCloseError(err) {
					return
				}
				log.Printf("goWrite: failed to write datagram: %s", err)
				continue
			}
			//log.Printf("%s [ goWrite -> ] wrote datagram (%d bytes) to address %s", t.socket.LocalAddr().String(), len(data), conn.Addr.String())

			if needResendTimer {
				conn.resendTimeMs = time.Now().UnixMilli() + conn.resendTimeout

				if t.resendTimers.Len() == 0 || conn.resendTimeMs < t.resendTimers.Min().resendTimeMs {
					needToSignalGoResend = true
				}
				t.resendTimers.Add(conn)
				conn.SetFlag(inResendQueueFlag, true)
			}
			if needRegenerateTimer {
				conn.regenerateTimeMs = time.Now().UnixMilli() + t.regenerateTimeout

				t.regenerateTimers.PushBack(conn)
				conn.SetFlag(inRegenerateQueue, true)

				t.regenerateCV.Signal()
			}
		}
		if needToSignalGoResend {
			select {
			case t.resendCV <- struct{}{}:
			default:
			}
		}
	}
}

// goWriteStep handles all new events
// Pre-condition: t.writeMu is acquired
// Post-condition: if `datagram` != nil, then t.writeMu is unlocked, otherwise locked
// returns:
// > datagram (and connection) to send if any
// > needConnResendTimer - whether resend timer for `connToWrite` is required
// > wasRepeatedResendTimers - were there any resend timers repeated
func (t *Transport) goWriteStep() (datagram []byte, connToWrite *Connection, needConnResendTimer, needConnRegenerateTimer, needToSignalGoResend, needToSignalGoResendRequest bool) {
	// When goWrite() wakes up, he moves newHdrRcvs, newMessages and newResends data
	// to newHdrRcvsLocal, newMessagesLocal and newTimeoutsLocal (just swaps slices pointers),
	// then unlocks writeMu and then updates OutgoingConnection's state without locking.
	//
	// This trick removes long mutex holding while iterating over new events
	// and lets goRead(), goTimeout() and SendMessage() not to block for the long period
	//
	t.newHdrRcvs.Swap(&t.newHdrRcvsLocal)
	t.newResends.Swap(&t.newResendsLocal)
	t.newResendRequestsRcvs.Swap(&t.newResendRequestsRcvsLocal)
	t.newResendRequestSnds.Swap(&t.newResendRequestSndsLocal)
	t.newMessages.Swap(&t.newMessagesLocal)
	t.newAckSnds.Swap(&t.newAckSndsLocal)
	t.closedConnections.Swap(&t.closedConnectionsLocal)
	//
	// UPD: we have a problem, that event handling sometime leads to addConnectionToSendQueue,
	// which requires locking, and with this trick we have to lock for every connection add.
	// For example, every newMessages event requires addConnectionToSendQueue
	//
	// To solve this we save connections in connectionSendQueueLocal

	needToDumpUdpTargets := t.newDumpUdpTargets
	t.newDumpUdpTargets = false

	t.writeMu.Unlock()

	if needToDumpUdpTargets {
		var handshakesCopy map[ConnectionID]*Connection

		t.handshakeByPidMu.RLock()
		handshakesCopy = t.handshakeByPid
		t.handshakeByPidMu.RUnlock()

		for _, conn := range handshakesCopy {
			t.writeMu.Lock()
			println("session: remote_pid", printPid(conn.remotePid()))
			println("\tgeneration", conn.generation)
			println("\tconn.status", conn.status)
			println("\tconn.acks.ackPrefix", conn.acks.ackPrefix)
			println("\tconn.outgoing.nextSeqNo", conn.outgoing.nextSeqNo)
			println("\tconn.outgoing.timeoutedSeqNum", conn.outgoing.timeoutedSeqNum)
			//println("\tmsg_confirm_set_count")
			//println("\tsince last_received_")
			//println("\tsince last_ack_")
			println("\tconn.resendTimeout", conn.resendTimeout)
			println("\tconn.resendRequestTimeout", conn.resendRequestTimeout)
			println()
			t.writeMu.Unlock()
		}
	}

	for t.closedConnectionsLocal.Len() > 0 {
		conn := t.closedConnectionsLocal.PopFront()
		conn.resetGoWriteUnlockedState(t)
	}
	for t.newHdrRcvsLocal.Len() > 0 {
		hdr := t.newHdrRcvsLocal.PopFront()
		conn := hdr.conn

		// update OutgoingConnection state with received acks
		t.handleAck(conn, &hdr.enc)

		// update AcksToSend with received seqNums
		if hdr.enc.IsSetPacketsFrom() {
			conn.acks.AddAckRange(hdr.enc.PacketsFrom, hdr.enc.PacketsFrom+hdr.enc.PacketsCount-1)
		}
		if hdr.enc.IsSetPacketNum() && hdr.enc.PacketNum != ^uint32(0) {
			conn.acks.AddAckRange(hdr.enc.PacketNum, hdr.enc.PacketNum)
		}

		conn.resendRequestTimeout = MinResendRequestTimeoutMillis
		if conn.acks.HaveHoles() {
			t.resendRequestTimersLocal.PushBack(conn)
		}
	}
	for t.newResendsLocal.Len() > 0 {
		conn := t.newResendsLocal.PopFront()
		conn.SetFlag(inResendQueueFlag, false)

		conn.outgoing.OnResendTimeout()

		if conn.outgoing.haveChunksToSendNow(t) {
			conn.resendTimeout = min(
				int64(float64(conn.resendTimeout)*ResendTimeoutCoef*ResendTimeoutCoef),
				MaxResendTimeoutMillis,
			)
			t.connectionSendQueueLocal.PushBack(conn)
		} else {
			t.stats.ResendTimerBurned.Add(1)
			conn.resendTimeout = max(
				int64(float64(conn.resendTimeout)/ResendTimeoutCoef),
				MinResendTimeoutMillis,
			)
			if !conn.outgoing.window.Empty() {
				// timer have burned but connection doesn't have timeouted chunks yet (all chunks has been sent just now) - so need to run timer again
				t.resendTimersLocal.PushBack(conn)
			}
		}
	}
	for t.newResendRequestSndsLocal.Len() > 0 {
		conn := t.newResendRequestSndsLocal.PopFront()

		if conn.acks.HaveHoles() {
			t.stats.ResendRequestTimerBurned.Add(1)
			conn.SetFlag(forceResendRequestFlag, true)
			t.connectionSendQueueLocal.PushBack(conn)

			// and re-run resend request timer
			t.resendRequestTimersLocal.PushBack(conn)
		}
	}
	for t.newResendRequestsRcvsLocal.Len() > 0 {
		connResReq := t.newResendRequestsRcvsLocal.PopFront()

		requestedSeqNumsOutOfWindow := 0
		requestedSeqNumsActuallyAcked := 0
		requestedSeqNumsNotAcked := 0
		for _, r := range connResReq.req.Ranges {
			for s := r.PacketNumFrom; s <= r.PacketNumTo; s++ {
				if s < connResReq.conn.outgoing.ackSeqNoPrefix {
					requestedSeqNumsOutOfWindow++
					continue
				}
				chunkPtr := connResReq.conn.outgoing.window.GetPtr(s)
				if chunkPtr != nil && chunkPtr.acked() {
					requestedSeqNumsActuallyAcked++
				} else {
					requestedSeqNumsNotAcked++
				}
			}
		}
		t.stats.RequestedSeqNumsOutOfWindow.Add(int64(requestedSeqNumsOutOfWindow))
		t.stats.RequestedSeqNumsActuallyAcked.Add(int64(requestedSeqNumsActuallyAcked))
		t.stats.RequestedSeqNumsNotAcked.Add(int64(requestedSeqNumsNotAcked))

		connResReq.conn.outgoing.resendRanges = connResReq.req
		connResReq.conn.outgoing.resendIndex = 0
		connResReq.conn.outgoing.rangeInnerIndex = 0
		t.connectionSendQueueLocal.PushBack(connResReq.conn)
	}
	for t.newMessagesLocal.Len() > 0 {
		cm := t.newMessagesLocal.PopFront()

		var m *OutgoingMessage
		if n := len(t.outgoingMessagesPool); n == 0 {
			m = &OutgoingMessage{}
		} else {
			m = t.outgoingMessagesPool[n-1]
			t.outgoingMessagesPool = t.outgoingMessagesPool[:n-1]
		}

		m.payload = cm.message
		m.seqNo = (1 << 32) - 1 // TODO: ???
		m.offset = cm.conn.outgoing.totalMessagesOffset
		m.refCount = 1

		cm.conn.outgoing.messageQueue.PushBack(m)
		cm.conn.outgoing.totalMessagesOffset += int64(len(*cm.message))

		t.connectionSendQueueLocal.PushBack(cm.conn)
	}
	for t.newAckSndsLocal.Len() > 0 {

		conn := t.newAckSndsLocal.PopFront()

		// if connection is inSendQueue or has already sent datagram since ack timer run, then skip empty ack
		if !conn.GetFlag(ackSentFlag) {
			t.stats.AckTimerBurned.Add(1)
			t.connectionSendQueueLocal.PushBack(conn)
		}
		conn.SetFlag(ackSentFlag, false)
	}

	t.writeMu.Lock()

	for t.connectionSendQueueLocal.Len() > 0 {
		conn := t.connectionSendQueueLocal.PopFront()
		if conn.GetFlag(closedFlag) {
			continue
		}
		t.addConnectionToSendQueueLocked(conn)
	}

	needToSignalGoResend = false
	for t.resendTimersLocal.Len() > 0 {
		conn := t.resendTimersLocal.PopFront()
		if conn.GetFlag(closedFlag) {
			continue
		}

		if t.resendTimers.Empty() || conn.resendTimeMs < t.resendTimers.Min().resendTimeMs {
			needToSignalGoResend = true
		}
		conn.resendTimeMs = time.Now().UnixMilli() + conn.resendTimeout
		t.resendTimers.Add(conn)
		conn.SetFlag(inResendQueueFlag, true)
	}

	for t.resendRequestTimersLocal.Len() > 0 {
		conn := t.resendRequestTimersLocal.PopFront()
		if conn.GetFlag(closedFlag) {
			continue
		}

		// start resend request timer if have holes
		if conn.acks.HaveHoles() && !conn.GetFlag(inResendRequestQueueFlag) {
			conn.resendRequestTimeMs = time.Now().UnixMilli() + conn.resendRequestTimeout

			if t.resendRequestTimers.Empty() || conn.resendRequestTimeMs < t.resendRequestTimers.Min().resendRequestTimeMs {
				needToSignalGoResendRequest = true
			}
			t.resendRequestTimers.Add(conn)
			conn.SetFlag(inResendRequestQueueFlag, true)

			conn.resendRequestTimeout = min(MaxResendTimeoutMillis, int64(float64(conn.resendRequestTimeout)*ResendTimeoutCoef))
		}
	}

	for t.unreliableMessages.Len() > 0 && (t.prevWasReliable || t.connectionSendQueue.Len() == 0) {
		cm := t.unreliableMessages.PopFront()
		conn := cm.conn
		if conn.GetFlag(closedFlag) {
			continue
		}

		if conn.GetFlag(inAckQueueFlag) {
			conn.SetFlag(ackSentFlag, true)
		}

		t.prevWasReliable = false

		// these fields must be read only under t.writeMu (except for goRead())
		status := conn.status
		generation := conn.generation
		remotePid := conn.remotePid()
		hash := conn.id.Hash
		oldCipher := status == ConnectionStatusWaitingForRemotePid || status == ConnectionSentObsoleteHash
		writeCipher := conn.writeNew
		t.writeMu.Unlock()
		if oldCipher {
			_, writeCipher = t.generateCryptoKeysOld(conn.keyID, conn.localPid(), remotePid, generation)
		}

		datagram, _ = t.buildDatagram(conn, writeCipher, generation, status, remotePid, hash, false, cm.message)

		return datagram, conn, false, false, needToSignalGoResend, needToSignalGoResendRequest
	}
	for t.connectionSendQueue.Len() > 0 {
		conn := t.popConnectionFromSendQueueLocked()
		if conn.GetFlag(closedFlag) {
			continue
		}

		t.prevWasReliable = true

		withoutPayload := false
		if conn.GetFlag(forceResendRequestFlag) {
			withoutPayload = true
		}
		if conn.GetFlag(inAckQueueFlag) {
			conn.SetFlag(ackSentFlag, true)
		}

		// these fields must be read only under t.writeMu (except for goRead())
		status := conn.status
		generation := conn.generation
		remotePid := conn.remotePid()
		if status == ConnectionSentObsoleteHash {
			remotePid = tlnet.Pid{}
		}
		hash := conn.id.Hash
		// TODO think what crypto use when send
		// ConnectionSentObsoletePid
		// and ConnectionSentObsoleteGeneration
		oldCipher := status == ConnectionStatusWaitingForRemotePid || status == ConnectionSentObsoleteHash
		writeCipher := conn.writeNew
		t.writeMu.Unlock()
		if oldCipher {
			_, writeCipher = t.generateCryptoKeysOld(conn.keyID, conn.localPid(), remotePid, generation)
		}

		var haveUserPayload bool
		datagram, haveUserPayload = t.buildDatagram(conn, writeCipher, generation, status, remotePid, hash, withoutPayload, nil)

		return datagram, conn, haveUserPayload && !conn.GetFlag(inResendQueueFlag), haveUserPayload && !conn.GetFlag(inRegenerateQueue), needToSignalGoResend, needToSignalGoResendRequest
	}

	// no datagram to send and timer to run
	return nil, nil, false, false, needToSignalGoResend, needToSignalGoResendRequest
}

func (t *Transport) handleAck(conn *Connection, ack *tlnetUdpPacket.EncHeader) {
	if ack.IsSetPacketAckPrefix() {
		err := conn.outgoing.AckPrefix(t, ack.PacketAckPrefix+1)
		if err != nil {
			log.Printf("goWrite: error from AckPrefix(): %s", err)
		}
	}

	if ack.IsSetPacketAckFrom() {
		for ackSeqNo := ack.PacketAckFrom; ackSeqNo <= ack.PacketAckTo; ackSeqNo++ {
			err := conn.outgoing.AckChunk(t, ackSeqNo)
			if err != nil {
				log.Printf("goWrite: error from AckChunk(): %s", err)
			}
		}
	}

	if ack.IsSetPacketAckSet() {
		for _, ackSeqNo := range ack.PacketAckSet {
			err := conn.outgoing.AckChunk(t, ackSeqNo)
			if err != nil {
				log.Printf("goWrite: error from AckChunk(): %s", err)
			}
		}
	}

	// TODO set flowControl
}

// just copypasted constants from engines udp code
const CryptoFlagModeAes uint32 = 1 << 0
const CryptoFlagHandshake uint32 = 1 << 7
const CryptoKeyIdMask uint32 = 0xfff
const CryptoKeyIdShift uint32 = 8

const ObsoleteGenerationPayloadSize = 4 + 12 + 4
const ObsoleteHashPayloadSize = 4 + 8 + 12

func (t *Transport) buildDatagram(
	conn *Connection, writeCipher cipher.Block, generation uint32, status ConnectionStatus,
	remotePid tlnet.Pid, hash int64, withoutPayload bool, unreliablePayload *[]byte,
) ([]byte, bool) {
	var unenc tlnetUdpPacket.UnencHeader
	var enc tlnetUdpPacket.EncHeader
	// TODO reuse !!!
	var msgToDealloc *[]byte
	var haveUserPayload = false

	// TODO: Think about new connection queue model: do not have connections in one queue, but have chunk queue,
	// to easily add obsoletePid/Hash/Generation and handshake instead of setting status and pattern matching it
	// and to reduce contention on Connection status/pid/generation locks and unlocks

	localPid := conn.localPid()
	t.writeChunks = t.writeChunks[:0]
	if unreliablePayload != nil {
		t.buildPayloadDatagram(conn, &enc, true)
		setUnreliableEncFlags(&enc)
		t.writeChunks = algo.ResizeSlice(t.writeChunks, 1)
		t.writeChunks[0] = *unreliablePayload
		msgToDealloc = unreliablePayload

		t.stats.UnreliableMessagesSent.Add(1)
	} else if status == ConnectionSentObsoleteGeneration {
		payload := make([]byte, 0, ObsoleteGenerationPayloadSize)
		payload = basictl.NatWrite(payload, tlnetUdpPacket.ObsoleteGeneration{}.TLTag())
		payload = localPid.Write(payload)
		payload = basictl.NatWrite(payload, generation)

		t.writeChunks = append(t.writeChunks, payload)

		setUnreliableEncFlags(&enc)

		t.stats.ObsoleteGenerationSent.Add(1)
	} else if status == ConnectionSentObsoleteHash {
		payload := make([]byte, 0, ObsoleteHashPayloadSize)
		payload = basictl.NatWrite(payload, tlnetUdpPacket.ObsoleteHash{}.TLTag())
		payload = basictl.LongWrite(payload, conn.receivedObsoleteHash)
		payload = localPid.Write(payload)

		t.writeChunks = append(t.writeChunks, payload)

		setUnreliableEncFlags(&enc)

		t.stats.ObsoleteHashSent.Add(1)
	} else if conn.GetFlag(forceResendRequestFlag) {
		conn.SetFlag(forceResendRequestFlag, false)
		var req tlnetUdpPacket.ResendRequest
		t.buildResendRequestDatagram(conn, &enc, &req)
		t.writeChunks = algo.ResizeSlice(t.writeChunks, 1)
		t.writeChunks[0] = req.WriteBoxed(t.writeChunks[0])

		t.stats.ResendRequestSent.Add(1)
	} else {
		haveUserPayload = t.buildPayloadDatagram(conn, &enc, withoutPayload)
	}

	// common unenc and enc flags for every datagram

	// Unencrypted header
	if status == ConnectionStatusEstablished {
		unenc.SetPidHash(hash)
	} else {
		unenc.SetLocalPid(localPid)
		unenc.SetRemotePid(remotePid)
		unenc.SetGeneration(generation)
	}
	cryptoFlags := CryptoFlagHandshake | CryptoFlagModeAes
	cryptoFlags |= (binary.LittleEndian.Uint32(conn.keyID[:]) & CryptoKeyIdMask) << CryptoKeyIdShift
	unenc.SetCryptoFlags(cryptoFlags)
	unenc.SetCryptoSha(true)
	nonce := uint64(conn.randomKey[0]) + (uint64(conn.randomKey[1]) << 32)
	nonce++
	conn.randomKey[0] = uint32(nonce)
	conn.randomKey[1] = uint32(nonce >> 32)
	unenc.SetCryptoRandom(conn.randomKey)
	unenc.SetEncryptedData(true)

	// Encrypted header
	enc.Flags |= unenc.Flags & EncryptedFlagsMask // some bullshit from C++ code
	enc.SetVersion(TransportVersion | TransportVersion<<16)

	bytes := t.serializeDatagram(writeCipher, unenc, enc, msgToDealloc)
	return bytes, haveUserPayload
}

func setUnreliableEncFlags(enc *tlnetUdpPacket.EncHeader) {
	enc.SetPacketNum(^uint32(0))
	enc.SetSingleRpcMsg(true)
	enc.SetMultipleRpcMsgs(false)
}

func (t *Transport) buildResendRequestDatagram(conn *Connection, enc *tlnetUdpPacket.EncHeader, req *tlnetUdpPacket.ResendRequest) {
	conn.acks.BuildAck(enc) // TODO set window control!!!
	conn.acks.BuildNegativeAck(req)
	setUnreliableEncFlags(enc)
}

func (t *Transport) buildPayloadDatagram(conn *Connection, enc *tlnetUdpPacket.EncHeader, withoutPayload bool) (haveUserPayload bool) {
	conn.acks.BuildAck(enc) // TODO set window control!!!
	if withoutPayload {
		return false
	}

	var singleMessage bool
	var firstSeqNum uint32
	t.writeChunks = t.writeChunks[:0]
	t.writeChunks, firstSeqNum, singleMessage = conn.outgoing.GetChunksToSend(t, t.writeChunks)
	haveUserPayload = len(t.writeChunks) > 0

	// Encrypted header
	if haveUserPayload {
		if firstSeqNum == ^uint32(0) {
			setUnreliableEncFlags(enc)
		} else {
			lastSeqNum := firstSeqNum + uint32(len(t.writeChunks)) - 1
			firstChunkRef, _ := conn.outgoing.window.Get(firstSeqNum)
			lastChunkRef, _ := conn.outgoing.window.Get(lastSeqNum)
			if firstSeqNum == lastSeqNum {
				enc.SetPacketNum(firstSeqNum)
			} else {
				enc.SetPacketsFrom(firstSeqNum)
				enc.SetPacketsCount(lastSeqNum - firstSeqNum + 1)
			}
			enc.SetPrevParts(firstChunkRef.prevParts)
			enc.SetNextParts(lastChunkRef.nextParts())
			enc.SetPacketOffset(firstChunkRef.offset())
			enc.SetPrevLength(firstChunkRef.prevOffset())
			enc.SetNextLength(lastChunkRef.nextOffset())
			enc.SetSingleRpcMsg(singleMessage)
			enc.SetMultipleRpcMsgs(!singleMessage)
		}
	}
	return
}

func (t *Transport) serializeDatagram(
	writeCipher cipher.Block,
	unenc tlnetUdpPacket.UnencHeader,
	enc tlnetUdpPacket.EncHeader,
	msgToDealloc *[]byte,
) []byte {
	t.writeBuffer = t.writeBuffer[:0]
	t.writeEncryptedBuffer = t.writeEncryptedBuffer[:0]

	t.writeBuffer = enc.Write(t.writeBuffer)
	for i, chunk := range t.writeChunks {
		if len(t.writeChunks) > 1 {
			t.writeBuffer = basictl.NatWrite(t.writeBuffer, uint32(len(chunk)))
		}
		t.writeBuffer = append(t.writeBuffer, chunk...)
		if msgToDealloc != nil {
			*msgToDealloc = (*msgToDealloc)[:0]
			t.messageDeallocator(msgToDealloc)
		}
		msgToDealloc = nil
		t.writeChunks[i] = nil
	}

	padding := 0
	bytes := len(t.writeBuffer) + encryptedUnencHeaderSize(unenc)
	if bytes%2 != 0 {
		padding += 1
		enc.SetZeroPadding1Byte(true)
		bytes += 1
	}
	if bytes%4 != 0 {
		padding += 2
		enc.SetZeroPadding2Bytes(true)
		bytes += 2
	}
	if bytes%8 != 0 {
		padding += 4
		enc.SetZeroPadding4Bytes(true)
		bytes += 4
	}
	if bytes%16 != 0 {
		padding += 8
		enc.SetZeroPadding8Bytes(true)
	}
	// just to add padding flags to serialized field mask
	basictl.NatWrite(t.writeBuffer[:0], enc.Flags)
	// padding
	t.writeBuffer = algo.ResizeSlice(t.writeBuffer, len(t.writeBuffer)+padding)
	for i := len(t.writeBuffer) - padding; i < len(t.writeBuffer); i++ {
		t.writeBuffer[i] = 0
	}

	t.writeBuffer = writeEncryptedUnencHeader(t.writeBuffer, unenc)

	// Unencrypted header
	t.writeEncryptedBuffer = unenc.WriteBoxed(t.writeEncryptedBuffer)

	// Unencrypted header + encrypted part
	unencHeaderSize := len(t.writeEncryptedBuffer)
	t.writeEncryptedBuffer = algo.ResizeSlice(t.writeEncryptedBuffer, len(t.writeEncryptedBuffer)+len(t.writeBuffer))

	var ivb [32]byte
	CopyIVTo(&ivb, unenc.CryptoRandom)
	ige.EncryptBlocks(writeCipher, ivb[:], t.writeEncryptedBuffer[unencHeaderSize:], t.writeBuffer)

	t.writeEncryptedBuffer = basictl.NatWrite(t.writeEncryptedBuffer, crc32.Checksum(t.writeEncryptedBuffer, castagnoliTable))

	return t.writeEncryptedBuffer
}

func writeEncryptedUnencHeader(payload []byte, unenc tlnetUdpPacket.UnencHeader) []byte {
	if unenc.IsSetPidHash() {
		payload = basictl.LongWrite(payload, unenc.PidHash)
	}
	if unenc.IsSetLocalPid() {
		payload = unenc.LocalPid.Write(payload)
		payload = unenc.RemotePid.Write(payload)
		payload = basictl.NatWrite(payload, unenc.Generation)
	}
	if unenc.IsSetCryptoFlags() {
		payload = basictl.NatWrite(payload, unenc.CryptoFlags)
	}
	return payload
}

func (t *Transport) goResend() {
	defer func() {
		t.wg.Done()
	}()

	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	timerStarted := false

	for {
		t.writeMu.Lock()
		if t.closed {
			// transport is closed
			t.writeMu.Unlock()
			return
		}

		now := time.Now().UnixMilli()
		wasBurntTimers := false
		var fastestConn *Connection
		var fastestConnMs int64

		for t.resendTimers.Len() > 0 {
			conn := t.resendTimers.ExtractMin()
			if conn.resendTimeMs <= now {
				if conn.GetFlag(closedFlag) {
					// this connection is already closed
					continue
				}
				// send timeout action to goWrite
				t.newResends.PushBack(conn)
				wasBurntTimers = true
				continue
			}
			// else resend timeout hasn't expired yet - so need to run timer
			fastestConn = conn
			break
		}
		if fastestConn != nil {
			// we will check it again after select emits
			t.resendTimers.Add(fastestConn)
			fastestConnMs = fastestConn.resendTimeMs
		}

		t.writeMu.Unlock()
		if wasBurntTimers {
			t.writeCV.Signal()
		}

		if fastestConn != nil {
			durationMs := fastestConnMs - time.Now().UnixMilli()
			if durationMs <= 0 {
				continue
			}
			if timerStarted && !timer.Stop() {
				<-timer.C
			}
			timer.Reset(time.Millisecond * time.Duration(durationMs))
			timerStarted = true
		}

		select {
		case <-t.resendCV:
			// either new timer, that less than previous min, or transport is closed
			continue
		case <-timer.C:
			timerStarted = false
			continue
		}
	}
}

func (t *Transport) goAck() {
	t.writeMu.Lock()

	defer func() {
		t.writeMu.Unlock()
		t.wg.Done()
	}()

	for {
		for t.ackTimers.Len() == 0 && !t.closed {
			t.ackCV.Wait()
		}
		if t.closed {
			break
		}
		conn := t.ackTimers.PopFront()
		if conn.GetFlag(closedFlag) {
			continue
		}

		// TODO replace 2 stupid queues (ackTimers and newAcksSnds) with simply one (ackTimers)
		// idea:
		// goResend() reads (without deleting) first queue element, waits for timer to burn,
		// then increments one integer index in Transport, signalling goWrite().
		// goWrite() reads this index and if it was incremented,
		// consider the first element of resendTimers as new event, handles and deletes it.
		// UPD: works only for equal timeout durations

		durationMs := conn.ackTimeMs - time.Now().UnixMilli()
		if durationMs > 0 {
			// to not hold mutex for the long time waiting for the timeout
			t.writeMu.Unlock()
			time.Sleep(time.Millisecond * time.Duration(durationMs))
			t.writeMu.Lock()
		}

		conn.SetFlag(inAckQueueFlag, false)
		// send force ack action to goWrite
		t.newAckSnds.PushBack(conn)
		t.writeCV.Signal()
	}
}

func (t *Transport) goResendRequest() {
	defer func() {
		t.wg.Done()
	}()

	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	timerStarted := false

	for {
		t.writeMu.Lock()
		if t.closed {
			// transport is closed
			t.writeMu.Unlock()
			return
		}

		now := time.Now().UnixMilli()
		wasBurntTimers := false
		var fastestConn *Connection
		var fastestConnMs int64

		for t.resendRequestTimers.Len() > 0 {
			conn := t.resendRequestTimers.ExtractMin()
			if conn.resendRequestTimeMs <= now {
				if conn.GetFlag(closedFlag) {
					// this connection is already closed
					continue
				}
				// send timeout action to goWrite
				conn.SetFlag(inResendRequestQueueFlag, false)
				t.newResendRequestSnds.PushBack(conn)
				wasBurntTimers = true
				continue
			}
			// else resend request timeout hasn't expired yet - so need to run timer
			fastestConn = conn
			break
		}
		if fastestConn != nil {
			// we will check it again after select emits
			t.resendRequestTimers.Add(fastestConn)
			fastestConnMs = fastestConn.resendRequestTimeMs
		}

		t.writeMu.Unlock()
		if wasBurntTimers {
			t.writeCV.Signal()
		}

		if fastestConn != nil {
			durationMs := fastestConnMs - time.Now().UnixMilli()
			if durationMs <= 0 {
				continue
			}
			if timerStarted && !timer.Stop() {
				<-timer.C
			}
			timer.Reset(time.Millisecond * time.Duration(durationMs))
			timerStarted = true
		}

		select {
		case <-t.resendRequestCV:
			// either new timer, that less than previous min, or transport is closed
			continue
		case <-timer.C:
			timerStarted = false
			continue
		}
	}
}

func (t *Transport) goRegenerate() {
	t.writeMu.Lock()

	defer func() {
		t.writeMu.Unlock()
		t.wg.Done()
	}()

	for {
		for t.regenerateTimers.Len() == 0 && !t.closed {
			t.regenerateCV.Wait()
		}
		if t.closed {
			break
		}
		conn := t.regenerateTimers.PopFront()

		durationMs := conn.regenerateTimeMs - time.Now().UnixMilli()
		if durationMs > 0 {
			// to not hold mutex for the long time waiting for the timeout
			t.writeMu.Unlock()
			time.Sleep(time.Millisecond * time.Duration(durationMs))
			t.writeMu.Lock()
		}

		conn.SetFlag(inRegenerateQueue, false)
		// send clean up action to goRead
		t.newGoReadRegenerates.PushBack(conn)
		t.writeCV.Signal()
	}
}

/********** METHODS FOR INCOMING CONNECTIONS MEMORY MANAGEMENT **********/

// No synchronization is needed for t.memoryWaiters, t.acquiredMemory,
// because all methods are called only from goRead goroutine
func (t *Transport) checkMemoryWaiters() {
	for t.memoryWaiters.Len() > 0 {
		firstWaiter := t.memoryWaiters.Front()
		if !t.tryAcquireMemoryForTheFirst(firstWaiter) {
			return
		}
		firstWaiter.incoming.OnAcquiredMemory()
		if t.debugUdpRPC >= 2 {
			log.Printf("udp connection %s returned from incoming memory waiters queue", firstWaiter.remoteAddr().String())
		}
	}
}

func (t *Transport) tryAcquireMemoryForTheFirst(first *Connection) bool {
	newAcquiredMemory := t.acquiredMemory + first.incoming.requestedMemorySize
	if newAcquiredMemory <= t.incomingMessagesMemoryLimit {
		t.acquiredMemory = newAcquiredMemory
		t.stats.AcquiredMemory.Store(t.acquiredMemory)
		t.memoryWaiters.PopFront()
		t.stats.MemoryWaitersSize.Store(int64(t.memoryWaiters.Len()))
		first.incoming.inMemoryWaitersQueue = false
		if t.debugUdpRPC >= 2 {
			log.Printf("udp memory acquired %d bytes", first.incoming.requestedMemorySize)
		}
		// calling function must notice or answer connection
		return true
	}
	return false
}

func (t *Transport) tryAcquireMemory(conn *Connection) bool {
	if !conn.incoming.inMemoryWaitersQueue {
		t.memoryWaiters.PushBack(conn)
		t.stats.MemoryWaitersSize.Store(int64(t.memoryWaiters.Len()))
		conn.incoming.inMemoryWaitersQueue = true
	} // else connection reduced his request

	firstWaiter := t.memoryWaiters.Front()
	if firstWaiter == conn {
		// this connection was already the first or waiters queue was empty
		acquired := t.tryAcquireMemoryForTheFirst(conn)
		if acquired {
			t.checkMemoryWaiters()
			return true
		}
	}
	if t.debugUdpRPC >= 2 {
		log.Printf("udp connection %s stuck in incoming memory waiters queue...", firstWaiter.remoteAddr().String())
	}
	return false
}

// TODO make releaseMemory public method, which is called, when higher level protocol releases message pointer
// (for example, when server calls rpc
func (t *Transport) releaseMemory(releasedSize int64) {
	if t.acquiredMemory-releasedSize < 0 {
		panic(fmt.Sprintf("released %d but was acquired only %d bytes", releasedSize, t.acquiredMemory))
	}
	if t.debugUdpRPC >= 2 {
		log.Printf("udp memory releases %d bytes", releasedSize)
	}
	t.acquiredMemory -= releasedSize
	t.stats.AcquiredMemory.Store(t.acquiredMemory)
	t.checkMemoryWaiters()
}

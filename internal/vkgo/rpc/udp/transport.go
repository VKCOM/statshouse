// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"crypto/aes"
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
	"time"

	"github.com/gotd/ige"

	"github.com/VKCOM/statshouse/internal/vkgo/algo"
	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tlnet"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tlnetUdpPacket"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

const MinCryptoKeyLen = 32

// DefaultResendTimeout TODO make resend timeout adaptive value
const DefaultResendTimeout = time.Millisecond * 10
const DefaultMaxWindowSize = 1000

const TransportVersion = 2

type ConnectionID struct {
	IP   uint32 // Actual remote address. Motivation - prevent collisions of hash between clients
	Port uint16 // Actual remote port. Motivation - prevent collisions of hash between clients
	Hash int64  // common connection identifier, defined in VK UDP Protocol. TODO - each side must be able to select its own hash
}

type ConnectionStatus uint8

const (
	ConnectionStatusClosed ConnectionStatus = iota
	ConnectionStatusWaitingForRemotePid
	ConnectionStatusWaitingForHash
	ConnectionStatusEstablished
	ConnectionSentObsoletePid
	ConnectionSentObsoleteHash
	ConnectionSentObsoleteGeneration
)

// MessageHandler is called when full message is received by transport.
// message - pointer on slice with received message
// canSave - whether handler owns message pointer and can do with it whatever it wants.
// canSave = false means handler can only read message and must copy it in case when it wants to use it after return.
type MessageHandler func(message *[]byte, canSave bool)

// Connection - Every Connection is created
// 1) either in Transport::ConnectTo() method and has fields MessageHandle and StreamLikeIncoming set by method parameters,
// 2) or in Transport::goRead() goroutine after datagram receive from the other endpoint.
// In second case fields MessageHandle and StreamLikeIncoming must be set by Transport::acceptHandler,
// obtained in NewTransport() method.
type Connection struct {
	id     ConnectionID
	status ConnectionStatus

	outgoing OutgoingConnection // outgoing messages, accessed mainly by Transport::goRead
	acks     AcksToSend         // data structure to send acks, accessed only by Transport::goWrite
	incoming IncomingConnection // incoming messages, accessed only by Transport::goWrite

	MessageHandle      MessageHandler
	StreamLikeIncoming bool
	UserData           any

	remotePid     tlnet.Pid
	generation    uint32
	remoteAddr    netip.AddrPort
	localUdpAddr  net.UDPAddr
	remoteUdpAddr net.UDPAddr
	localIp       uint32
	closed        bool

	randomKey [8]uint32
	nonce     uint64
	// readOld and writeOld ciphers are derived from full client pid and partial server pid
	readOld  cipher.Block
	writeOld cipher.Block
	// readOld and writeOld ciphers are derived from full client and server pids
	readNew  cipher.Block
	writeNew cipher.Block
	keyID    [4]byte

	inSendQueue bool

	inResendQueue bool
	resendTime    time.Time

	inAckQueue bool
	ackTime    time.Time
	ackSent    bool // acks were sent with payload before timer burned - so no need to send acks again at this time

	inResendRequestQueue bool
	resendRequestTime    time.Time
	forceResendRequest   bool

	receivedObsoletePid  tlnet.Pid
	receivedObsoleteHash int64
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
	if len(*message)%4 != 0 {
		return fmt.Errorf("chunks length must be a multiple of 4")
	}

	c.outgoing.transport.writeMu.Lock()
	c.outgoing.transport.newMessages.PushBack(ConnectionMessage{
		conn:       c,
		message:    message,
		unreliable: unreliable,
	})
	c.outgoing.transport.writeMu.Unlock()
	c.outgoing.transport.writeCV.Signal()
	return nil
}

func (c *Connection) LocalAddr() net.Addr {
	return &c.localUdpAddr
}

func (c *Connection) RemoteAddr() net.Addr {
	return &c.remoteUdpAddr
}

func (c *Connection) KeyID() [4]byte {
	return c.keyID
}

func (c *Connection) localPid() tlnet.Pid {
	pid := c.incoming.transport.localPid
	pid.Ip = c.localIp
	return pid
}

func getIpUint32(addr netip.AddrPort) uint32 {
	ipBytes := addr.Addr().As4()
	return binary.BigEndian.Uint32(ipBytes[:])
}

func ipPortFromAddr(addr netip.AddrPort) (ip uint32, port uint16) {
	return getIpUint32(addr), addr.Port()
}

// TODO move to util
func ipPortFromNetPid(pid tlnet.Pid) (ip uint32, port uint16) {
	return pid.Ip, uint16(pid.PortPid & ((1 << 16) - 1))
}

// TODO normal naming !!!!!!!!!!!!!
type ConnectionMessage struct {
	conn       *Connection
	message    *[]byte
	unreliable bool
}

type ConnResendRequest struct {
	conn *Connection
	req  tlnetUdpPacket.ResendRequest
}

type ConnectionHeader struct {
	conn *Connection
	enc  tlnetUdpPacket.EncHeader
}

type ConnAcquiredChunk struct {
	conn   *Connection
	seqNum uint32
}

type Transport struct {
	// accessed from goRead and ConnectTo
	handshakeByPidMu sync.RWMutex
	handshakeByPid   map[ConnectionID]*Connection // here all CID have zero Hash
	shutdown         bool
	debugUdpRPC      int // 0 - nothing; 1 - prints key udp events; 2 - prints all udp activities (<0 equals to 0; >2 equals to 2)

	// accessed only from goRead
	connectionById map[ConnectionID]*Connection

	// goWrite waits for changes in newMessages, newAckRcvs, newAckSnds and newResends, moves them to local copies
	// and updates state of OutgoingConnection exclusively without locking
	writeMu               sync.Mutex
	writeCV               *sync.Cond // signaled when newMessages, newHdrRcvs, newAckSnds or newResends has new value
	newMessages           algo.CircularSlice[ConnectionMessage]
	newHdrRcvs            algo.CircularSlice[ConnectionHeader] // goWrite use it to update Connection::AcksToSend and Connection::OutgoingConnection
	newResends            algo.CircularSlice[*Connection]
	newAckSnds            algo.CircularSlice[*Connection]
	newResendRequestsRcvs algo.CircularSlice[ConnResendRequest]
	newResendRequestSnds  algo.CircularSlice[*Connection]
	connectionSendQueue   algo.CircularSlice[*Connection]

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

	// filled in checkMemoryWaiters()
	acquiredMemoryEvents []ConnAcquiredChunk

	resendTimeout time.Duration
	resendCV      *sync.Cond // signaled from goWrite when resendTimers has new value
	resendTimers  algo.CircularSlice[*Connection]

	ackTimeout time.Duration
	ackCV      *sync.Cond // signaled from goRead when ackTimers has new value
	ackTimers  algo.CircularSlice[*Connection]

	resendRequestTimeout time.Duration
	resendRequestCV      *sync.Cond // signaled from goRead when resendRequestTimers has new value
	resendRequestTimers  algo.CircularSlice[*Connection]

	socket   *net.UDPConn
	localPid tlnet.Pid // if IP is 0.0.0.0, then Transport forbids ConnectTo() and accepted connection will have Connection::localIp equal to dstIp of received datagram
	closed   bool

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
	memoryWaiters               algo.CircularSlice[*IncomingConnection]

	writeChunks  [][]byte
	messagesPool []*OutgoingMessage

	// encrypting and decrypting
	cryptoKeys           map[[4]byte]string
	readByteCache        []byte
	writeBuffer          []byte
	writeEncryptedBuffer []byte
}

func NewTransport(
	incomingMessagesMemoryLimit int64,
	cryptoKeys []string,
	socket *net.UDPConn,
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
	if resendTimeout == 0 {
		resendTimeout = DefaultResendTimeout
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
		resendTimeout:               resendTimeout,
		ackTimeout:                  resendTimeout / 2,
		resendRequestTimeout:        resendTimeout / 2,
		acceptHandler:               acceptHandler,
		closeHandler:                closeHandler,
		messageAllocator:            messageAllocator,
		messageDeallocator:          messageDeallocator,
		incomingMessagesMemoryLimit: incomingMessagesMemoryLimit,
		socket:                      socket,
		cryptoKeys:                  make(map[[4]byte]string),
	}
	t.writeCV = sync.NewCond(&t.writeMu)
	t.resendCV = sync.NewCond(&t.writeMu)
	t.ackCV = sync.NewCond(&t.writeMu)
	t.resendRequestCV = sync.NewCond(&t.writeMu)

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
	strAddr := t.socket.LocalAddr().(*net.UDPAddr).String()
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
	t.wg.Add(5)

	go t.goWrite()
	go t.goResend()
	go t.goAck()
	go t.goResendRequest()
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

func (t *Transport) Close() error {
	t.writeMu.Lock()
	t.closed = true
	t.writeMu.Unlock()
	err := t.socket.Close()
	t.writeCV.Signal()
	t.resendCV.Signal()
	t.ackCV.Signal()
	t.resendRequestCV.Signal()
	return err
}

func (t *Transport) setStatusAndAddConnectionToSendQueueUnlocked(conn *Connection, status ConnectionStatus) {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	conn.status = status
	t.addConnectionToSendQueueLocked(conn)
}

func (t *Transport) addConnectionToSendQueueLocked(conn *Connection) {
	if !conn.inSendQueue {
		// TODO maybe allow several instances of connection in send queue?
		t.connectionSendQueue.PushBack(conn)
		conn.inSendQueue = true
	}
}

func (t *Transport) addConnectionToSendQueueUnlocked(conn *Connection) {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	t.addConnectionToSendQueueLocked(conn)
}

func (t *Transport) popConnectionFromSendQueueLocked() *Connection {
	conn := t.connectionSendQueue.PopFront()
	conn.inSendQueue = false
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
	conn = t.createConnection(ConnectionStatusWaitingForRemotePid, t.localPid.Ip, addr, tlnet.Pid{Ip: ip, PortPid: uint32(port)}, 0, false)
	conn.MessageHandle = incomingMessageHandle
	conn.StreamLikeIncoming = streamLikeIncoming
	conn.UserData = userData

	for keyID := range t.cryptoKeys {
		t.generateCryptoKeysOld(keyID, conn, t.localPid, conn.remotePid, 0)
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

	// TODO why do we add connection to send queue? Just to send empty handshake? Do we need it or lazy handshake is better?
	t.addConnectionToSendQueueUnlocked(conn)
	t.writeCV.Signal()

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
		// TODO process n bytes in case when err != nil and n > 0 !!!
		n, addr, err := t.socket.ReadFromUDPAddrPort(buf)
		if t.debugUdpRPC >= 2 {
			log.Printf("goRead() <- udp datagram from %s", addr.String())
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
				log.Printf("%v goRead() closed", time.Now().String())
				return
			}
			log.Printf("[ <- goRead ] network error: %v", err) // TODO - rare log. Happens when attaching or detaching adapters.
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
				conn.closed = true
				// TODO must do nothing and just continue??
			}

			startedAckTimer, startedResendRequestTimer := t.GoReadHandleEncHdr(conn, enc)

			if len(t.acquiredMemoryEvents) > 0 {
				for _, e := range t.acquiredMemoryEvents {
					var encHdrAcquired tlnetUdpPacket.EncHeader
					encHdrAcquired.SetPacketNum(e.seqNum)

					startedAckTimerAcquired, startedResendRequestTimerAcquired := t.GoReadHandleEncHdr(e.conn, encHdrAcquired)
					startedAckTimer = startedAckTimer || startedAckTimerAcquired
					startedResendRequestTimer = startedAckTimer || startedResendRequestTimerAcquired
				}
				t.acquiredMemoryEvents = t.acquiredMemoryEvents[:0]
			}

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
			if startedResendRequestTimer {
				t.resendRequestCV.Signal()
			}
		}
	}
}

func (t *Transport) GoReadHandleEncHdr(conn *Connection, enc tlnetUdpPacket.EncHeader) (startedAckTimer bool, startedResendRequestTimer bool) {
	t.newHdrRcvs.PushBack(ConnectionHeader{
		conn: conn,
		enc:  enc,
	})

	packetHasData := enc.IsSetPacketNum() || enc.IsSetPacketsFrom()
	if packetHasData {
		// start resend request timer if have holes
		if conn.incoming.haveHoles() && !conn.inResendRequestQueue {
			startedResendRequestTimer = true
			conn.resendRequestTime = time.Now().Add(t.ackTimeout)

			t.resendRequestTimers.PushBack(conn)
			conn.inResendRequestQueue = true
			t.resendRequestCV.Signal()
		}
	}
	startedAckTimer = packetHasData && !conn.inAckQueue && !conn.inSendQueue
	if startedAckTimer {
		// when conn.inAckQueue - ack timer is already run
		// when conn.inSendQueue - we will send acks with our own payload in near time

		t.newAckSnds.PushBack(conn)

		// We do not call
		// if !conn.ackTimer.Stop() {
		//         <-conn.ackTimer.C
		// }
		// because we have synchronization point with goAck() goroutine via conn.inAckQueue flag

		// TODO return
		conn.ackTime = time.Now().Add(t.ackTimeout)

		t.ackTimers.PushBack(conn)
		conn.inAckQueue = true
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
		conn = t.createConnection(ConnectionSentObsoleteHash, localIp, addr, tlnet.Pid{}, 0, true)
		conn.receivedObsoleteHash = unenc.PidHash

		for keyID := range t.cryptoKeys {
			t.generateCryptoKeysOld(keyID, conn, conn.localPid(), conn.remotePid, unenc.Generation)
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
		conn = t.createConnection(ConnectionStatusWaitingForHash, localIp, addr, unenc.LocalPid, unenc.Generation, false)
		t.generateCryptoKeysOld(getKeyID(unenc.CryptoFlags), conn, unenc.RemotePid, unenc.LocalPid, unenc.Generation)

		created = true
	}

	if conn.remotePid.Utime != 0 && (conn.remotePid.PortPid>>16) != 0 && conn.remotePid != unenc.LocalPid {
		// known remote pid and actual datagram pid mismatch

		if unenc.LocalPid.Utime < conn.remotePid.Utime {
			// old handshake
			return nil, false, nil
		}

		// Remote side has renewed its pid because of restart or close
		if conn.id.Hash != 0 {
			delete(t.connectionById, conn.id)
		}

		t.handshakeByPidMu.Lock()
		delete(t.handshakeByPid, handshakeCid)
		t.handshakeByPidMu.Unlock()
		t.closeHandler(conn)
		return conn, true, nil
	}

	localPid := conn.localPid()
	if (unenc.RemotePid.Utime != 0 || unenc.RemotePid.PortPid>>16 != 0) && unenc.RemotePid != localPid {
		// That side knows our old pid, or it is old message
		// We do not send obsolete pid, because other side will eventually receive our actual pid
		return nil, false, fmt.Errorf("received datagram with incorrect remote pid (received %+v but actual local pid is %+v) from udp connection %s", unenc.RemotePid, conn.localPid(), conn.remoteAddr.String())
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
		}

		t.handshakeByPidMu.Lock()
		delete(t.handshakeByPid, handshakeCid)
		t.handshakeByPidMu.Unlock()
		t.closeHandler(conn)
		return nil, true, nil
	}

	if conn.status == ConnectionStatusWaitingForRemotePid || created {
		// This code is both for server just received new handshake
		// and for client received answer handshake from server.
		// We need to generate full crypto keys and calculate session hash

		remotePid := unenc.LocalPid
		connectionHash := int64(CalcHash(&localPid, &remotePid, unenc.Generation))
		if t.debugUdpRPC >= 2 {
			log.Printf("calculated hash(local(%s) remote(%s) generation(%d)) => %d for %s", localPid, remotePid, unenc.Generation, connectionHash, conn.remoteAddr.String())
		}
		cid := ConnectionID{IP: ip, Port: port, Hash: connectionHash}
		conn.id = cid
		t.connectionById[cid] = conn

		// for next datagram we must generate new keys
		if _, keyExists := t.cryptoKeys[getKeyID(unenc.CryptoFlags)]; !keyExists {
			return nil, false, fmt.Errorf("unknown key ID %v", getKeyID(unenc.CryptoFlags))
		}
		t.generateCryptoKeysNew(t.cryptoKeys[getKeyID(unenc.CryptoFlags)], conn, localPid, remotePid, unenc.Generation)

		if conn.status == ConnectionStatusWaitingForRemotePid {
			// update client connection
			t.writeMu.Lock()
			conn.status = ConnectionStatusEstablished
			conn.remotePid = remotePid
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
	// only client encrypts data with old cipher
	oldCipher := unenc.RemotePid.Utime == 0 && unenc.RemotePid.PortPid>>16 == 0
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

// TODO support stateless handshake in C++ and go rpc code and add capability bit in handshake
func (t *Transport) createConnection(status ConnectionStatus, localIp uint32, remoteAddr netip.AddrPort, remotePid tlnet.Pid, generation uint32, noTimers bool) *Connection {
	localIpSlice := make([]byte, 4)
	binary.BigEndian.PutUint32(localIpSlice, localIp)
	var remoteIp [4]byte
	binary.BigEndian.PutUint32(remoteIp[:], remotePid.Ip)
	conn := &Connection{
		status:     status,
		remoteAddr: remoteAddr,
		localUdpAddr: net.UDPAddr{
			IP:   localIpSlice,
			Port: int(t.localPid.PortPid & 0xffff),
		},
		remoteUdpAddr: net.UDPAddr{
			IP:   remoteAddr.Addr().AsSlice(),
			Port: int(remoteAddr.Port()),
		},
		localIp: localIp,
		outgoing: OutgoingConnection{
			transport:           t,
			MaxWindowSize:       DefaultMaxWindowSize,
			MaxPayloadSize:      MaxChunkSize, // TODO make MaxPayloadSize equal corresponding value in engine repo
			WindowControlSeqNum: -1,
		},
		incoming: IncomingConnection{
			transport:     t,
			maxWindowSize: DefaultMaxWindowSize,
			maxChunkSize:  MaxChunkSize,
			windowControl: -1,
		},
		generation: generation,
		remotePid:  remotePid,
	}
	if t.debugUdpRPC >= 1 {
		log.Printf("created udp transport connection: localPid(%+v) remotePid(%+v) generation(%d) keyId(%+x)", conn.localPid(), remotePid, generation, conn.keyID)
	}
	var randomKey [32]byte
	_, _ = cryptorand.Read(randomKey[:])
	CopyIVFrom(&conn.randomKey, randomKey)
	conn.nonce = uint64(conn.randomKey[0]) + (uint64(conn.randomKey[1]) << 32)
	conn.outgoing.conn = conn
	conn.incoming.conn = conn

	return conn
}

func (t *Transport) generateCryptoKeysOld(keyID [4]byte, conn *Connection, localPid tlnet.Pid, remotePid tlnet.Pid, generation uint32) {
	t.generateCryptoKeys(t.cryptoKeys[keyID], &conn.readOld, &conn.writeOld, localPid, remotePid, generation)
	conn.keyID = keyID
}

func (t *Transport) generateCryptoKeysNew(cryptoKey string, conn *Connection, localPid tlnet.Pid, remotePid tlnet.Pid, generation uint32) {
	t.generateCryptoKeys(cryptoKey, &conn.readNew, &conn.writeNew, localPid, remotePid, generation)
}

func (t *Transport) generateCryptoKeys(cryptoKey string, read *cipher.Block, write *cipher.Block, localPid tlnet.Pid, remotePid tlnet.Pid, generation uint32) {
	keys := DeriveCryptoKeysUdp(cryptoKey, &localPid, &remotePid, generation)
	var err error
	*read, err = aes.NewCipher(keys.ReadKey[:])
	if err != nil {
		log.Panicf("error while generating read crypto keys: %+v", err)
	}
	*write, err = aes.NewCipher(keys.WriteKey[:])
	if err != nil {
		log.Panicf("error while generating read crypto keys: %+v", err)
	}
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
		readCipher = conn.readOld
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
	return t.newMessages.Len() == 0 && t.newHdrRcvs.Len() == 0 && t.newResends.Len() == 0 &&
		t.newAckSnds.Len() == 0 && t.newResendRequestsRcvs.Len() == 0 &&
		t.newResendRequestSnds.Len() == 0 && t.connectionSendQueue.Len() == 0
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

		data, conn, needResendTimer, wasRepeatedResendTimers := t.goWriteStep()

		if data != nil {
			_, err := t.socket.WriteToUDPAddrPort(data, conn.remoteAddr)
			if t.debugUdpRPC >= 2 {
				log.Printf("goWrite() -> datagram to %s", conn.remoteAddr.String())
			}

			t.writeMu.Lock()
			if conn.outgoing.haveChunksToSendNow() {
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
				// We do not call
				// if !conn.resendTimer.Stop() {
				//         <-conn.resendTimer.C
				// }
				// because we have synchronization point with goResend() goroutine via t.newResends queue
				conn.resendTime = time.Now().Add(t.resendTimeout)

				t.resendTimers.PushBack(conn)
				conn.inResendQueue = true
			}
		}
		if needResendTimer || wasRepeatedResendTimers {
			t.resendCV.Signal()
		}
	}
}

// goWriteStep handles all new events
// Pre-condition: t.writeMu is acquired
// Post-condition: if datagram != nil, then t.writeMu is unlocked, otherwise locked
// returns:
// > datagram (and connection) to send if any
// > needConnResendTimer - whether resend timer for conn is required
// > wasRepeatedResendTimers - were there any resend timers repeated
func (t *Transport) goWriteStep() (datagram []byte, conn *Connection, needConnResendTimer, wasRepeatedResendTimers bool) {
	// TODO add extra instances of newHdrRcvs, newMessages and newResends to reduce contention
	// when goWrite() iterates over them and updates OutgoingConnection's state
	// idea:
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
	//
	// UPD: we have a problem, that event handling sometime leads to addConnectionToSendQueue,
	// which requires locking, and with this trick we have to lock for every connection add.
	// For example, every newMessages event requires addConnectionToSendQueue
	//
	// To solve this we save connections in connectionSendQueueLocal

	t.writeMu.Unlock()

	for t.newHdrRcvsLocal.Len() > 0 {
		hdr := t.newHdrRcvsLocal.PopFront()
		conn = hdr.conn

		// update OutgoingConnection state with received acks
		t.handleAck(conn, &hdr.enc)
		if conn.outgoing.haveChunksToSendNow() {
			t.connectionSendQueueLocal.PushBack(conn)
		}

		// update AcksToSend with received seqNums
		if hdr.enc.IsSetPacketsFrom() {
			conn.acks.AddAckRange(hdr.enc.PacketsFrom, hdr.enc.PacketsFrom+hdr.enc.PacketsCount-1)
		}
		if hdr.enc.IsSetPacketNum() && hdr.enc.PacketNum != ^uint32(0) {
			conn.acks.AddAckRange(hdr.enc.PacketNum, hdr.enc.PacketNum)
		}
	}
	for t.newResendsLocal.Len() > 0 {
		conn = t.newResendsLocal.PopFront()
		conn.inResendQueue = false

		conn.outgoing.OnResendTimeout()
		if conn.outgoing.haveChunksToSendNow() {
			t.connectionSendQueueLocal.PushBack(conn)
		} else if conn.outgoing.Window.Len() > 0 {
			// timer have burned but connection doesn't have timeouted chunks yet (all chunks has been sent just now) - so need to run timer again
			t.resendTimersLocal.PushBack(conn)
		}
	}
	for t.newResendRequestSndsLocal.Len() > 0 {
		conn = t.newResendRequestSndsLocal.PopFront()

		if conn.acks.HaveHoles() {
			conn.forceResendRequest = true
			t.connectionSendQueueLocal.PushBack(conn)
		}
	}
	for t.newResendRequestsRcvsLocal.Len() > 0 {
		connResReq := t.newResendRequestsRcvsLocal.PopFront()

		isNewRange := true
		i := 0
		j := 0
		for i < len(connResReq.conn.outgoing.resendRanges.Ranges) && j < len(connResReq.req.Ranges) {
			wasRange := connResReq.conn.outgoing.resendRanges.Ranges[i]
			rcvRange := connResReq.req.Ranges[j]
			if rcvRange.PacketNumFrom < wasRange.PacketNumFrom {
				// it is old resend request
				isNewRange = false
				break
			}
			if rcvRange.PacketNumFrom > wasRange.PacketNumFrom {
				// it is new resend request
				connResReq.conn.outgoing.resendRanges = connResReq.req
				connResReq.conn.outgoing.resendIndex = 0
				connResReq.conn.outgoing.rangeInnerIndex = 0
				break
			}

			if rcvRange.PacketNumTo > wasRange.PacketNumTo {
				// it is old resend request
				isNewRange = false
				break
			}
			if rcvRange.PacketNumTo < wasRange.PacketNumTo {
				// it is new resend request
				connResReq.conn.outgoing.resendRanges = connResReq.req
				connResReq.conn.outgoing.resendIndex = 0
				connResReq.conn.outgoing.rangeInnerIndex = 0
				break
			}

			// identical intervals
			i++
			j++
		}

		if isNewRange {
			connResReq.conn.outgoing.resendRanges = connResReq.req
			t.connectionSendQueueLocal.PushBack(connResReq.conn)
		}
	}
	for t.newMessagesLocal.Len() > 0 {
		cm := t.newMessagesLocal.PopFront()

		if cm.unreliable {
			cm.conn.outgoing.UnreliableMessages.PushBack(cm.message)
		} else {
			var m *OutgoingMessage
			if n := len(t.messagesPool); n == 0 {
				m = &OutgoingMessage{}
			} else {
				m = t.messagesPool[n-1]
				t.messagesPool = t.messagesPool[:n-1]
			}

			m.payload = cm.message
			m.seqNo = (1 << 32) - 1 // TODO: ???
			m.offset = cm.conn.outgoing.LastAckedMessageOffset + cm.conn.outgoing.TotalMessagesSize
			m.refCount = 1

			cm.conn.outgoing.MessageQueue.PushBack(m)
			cm.conn.outgoing.TotalMessagesSize += int64(len(*cm.message))
		}
		t.connectionSendQueueLocal.PushBack(cm.conn)
	}
	for t.newAckSndsLocal.Len() > 0 {

		conn = t.newAckSndsLocal.PopFront()

		// if connection is inSendQueue or has already sent datagram since ack timer run, then skip empty ack
		if !conn.ackSent {
			t.connectionSendQueueLocal.PushBack(conn)
		}
		conn.ackSent = false
	}

	t.writeMu.Lock()

	for t.connectionSendQueueLocal.Len() > 0 {
		conn = t.connectionSendQueueLocal.PopFront()
		t.addConnectionToSendQueueLocked(conn)
	}

	wasRepeatedResendTimers = false
	for t.resendTimersLocal.Len() > 0 {
		wasRepeatedResendTimers = true

		conn = t.resendTimersLocal.PopFront()
		conn.resendTime = time.Now().Add(t.resendTimeout)
		t.resendTimers.PushBack(conn)
		conn.inResendQueue = true
	}

	for t.connectionSendQueue.Len() > 0 {
		conn = t.popConnectionFromSendQueueLocked()
		if conn.closed {
			continue
		}

		withoutPayload := false
		if conn.forceResendRequest {
			withoutPayload = true
		}
		if conn.inAckQueue {
			conn.ackSent = true
		}

		status := conn.status
		remotePid := conn.remotePid
		t.writeMu.Unlock()

		var haveUserPayload bool
		var err error
		datagram, haveUserPayload, err = t.buildDatagram(conn, status, remotePid, withoutPayload)
		if err != nil {
			log.Printf("goWrite: failed to build datagram: %s", err)
			t.writeMu.Lock()
			if conn.outgoing.haveChunksToSendNow() {
				t.addConnectionToSendQueueLocked(conn)
			}
			return nil, nil, false, wasRepeatedResendTimers
		}

		return datagram, conn, haveUserPayload && !conn.inResendQueue, wasRepeatedResendTimers
	}

	// no datagram to send and timer to run
	return nil, nil, false, wasRepeatedResendTimers
}

func (t *Transport) handleAck(conn *Connection, ack *tlnetUdpPacket.EncHeader) {
	if ack.IsSetPacketAckPrefix() {
		err := conn.outgoing.AckPrefix(ack.PacketAckPrefix + 1)
		if err != nil {
			log.Printf("goWrite: error from AckPrefix(): %s", err)
		}
	}

	if ack.IsSetPacketAckFrom() {
		for ackSeqNo := ack.PacketAckFrom; ackSeqNo <= ack.PacketAckTo; ackSeqNo++ {
			err := conn.outgoing.AckChunk(ackSeqNo)
			if err != nil {
				log.Printf("goWrite: error from AckChunk(): %s", err)
			}
		}
	}

	if ack.IsSetPacketAckSet() {
		for _, ackSeqNo := range ack.PacketAckSet {
			err := conn.outgoing.AckChunk(ackSeqNo)
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

const ObsoletePidPayloadSize = 4 + 12 + 12 + 4
const ObsoleteGenerationPayloadSize = 4 + 12 + 4
const ObsoleteHashPayloadSize = 4 + 8 + 12

func (t *Transport) buildDatagram(conn *Connection, status ConnectionStatus, remotePid tlnet.Pid, withoutPayload bool) ([]byte, bool, error) {
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
	if status == ConnectionSentObsoletePid {
		payload := make([]byte, 0, ObsoletePidPayloadSize)
		payload = basictl.NatWrite(payload, tlnetUdpPacket.ObsoletePid{}.TLTag())
		payload = conn.receivedObsoletePid.Write(payload)
		payload = localPid.Write(payload)
		payload = basictl.NatWrite(payload, conn.generation)

		t.writeChunks = append(t.writeChunks, payload)

		setUnreliableEncFlags(&enc)

	} else if status == ConnectionSentObsoleteGeneration {
		payload := make([]byte, 0, ObsoleteGenerationPayloadSize)
		payload = basictl.NatWrite(payload, tlnetUdpPacket.ObsoleteGeneration{}.TLTag())
		payload = localPid.Write(payload)
		payload = basictl.NatWrite(payload, conn.generation)

		t.writeChunks = append(t.writeChunks, payload)

		setUnreliableEncFlags(&enc)

	} else if status == ConnectionSentObsoleteHash {
		payload := make([]byte, 0, ObsoleteHashPayloadSize)
		payload = basictl.NatWrite(payload, tlnetUdpPacket.ObsoleteHash{}.TLTag())
		payload = basictl.LongWrite(payload, conn.receivedObsoleteHash)
		payload = localPid.Write(payload)

		t.writeChunks = append(t.writeChunks, payload)

		setUnreliableEncFlags(&enc)

	} else if conn.forceResendRequest {
		conn.forceResendRequest = false
		var req tlnetUdpPacket.ResendRequest
		t.buildResendRequestDatagram(conn, &enc, &req)
		t.writeChunks = algo.ResizeSlice(t.writeChunks, 1)
		t.writeChunks[0] = req.WriteBoxed(t.writeChunks[0])
	} else {
		msgToDealloc, haveUserPayload = t.buildPayloadDatagram(conn, &enc, withoutPayload)
	}

	// common unenc and enc flags for every datagram

	// Unencrypted header
	if status == ConnectionStatusEstablished {
		unenc.SetPidHash(conn.id.Hash)
	} else {
		unenc.SetLocalPid(localPid)
		unenc.SetRemotePid(remotePid)
		unenc.SetGeneration(conn.generation)
	}
	cryptoFlags := CryptoFlagHandshake | CryptoFlagModeAes
	cryptoFlags |= (binary.LittleEndian.Uint32(conn.keyID[:]) & CryptoKeyIdMask) << CryptoKeyIdShift
	unenc.SetCryptoFlags(cryptoFlags)
	unenc.SetCryptoSha(true)
	conn.nonce++
	conn.randomKey[0] = uint32(conn.nonce)
	conn.randomKey[1] = uint32(conn.nonce >> 32)
	unenc.SetCryptoRandom(conn.randomKey)
	unenc.SetEncryptedData(true)

	// Encrypted header
	enc.Flags |= unenc.Flags & EncryptedFlagsMask // some bullshit from C++ code
	enc.SetVersion(TransportVersion | TransportVersion<<16)

	// TODO think what crypto use when send
	// ConnectionSentObsoletePid
	// and ConnectionSentObsoleteGeneration

	oldCipher := status == ConnectionStatusWaitingForRemotePid || status == ConnectionSentObsoleteHash
	bytes, err := t.serializeDatagram(conn, unenc, enc, oldCipher, msgToDealloc)
	return bytes, haveUserPayload, err
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

func (t *Transport) buildPayloadDatagram(conn *Connection, enc *tlnetUdpPacket.EncHeader, withoutPayload bool) (msgToDealloc *[]byte, haveUserPayload bool) {
	conn.acks.BuildAck(enc) // TODO set window control!!!
	if withoutPayload {
		return nil, false
	}

	var singleMessage bool
	var firstSeqNum uint32
	t.writeChunks = t.writeChunks[:0]
	t.writeChunks, msgToDealloc, firstSeqNum, singleMessage = conn.outgoing.GetChunksToSend(t.writeChunks)
	haveUserPayload = len(t.writeChunks) > 0

	// Encrypted header
	if haveUserPayload {
		if firstSeqNum == ^uint32(0) {
			setUnreliableEncFlags(enc)
		} else {
			lastSeqNum := firstSeqNum + uint32(len(t.writeChunks)) - 1
			firstChunkRef := conn.outgoing.Window.IndexRef(int(firstSeqNum - conn.outgoing.AckSeqNoPrefix))
			lastChunkRef := conn.outgoing.Window.IndexRef(int(lastSeqNum - conn.outgoing.AckSeqNoPrefix))
			if firstSeqNum == lastSeqNum {
				enc.SetPacketNum(firstSeqNum)
			} else {
				enc.SetPacketsFrom(firstSeqNum)
				enc.SetPacketsCount(lastSeqNum - firstSeqNum + 1)
			}
			enc.SetPrevParts(firstChunkRef.prevParts)
			enc.SetNextParts(lastChunkRef.nextParts)
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
	conn *Connection,
	unenc tlnetUdpPacket.UnencHeader,
	enc tlnetUdpPacket.EncHeader,
	oldCipher bool,
	msgToDealloc *[]byte,
) ([]byte, error) {
	t.writeBuffer = t.writeBuffer[:0]
	t.writeEncryptedBuffer = t.writeEncryptedBuffer[:0]

	var err error
	t.writeBuffer = enc.Write(t.writeBuffer)
	for i, chunk := range t.writeChunks {
		if len(t.writeChunks) > 1 {
			t.writeBuffer = basictl.NatWrite(t.writeBuffer, uint32(len(chunk)))
		}
		t.writeBuffer = append(t.writeBuffer, chunk...)
		if msgToDealloc != nil {
			t.messageDeallocator(msgToDealloc)
		}
		t.writeChunks[i] = nil
	}

	padding := 0
	bytes := len(t.writeBuffer) + encryptedUnencHeaderSize(unenc)
	if bytes%8 != 0 {
		padding += 4
		enc.SetZeroPadding4Bytes(true)
		bytes += padding
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

	t.writeBuffer, err = writeEncryptedUnencHeader(t.writeBuffer, unenc)
	if err != nil {
		return nil, err
	}

	// Unencrypted header
	t.writeEncryptedBuffer = unenc.WriteBoxed(t.writeEncryptedBuffer)

	// Unencrypted header + encrypted part
	unencHeaderSize := len(t.writeEncryptedBuffer)
	t.writeEncryptedBuffer = algo.ResizeSlice(t.writeEncryptedBuffer, len(t.writeEncryptedBuffer)+len(t.writeBuffer))

	var writeCipher cipher.Block
	if oldCipher {
		writeCipher = conn.writeOld
	} else {
		writeCipher = conn.writeNew
	}
	var ivb [32]byte
	CopyIVTo(&ivb, unenc.CryptoRandom)
	ige.EncryptBlocks(writeCipher, ivb[:], t.writeEncryptedBuffer[unencHeaderSize:], t.writeBuffer)

	t.writeEncryptedBuffer = basictl.NatWrite(t.writeEncryptedBuffer, crc32.Checksum(t.writeEncryptedBuffer, castagnoliTable))

	return t.writeEncryptedBuffer, err
}

func writeEncryptedUnencHeader(payload []byte, unenc tlnetUdpPacket.UnencHeader) ([]byte, error) {
	var err error
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

	return payload, err
}

func (t *Transport) goResend() {
	t.writeMu.Lock()

	defer func() {
		t.writeMu.Unlock()
		t.wg.Done()
	}()

	for {
		for t.resendTimers.Len() == 0 && !t.closed {
			t.resendCV.Wait()
		}
		if t.closed {
			break
		}
		// TODO replace queue with priorityQueue to allow timeouts with different durations
		conn := t.resendTimers.PopFront()

		duration := time.Until(conn.resendTime)
		if duration > 0 {
			// to not hold mutex for the long time waiting for the timeout
			t.writeMu.Unlock()
			time.Sleep(duration)
			t.writeMu.Lock()
		}

		// TODO replace 2 stupid queues (resendTimers and newResends) with simply one queue (resendTimers)
		// idea:
		// goResend() reads (without deleting) first queue element, waits for timer to burn,
		// then increments one integer index in Transport, signalling goWrite().
		// goWrite() reads this index and if it was incremented,
		// consider the first element of resendTimers as new event, handles and deletes it.
		// UPD: works only for equal timeout durations

		// send timeout action to goWrite
		t.newResends.PushBack(conn)
		t.writeCV.Signal()
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
		// TODO replace queue with priorityQueue to allow timeouts with different durations
		conn := t.ackTimers.PopFront()

		duration := time.Until(conn.ackTime)
		if duration > 0 {
			// to not hold mutex for the long time waiting for the timeout
			t.writeMu.Unlock()
			time.Sleep(duration)
			t.writeMu.Lock()
		}

		conn.inAckQueue = false
		// send force ack action to goWrite
		t.newAckSnds.PushBack(conn)
		t.writeCV.Signal()
	}
}

func (t *Transport) goResendRequest() {
	t.writeMu.Lock()

	defer func() {
		t.writeMu.Unlock()
		t.wg.Done()
	}()

	for {
		for t.resendRequestTimers.Len() == 0 && !t.closed {
			t.resendRequestCV.Wait()
		}
		if t.closed {
			break
		}
		// TODO replace queue with priorityQueue to allow timeouts with different durations
		conn := t.resendRequestTimers.PopFront()

		duration := time.Until(conn.resendRequestTime)
		if duration > 0 {
			// to not hold mutex for the long time waiting for the timeout
			t.writeMu.Unlock()
			time.Sleep(duration)
			t.writeMu.Lock()
		}

		conn.inResendRequestQueue = false
		// send force ack action to goWrite
		t.newResendRequestSnds.PushBack(conn)
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
		firstWaiter.OnAcquiredMemory()
		if t.debugUdpRPC >= 2 {
			log.Printf("udp connection %s returned from incoming memory waiters queue", firstWaiter.conn.remoteAddr.String())
		}
		t.acquiredMemoryEvents = append(t.acquiredMemoryEvents, ConnAcquiredChunk{
			conn:   firstWaiter.conn,
			seqNum: firstWaiter.requestedChunkSeqNo,
		})
	}
}

func (t *Transport) tryAcquireMemoryForTheFirst(first *IncomingConnection) bool {
	newAcquiredMemory := t.acquiredMemory + first.requestedMemorySize
	if newAcquiredMemory <= t.incomingMessagesMemoryLimit {
		t.acquiredMemory = newAcquiredMemory
		t.memoryWaiters.PopFront()
		first.inMemoryWaitersQueue = false
		if t.debugUdpRPC >= 2 {
			log.Printf("udp memory acquired %d bytes", first.requestedMemorySize)
		}
		// calling function must notice or answer connection
		return true
	}
	return false
}

func (t *Transport) tryAcquireMemory(conn *IncomingConnection) bool {
	if !conn.inMemoryWaitersQueue {
		t.memoryWaiters.PushBack(conn)
		conn.inMemoryWaitersQueue = true
	} // else connection reduced his request

	firstWaiter := t.memoryWaiters.Front()
	if firstWaiter == conn {
		// this connection was already the first or waiters queue was empty
		acquired := t.tryAcquireMemoryForTheFirst(firstWaiter)
		if acquired {
			t.checkMemoryWaiters()
			return true
		}
	}
	if t.debugUdpRPC >= 2 {
		log.Printf("udp connection %s stuck in incoming memory waiters queue...", firstWaiter.conn.remoteAddr.String())
	}
	return false
}

func (t *Transport) releaseMemory(releasedSize int64) {
	if t.acquiredMemory-releasedSize < 0 {
		panic(fmt.Sprintf("released %d but was acquired only %d bytes", releasedSize, t.acquiredMemory))
	}
	if t.debugUdpRPC >= 2 {
		log.Printf("udp memory releases %d bytes", releasedSize)
	}
	t.acquiredMemory -= releasedSize
	t.checkMemoryWaiters()
}

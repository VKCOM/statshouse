// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
)

const (
	packetOverhead       = 4 * 4
	maxPacketLen         = 16*1024*1024 - 1
	maxNonceHandshakeLen = 1024 - 1 // this limit is not exact, but it still prevents attack to force allocating lots of memory
	blockSize            = 16
	padVal               = 4
	startSeqNum          = -2

	memcachedStatsReqRN  = "stats\r\n"
	memcachedStatsReqN   = "stats\n"
	memcachedGetStatsReq = "get stats\r\n"
	memcachedVersionReq  = "version\r\n"
)

var (
	memcachedCommands = []string{
		memcachedStatsReqRN,
		memcachedStatsReqN,
		memcachedGetStatsReq,
		memcachedVersionReq,
	}

	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

	errHeaderCorrupted = fmt.Errorf("packet header corrupted")
)

// transport stream, encrypted using standard VK rpc scheme
type PacketConn struct {
	conn       net.Conn
	tcpconn_fd *os.File

	remoteAddr      string
	localAddr       string
	timeoutAccuracy time.Duration

	flagCancelReq   bool
	protocolVersion uint32 // initially 0, will be set after nonce message is received from the peer

	readMu        sync.Mutex
	r             *cryptoReader
	rDeadline     time.Time
	headerReadBuf [packetOverhead - 4]byte
	readSeqNum    int64

	writeMu        sync.Mutex
	w              *cryptoWriter
	wDeadline      time.Time
	headerWriteBuf []byte // contains either crc from previous packet or nothing after flush
	writeSeqNum    int64
	writeCRC       uint32 // when writing packet in parts, collected here
	writeAlignTo4  int    // bytes after crc/xxhash

	keyID [4]byte // to identify clients for Server with more than 1 crypto key
	table *crc32.Table

	closeOnce sync.Once
	closeErr  error

	pingMu          sync.Mutex
	pingSent        bool
	currentPingID   int64
	pingBodyReadBuf [8 + 4]byte // pingID + space for CRC, also used for pongs

	// mini send queue for ping-pongs
	writePing   bool
	writePingID [8]byte
	writePong   bool
	writePongID [8]byte
}

func NewPacketConn(c net.Conn, readBufSize int, writeBufSize int) *PacketConn {
	pc := &PacketConn{
		conn:            c,
		remoteAddr:      c.RemoteAddr().String(),
		localAddr:       c.LocalAddr().String(),
		timeoutAccuracy: DefaultConnTimeoutAccuracy,
		r:               newCryptoReader(c, readBufSize),
		w:               newCryptoWriter(c, writeBufSize),
		readSeqNum:      startSeqNum,
		writeSeqNum:     startSeqNum,
		table:           crc32.IEEETable,
	}

	if tcpconn, ok := c.(*net.TCPConn); ok { // pay cast and dup() const only on start
		if fd, err := tcpconn.File(); err == nil { // ok, will work as not a tcp connection
			pc.tcpconn_fd = fd
		}
	}

	return pc
}

func NewDefaultPacketConn(c net.Conn) *PacketConn {
	return NewPacketConn(c, DefaultServerConnReadBufSize, DefaultServerConnWriteBufSize)
}

// Negotiated during handshake, does not change after handshake
func (pc *PacketConn) FlagCancelReq() bool {
	return pc.flagCancelReq
}

func (pc *PacketConn) LocalAddr() string {
	return pc.localAddr
}

func (pc *PacketConn) RemoteAddr() string {
	return pc.remoteAddr
}

func (pc *PacketConn) ProtocolVersion() uint32 {
	return pc.protocolVersion
}

func (pc *PacketConn) Close() error {
	pc.closeOnce.Do(func() {
		if pc.tcpconn_fd != nil {
			_ = pc.tcpconn_fd.Close()
		}
		pc.closeErr = pc.conn.Close()
	})
	return pc.closeErr
}

func (pc *PacketConn) setCRC32C() {
	pc.table = castagnoliTable
}

func (pc *PacketConn) setReadTimeoutUnlocked(timeout time.Duration) error {
	if timeout <= 0 {
		if pc.rDeadline == (time.Time{}) {
			return nil
		}
		err := pc.conn.SetReadDeadline(time.Time{})
		if err != nil {
			return err
		}
		pc.rDeadline = time.Time{}
		return nil
	}
	deadlineDiff := time.Until(pc.rDeadline) - timeout
	if deadlineDiff < -pc.timeoutAccuracy || deadlineDiff > pc.timeoutAccuracy {
		deadline := time.Now().Add(timeout)
		err := pc.conn.SetReadDeadline(deadline)
		if err != nil {
			return err
		}
		pc.rDeadline = deadline
	}
	return nil
}

func (pc *PacketConn) setWriteTimeoutUnlocked(timeout time.Duration) error {
	if timeout <= 0 {
		if pc.wDeadline == (time.Time{}) {
			return nil
		}
		err := pc.conn.SetWriteDeadline(time.Time{})
		if err != nil {
			return err
		}
		pc.wDeadline = time.Time{}
		return nil
	}
	deadlineDiff := time.Until(pc.wDeadline) - timeout
	if deadlineDiff < -pc.timeoutAccuracy || deadlineDiff > pc.timeoutAccuracy {
		deadline := time.Now().Add(timeout)
		err := pc.conn.SetWriteDeadline(deadline)
		if err != nil {
			return err
		}
		pc.wDeadline = deadline
	}
	return nil
}

// ReadPacket will resize/reuse body to size of packet + up to 8 bytes overhead
func (pc *PacketConn) ReadPacket(body []byte, timeout time.Duration) (tip uint32, _ []byte, err error) {
	for {
		var isBuiltin bool
		tip, _, body, isBuiltin, _, err = pc.readPacketWithMagic(body, timeout)
		if err != nil {
			return tip, body, err
		}
		if !isBuiltin { // fast path
			return tip, body, nil
		}
		if err := pc.WritePacketBuiltin(timeout); err != nil {
			return tip, body, err
		}
	}
}

// ReadPacketUnlocked for high-efficiency users, which write with *Unlocked functions
// if isBuiltin is true, caller must call WritePacketBuiltinNoFlushUnlocked(timeout)
// TODO - builtinKind is only for temporary testing, remove later
func (pc *PacketConn) ReadPacketUnlocked(body []byte, timeout time.Duration) (tip uint32, _ []byte, isBuiltin bool, builtinKind string, err error) {
	tip, _, body, isBuiltin, builtinKind, err = pc.readPacketWithMagic(body, timeout)
	return tip, body, isBuiltin, builtinKind, err
}

// supports sending ascii command via terminal instead of first TL RPC packet, returns command in
func (pc *PacketConn) readPacketWithMagic(body []byte, timeout time.Duration) (tip uint32, magic []byte, _ []byte, isBuiltin bool, builtinKind string, err error) {
	pc.readMu.Lock()
	defer pc.readMu.Unlock()

	var header packetHeader
	magicHead, isBuiltin, builtinKind, err := pc.readPacketHeaderUnlocked(&header, timeout)
	if err != nil {
		return 0, magicHead, body, isBuiltin, builtinKind, err
	}
	if isBuiltin {
		return 0, nil, body, true, builtinKind, nil
	}

	body, err = pc.readPacketBodyUnlocked(&header, body)
	return header.tip, nil, body, isBuiltin, builtinKind, err
}

func (pc *PacketConn) readPacketHeaderUnlocked(header *packetHeader, timeout time.Duration) (magicHead []byte, isBuiltin bool, builtinKind string, err error) {
	for {
		if err = pc.setReadTimeoutUnlocked(timeout); err != nil {
			return nil, false, "", fmt.Errorf("failed to set read timeout: %w", err)
		}
		magicHead, err = pc.readPacketHeaderUnlockedImpl(header)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return magicHead, false, "", err
			}
			if pc.readSeqNum < 0 || len(magicHead) != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
				// if performing handshake || read at least 1 byte || not a timeout error
				return magicHead, false, "", &tagError{
					tag: "bad_header",
					err: fmt.Errorf("failed to read header for seq_num %d, magic(hex) %x: %w", pc.readSeqNum, magicHead, err),
				}
			}
			if !pc.sendPing() {
				return nil, false, "", fmt.Errorf("timeout after ping sent: %w", err)
			}
			return nil, true, "sendPing", nil // send builtin ping
		}
		if header.tip == PacketTypeRPCPing {
			if header.length != packetOverhead+8 {
				return nil, false, "", &tagError{
					tag: "out_of_range_packet_size",
					err: fmt.Errorf("ping packet has wrong length: %d", header.length),
				}
			}
			_, err := pc.readPacketBodyUnlocked(header, pc.pingBodyReadBuf[:])
			if err != nil {
				return nil, false, "", fmt.Errorf("failed to read ping body: %w", err)
			}
			if err := pc.onPing(pc.pingBodyReadBuf[:]); err != nil { // if pingBodyReadBuf is not big enough, here will be garbage due to realloc above
				return nil, false, "", err
			}
			return nil, true, "sendPong", nil // send builtin pong
		}
		if header.tip == PacketTypeRPCPong {
			if header.length != packetOverhead+8 {
				return nil, false, "", fmt.Errorf("pong packet has wrong length %d", header.length)
			}
			_, err := pc.readPacketBodyUnlocked(header, pc.pingBodyReadBuf[:])
			if err != nil {
				return nil, false, "", fmt.Errorf("failed to read pong body: %w", err)
			}
			if err := pc.onPong(pc.pingBodyReadBuf[:]); err != nil { // if pingBodyReadBuf is not big enough, here will be garbage due to realloc above
				return nil, false, "", err
			}
			continue // read next header without sending anything
		}
		return magicHead, false, "", nil
	}
}

func (pc *PacketConn) readPacketHeaderUnlockedImpl(header *packetHeader) (magicHead []byte, err error) {
	// special-case first packet: it can't contain padding, but can be a memcached command
	if pc.readSeqNum == startSeqNum {
		n, err := readFullOrMagic(pc.r, pc.headerReadBuf[:12], memcachedCommands)
		if err != nil {
			return pc.headerReadBuf[:n], err
		}
		header.length = binary.LittleEndian.Uint32(pc.headerReadBuf[:4])
	} else {
		header.length = padVal
		// it is important to return (0, eof) when FIN is read on the message boundary
		for i := 0; header.length == padVal; i++ {
			if i >= blockSize/4 {
				return nil, &tagError{
					tag: "excessive_padding",
					err: fmt.Errorf("excessive (%d) padding", i),
				}
			}
			n, err := io.ReadFull(pc.r, pc.headerReadBuf[:4])
			if err != nil {
				return pc.headerReadBuf[:n], err
			}
			header.length = binary.LittleEndian.Uint32(pc.headerReadBuf[:4])
		}

		n, err := io.ReadFull(pc.r, pc.headerReadBuf[4:12])
		if err != nil {
			if err == io.EOF {
				return pc.headerReadBuf[:4+n], io.ErrUnexpectedEOF // EOF before packet header end is not expected
			}
			return pc.headerReadBuf[:4+n], err
		}
	}
	header.seqNum = binary.LittleEndian.Uint32(pc.headerReadBuf[4:8])
	header.tip = binary.LittleEndian.Uint32(pc.headerReadBuf[8:12])

	if header.length < packetOverhead || header.length > maxPacketLen {
		return pc.headerReadBuf[:12], &tagError{
			tag: "out_of_range_packet_size",
			err: fmt.Errorf("packet size %v outside [%v, %v]: %w", header.length, packetOverhead, maxPacketLen, errHeaderCorrupted),
		}
	}
	if pc.protocolVersion == 0 && header.length%4 != 0 {
		return pc.headerReadBuf[:12], &tagError{
			tag: "bad_packet_size",
			err: fmt.Errorf("packet size %v must be a multiple of 4: %w", header.length, errHeaderCorrupted),
		}
	}
	if pc.readSeqNum < 0 {
		// checks below also forbid ping-pong during handshake
		if pc.readSeqNum == startSeqNum && header.tip != packetTypeRPCNonce { // this check is in nonceExchangeServer, but repeated here to detect non-RPC protocol earlier for connection hijack
			return pc.headerReadBuf[:12], &tagError{
				tag: "bad_nonce_packet_type",
				err: fmt.Errorf("nonce packet type 0x%x instead of 0x%x: %w", header.tip, packetTypeRPCNonce, errHeaderCorrupted),
			}

		}
		if pc.readSeqNum == startSeqNum+1 && header.tip != packetTypeRPCHandshake { // this check is in handshakeExchangeServer, but repeated here to better detect different keys with common prefixes leading to different AES keys
			return pc.headerReadBuf[:12], &tagError{
				tag: "bad_handshake_packet_type",
				err: fmt.Errorf("handshake packet type 0x%x instead of 0x%x: %w", header.tip, packetTypeRPCHandshake, errHeaderCorrupted),
			}
		}
		if header.length > maxNonceHandshakeLen {
			return pc.headerReadBuf[:12], &tagError{
				tag: "out_of_range_packet_size",
				err: fmt.Errorf("nonce/handshake packet size %v outside  [%v, %v]: %w", header.length, packetOverhead, maxNonceHandshakeLen, errHeaderCorrupted),
			}
		}
	}
	if header.seqNum != uint32(pc.readSeqNum) {
		return pc.headerReadBuf[:12], fmt.Errorf("seqnum mismatch: read %v, expected %v: %w", header.seqNum, pc.readSeqNum, errHeaderCorrupted)
	}
	pc.readSeqNum++

	return nil, nil
}

func (pc *PacketConn) readPacketBodyUnlocked(header *packetHeader, body []byte) (_ []byte, err error) {
	if header.length < packetOverhead || header.length > maxPacketLen {
		panic(fmt.Sprintf("packet size %v outside [%v, %v], was checked in readPacketHeaderUnlocked", header.length, packetOverhead, maxPacketLen))
	}
	bodySize := int(header.length) - packetOverhead
	alignTo4 := 0
	if pc.w.isEncrypted() {
		alignTo4 = int(-uint(header.length) & 3)
	}

	sz := bodySize + 4 + alignTo4 // read body, crc and alignment to 4 all at once
	if cap(body) < sz {
		body = make([]byte, sz)
	} else {
		body = body[:sz]
	}
	if n, err := io.ReadFull(pc.r, body); err != nil {
		if err == io.EOF {
			return body[:n], io.ErrUnexpectedEOF // EOF before packet end is not expected
		}
		return body[:n], err
	}
	readCRC := binary.LittleEndian.Uint32(body[bodySize:])
	for i := 0; i < alignTo4; i++ {
		if a := body[bodySize+4+i]; a != 0 {
			return body, &tagError{
				tag: "bad_body_padding_contents",
				err: fmt.Errorf("body padding to 4 byte boundary must be with zeroes, read 0x%x at position %d instead", a, i),
			}
		}
	}
	body = body[:bodySize]

	crc := crc32.Update(0, pc.table, pc.headerReadBuf[:12])
	crc = crc32.Update(crc, pc.table, body)

	if readCRC != crc {
		return body, &tagError{
			tag: "crc_mismatch",
			err: fmt.Errorf("CRC mismatch: read 0x%x, expected 0x%x", readCRC, crc),
		}
	}

	return body, nil
}

func (pc *PacketConn) WritePacket(packetType uint32, body []byte, timeout time.Duration) error {
	pc.writeMu.Lock()
	defer pc.writeMu.Unlock()

	if err := pc.WritePacketNoFlushUnlocked(packetType, body, timeout); err != nil {
		return err
	}
	return pc.FlushUnlocked()
}

func (pc *PacketConn) WritePacketNoFlush(packetType uint32, body []byte, timeout time.Duration) error {
	pc.writeMu.Lock()
	defer pc.writeMu.Unlock()

	return pc.WritePacketNoFlushUnlocked(packetType, body, timeout)
}

// If all writing is performed from the same goroutine, you can call Unlocked version of Write and Flush
func (pc *PacketConn) WritePacketNoFlushUnlocked(packetType uint32, body []byte, timeout time.Duration) error {
	if err := pc.writePacketHeaderUnlocked(packetType, len(body), timeout); err != nil {
		return err
	}
	if err := pc.writePacketBodyUnlocked(body); err != nil {
		return err
	}
	pc.writePacketTrailerUnlocked()
	return nil
}

func (pc *PacketConn) Flush() error {
	pc.writeMu.Lock()
	defer pc.writeMu.Unlock()

	return pc.FlushUnlocked()
}

// If all writing is performed from the same goroutine, you can call Unlocked version of Write and Flush
func (pc *PacketConn) FlushUnlocked() error {
	prevBytes := len(pc.headerWriteBuf)
	toWrite := prevBytes + pc.w.Padding(prevBytes)
	if toWrite != 0 {
		// We write crc and crypto padding together in single call
		if toWrite != prevBytes { // optimize for unencrypted messages and 1/4 of encrypted
			const padding = "\x04\x00\x00\x00\x04\x00\x00\x00\x04\x00\x00\x00" // []byte{padVal, 0, 0, 0, padVal, 0, 0, 0, padVal, 0, 0, 0}
			pc.headerWriteBuf = append(pc.headerWriteBuf, padding...)
		}

		if _, err := pc.w.Write(pc.headerWriteBuf[:toWrite]); err != nil {
			return err
		}
		pc.headerWriteBuf = pc.headerWriteBuf[:0]
	}
	return pc.w.Flush()
}

type closeWriter interface {
	CloseWrite() error
}

// Motivation - you call ShutdownWrite, and your blocking ReadPacket* will stop after receiving FIN with compatible sockets
// if you receive error for this method, you should call Close()
func (pc *PacketConn) ShutdownWrite() error {
	if err := pc.Flush(); err != nil { // Rare, so no problem to make excess locked call
		return err
	}
	cw, ok := pc.conn.(closeWriter) // UnixConn, TCPConn, and any other
	if !ok {
		return io.ErrShortWrite // TODO - better error
	}
	return cw.CloseWrite()
}

// how to use:
// first call writePacketHeaderUnlocked with sum of all body chunk lengths you are going to write on the next step
// then call writePacketBodyUnlocked 0 or more times
// then call writePacketTrailerUnlocked
func (pc *PacketConn) writePacketHeaderUnlocked(packetType uint32, packetBodyLen int, timeout time.Duration) error {
	if err := validBodyLen(packetBodyLen); err != nil {
		return err
	}
	if pc.protocolVersion == 0 && packetBodyLen%4 != 0 {
		return &tagError{
			tag: "bad_packet_size",
			err: fmt.Errorf("packet size %v must be a multiple of 4", packetBodyLen),
		}
	}

	if err := pc.setWriteTimeoutUnlocked(timeout); err != nil {
		return fmt.Errorf("failed to set write timeout: %w", err)
	}
	prevBytes := len(pc.headerWriteBuf)
	buf := pc.headerWriteBuf

	buf = basictl.NatWrite(buf, uint32(packetBodyLen+packetOverhead))
	buf = basictl.NatWrite(buf, uint32(pc.writeSeqNum))
	buf = basictl.NatWrite(buf, packetType)
	pc.headerWriteBuf = buf[:0] // reuse, prepare to accept crc32
	pc.writeSeqNum++
	if pc.w.isEncrypted() {
		pc.writeAlignTo4 = int(-uint(packetBodyLen) & 3)
	} else {
		pc.writeAlignTo4 = 0
	}

	if _, err := pc.w.Write(buf); err != nil { // with prevBytes
		return err
	}
	pc.writeCRC = 0
	pc.updateWriteCRC(buf[prevBytes:]) // without prevBytes
	return nil
}

func (pc *PacketConn) writePacketBodyUnlocked(body []byte) error {
	if _, err := pc.w.Write(body); err != nil {
		return err
	}
	pc.updateWriteCRC(body)
	return nil
}

func (pc *PacketConn) writePacketTrailerUnlocked() {
	pc.headerWriteBuf = binary.LittleEndian.AppendUint32(pc.headerWriteBuf, pc.writeCRC)
	switch pc.writeAlignTo4 {
	case 3:
		pc.headerWriteBuf = append(pc.headerWriteBuf, 0, 0, 0)
	case 2:
		pc.headerWriteBuf = append(pc.headerWriteBuf, 0, 0)
	case 1:
		pc.headerWriteBuf = append(pc.headerWriteBuf, 0)
	}
}

func (pc *PacketConn) updateWriteCRC(data []byte) {
	pc.writeCRC = crc32.Update(pc.writeCRC, pc.table, data)
}

func (pc *PacketConn) encrypt(readKey []byte, readIV []byte, writeKey []byte, writeIV []byte) error {
	pc.readMu.Lock()
	defer pc.readMu.Unlock()

	pc.writeMu.Lock()
	defer pc.writeMu.Unlock()

	rc, err := aes.NewCipher(readKey)
	if err != nil {
		return &tagError{
			tag: "nil_aes_reader",
			err: fmt.Errorf("read AES init failed: %w", err),
		}
	}

	wc, err := aes.NewCipher(writeKey)
	if err != nil {
		return &tagError{
			tag: "nil_aes_writer",
			err: fmt.Errorf("write AES init failed: %w", err),
		}
	}

	if len(readIV) != blockSize {
		return &tagError{
			tag: "bad_read_iv_size",
			err: fmt.Errorf("read IV size must be %v, not %v", blockSize, len(readIV)),
		}
	}
	if len(writeIV) != blockSize {
		return &tagError{
			tag: "bad_write_iv_size",
			err: fmt.Errorf("write IV size must be %v, not %v", blockSize, len(writeIV)),
		}
	}

	rcbc := cipher.NewCBCDecrypter(rc, readIV)
	wcbc := cipher.NewCBCEncrypter(wc, writeIV)
	if rcbc.BlockSize() != blockSize {
		return &tagError{
			tag: "bad_read_cbc_block_size",
			err: fmt.Errorf("CBC read decrypter BlockSize must be %v, not %v", blockSize, rcbc.BlockSize()),
		}
	}
	if wcbc.BlockSize() != blockSize {
		return &tagError{
			tag: "bad_write_cbc_block_size",
			err: fmt.Errorf("CBC write decrypter BlockSize must be %v, not %v", blockSize, wcbc.BlockSize()),
		}
	}

	pc.r.encrypt(rcbc)
	pc.w.encrypt(wcbc)

	return nil
}

func validBodyLen(n int) error { // Motivation - high byte was used for some flags, we must not use it
	if n > maxPacketLen-packetOverhead {
		return &tagError{
			tag: "out_of_range_packet_size",
			err: errTooLarge,
		}
	}
	return nil
}

// Merge of io.ReadFull and io.ReadAtLeast, but with a twist:
// we return early with io.EOF if we have read a magic sequence of bytes.
func readFullOrMagic(r io.Reader, buf []byte, magics []string) (n int, err error) {
	m := len(buf)
	for n < m && err == nil {
		var nn int
		nn, err = r.Read(buf[n:])
		n += nn

		// a twist:
		for _, magic := range magics {
			// cmd/compile does not allocate for this string conversion
			if string(buf[:n]) == magic {
				if err == nil {
					err = io.EOF
				}
				return
			}
		}
	}
	if n >= m { // actually never >
		err = nil
	} else if n > 0 && err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return
}

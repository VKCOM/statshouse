package rpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

var errZeroRead = fmt.Errorf("read returned zero bytes without an error")

type ReadWriteError struct {
	ReadErr  error
	WriteErr error
}

type ForwardPacketsResult struct {
	PacketHeaderCircularBuffer
	ReadWriteError
	ServerWantsFin bool
	ClientWantsFin bool
}

type ForwardPacketResult struct {
	packetHeader
	ReadWriteError
	ServerWantsFin bool
	ClientWantsFin bool
}

type forwardPacketOptions struct {
	testEnv bool
}

type PacketHeaderCircularBuffer struct {
	s    []packetHeader
	x, n int
}

func (p packetHeader) String() string {
	return fmt.Sprintf(`{"len":%d,"seq":%d,"tip":"0x%08X"}`, p.length, p.seqNum, p.tip)
}

func (res ForwardPacketsResult) String() string {
	return fmt.Sprintf(`{"read":%v,"write":%v,"clientFin":%t,"serverFin":%t}`, res.ReadErr, res.WriteErr, res.ClientWantsFin, res.ServerWantsFin)
}

func (b *PacketHeaderCircularBuffer) add(p packetHeader) {
	if len(b.s) == 0 {
		b.s = make([]packetHeader, 16)
	}
	b.s[b.x] = p
	b.x = (b.x + 1) % len(b.s)
	b.n++
}

func (b *PacketHeaderCircularBuffer) String() string {
	if b.n == 0 {
		return "[]"
	}
	var sb strings.Builder
	sb.WriteString("[")
	if b.n < len(b.s) {
		sb.WriteString(b.s[0].String())
		for i := 1; i < b.n; i++ {
			sb.WriteString(",")
			sb.WriteString(b.s[i].String())
		}
	} else {
		sb.WriteString(b.s[b.x].String())
		for i := (b.x + 1) % len(b.s); i != b.x; i = (i + 1) % len(b.s) {
			sb.WriteString(",")
			sb.WriteString(b.s[i].String())
		}
	}
	sb.WriteString("]")
	return sb.String()
}

func (r ReadWriteError) Error() error {
	if r.ReadErr != nil {
		return r.ReadErr
	}
	return r.WriteErr
}

func (pc *PacketConn) KeyID() [4]byte {
	return pc.keyID
}

func ForwardPackets(ctx context.Context, dst, src *PacketConn) ForwardPacketsResult {
	var res ForwardPacketResult
	var buf PacketHeaderCircularBuffer
	for {
		res = ForwardPacket(dst, src, forwardPacketOptions{})
		buf.add(res.packetHeader)
		if res.Error() != nil {
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
	return ForwardPacketsResult{
		PacketHeaderCircularBuffer: buf,
		ReadWriteError:             res.ReadWriteError,
		ServerWantsFin:             res.ServerWantsFin,
		ClientWantsFin:             res.ClientWantsFin,
	}
}

func ForwardPacket(dst, src *PacketConn, opt forwardPacketOptions) (res ForwardPacketResult) {
	src.readMu.Lock()
	_, isBuiltin, _, err := src.readPacketHeaderUnlocked(&res.packetHeader, DefaultPacketTimeout)
	src.readMu.Unlock()
	if err != nil {
		res.ReadErr = err
		return res
	}
	if opt.testEnv {
		switch res.packetHeader.tip {
		case packetTypeRPCNonce, packetTypeRPCHandshake:
			src.table = crc32.IEEETable
		default:
			src.table = castagnoliTable
		}
	}
	if isBuiltin {
		res.WriteErr = src.WritePacketBuiltin(DefaultPacketTimeout)
	} else {
		switch res.packetHeader.tip {
		case tl.RpcClientWantsFin{}.TLTag():
			res.ClientWantsFin = true
		case tl.RpcServerWantsFin{}.TLTag():
			res.ServerWantsFin = true
		}
		res.ReadWriteError = forwardPacket(dst, src, &res.packetHeader)
	}
	return res
}

var forwardPacketTrailer = [][]byte{{}, {0}, {0, 0}, {0, 0, 0}}

func forwardPacket(dst, src *PacketConn, header *packetHeader) (res ReadWriteError) {
	dst.writeMu.Lock()
	defer dst.writeMu.Unlock()
	// write header
	bodySize := header.length - packetOverhead
	// legacy RPC protocol used to align packet length to 4 bytes boundary, including
	// padding into checksum; packet length is guaranteed to be aligned by 4 bytes then
	var legacyWriteAlignTo4 uint32
	if dst.protocolVersion == 0 {
		legacyWriteAlignTo4 = -bodySize & 3
		bodySize += legacyWriteAlignTo4
	}
	if err := dst.writePacketHeaderUnlocked(header.tip, int(bodySize), DefaultPacketTimeout); err != nil {
		res.WriteErr = err
		return res
	}
	// write body
	if res = copyBodyCheckedSkipCryptoPadding(dst, src, header); res.Error() != nil {
		return res
	}
	if 0 < legacyWriteAlignTo4 && legacyWriteAlignTo4 < 4 {
		if err := dst.writePacketBodyUnlocked(forwardPacketTrailer[legacyWriteAlignTo4]); err != nil {
			res.WriteErr = err
			return res
		}
	}
	// write CRC and padding
	dst.writePacketTrailerUnlocked()
	res.WriteErr = dst.FlushUnlocked()
	return res
}

func copyBodyCheckedSkipCryptoPadding(dst, src *PacketConn, header *packetHeader) (res ReadWriteError) {
	src.readMu.Lock()
	defer src.readMu.Unlock()
	// copy body
	var crc uint32
	crc = crc32.Update(0, src.table, src.headerReadBuf[:12])
	crc, res = packetConnCopy(dst, src, int(header.length-packetOverhead), crc)
	if res.Error() != nil {
		return res
	}
	// read CRC
	if _, err := io.ReadFull(src.r, src.headerReadBuf[:4]); err != nil {
		res.ReadErr = err
		return res
	}
	readCRC := binary.LittleEndian.Uint32(src.headerReadBuf[:])
	// check CRC
	if crc != readCRC {
		res.ReadErr = &tagError{
			tag: "crc_mismatch",
			err: fmt.Errorf("CRC mismatch: read 0x%x, expected 0x%x", readCRC, crc),
		}
		return res
	}
	// skip crypto padding
	if src.w.isEncrypted() {
		res.ReadErr = src.r.discard(int(-header.length & 3))
	}
	return res
}

func (hctx *HandlerContext) WriteReponseAndFlush(conn *PacketConn, err error) error {
	hctx.extraStart = len(hctx.Response)
	err = hctx.prepareResponseBody(err)
	if err != nil {
		return err
	}
	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()
	err = hctx.writeReponseUnlocked(conn)
	if err != nil {
		return err
	}
	conn.FlushUnlocked()
	return nil
}

func (hctx *HandlerContext) ForwardAndFlush(conn *PacketConn, tip uint32, timeout time.Duration) error {
	switch tip {
	case tl.RpcCancelReq{}.TLTag(), tl.RpcClientWantsFin{}.TLTag():
		conn.writeMu.Lock()
		defer conn.writeMu.Unlock()
		err := writeCustomPacketUnlocked(conn, tip, hctx.Request, timeout)
		if err != nil {
			return err
		}
		return conn.FlushUnlocked()
	case tl.RpcInvokeReqHeader{}.TLTag():
		req := Request{
			Body:    hctx.Request,
			Extra:   hctx.RequestExtra,
			queryID: hctx.QueryID,
		}
		if err := preparePacket(&req); err != nil {
			return err
		}
		hctx.Request = req.Body[:0] // buffer reuse
		conn.writeMu.Lock()
		defer conn.writeMu.Unlock()
		if err := writeRequestUnlocked(conn, &req, timeout); err != nil {
			return err
		}
		if err := conn.FlushUnlocked(); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unknown packet type 0x%x", tip)
	}
}

func (hctx *HandlerContext) writeReponseUnlocked(conn *PacketConn) error {
	resp := hctx.Response
	extraStart := hctx.extraStart

	if err := conn.writePacketHeaderUnlocked(tl.RpcReqResultHeader{}.TLTag(), len(resp), DefaultPacketTimeout); err != nil {
		return err
	}
	// we serialize Extra after Body, so we have to twist spacetime a bit
	if err := conn.writePacketBodyUnlocked(resp[extraStart:]); err != nil {
		return err
	}
	if err := conn.writePacketBodyUnlocked(resp[:extraStart]); err != nil {
		return err
	}
	conn.writePacketTrailerUnlocked()
	return nil
}

func packetConnCopy(dst, src *PacketConn, n int, readCRC uint32) (uint32, ReadWriteError) {
	return cryptoCopy(dst.w, src.r, n, readCRC, src.table, dst.updateWriteCRC)
}

func cryptoCopy(dst *cryptoWriter, src *cryptoReader, n int, readCRC uint32, table *crc32.Table, cb func([]byte)) (_ uint32, res ReadWriteError) {
	if n == 0 {
		return readCRC, ReadWriteError{}
	}
	for {
		if m := src.end - src.begin; m > 0 {
			if m > n {
				m = n
			}
			s := src.buf[src.begin : src.begin+m]
			m, res.WriteErr = dst.Write(s)
			if res.WriteErr != nil {
				return readCRC, res
			}
			if table != nil {
				readCRC = crc32.Update(readCRC, table, s)
			}
			if cb != nil {
				cb(s)
			}
			src.begin += m
			n -= m
			if n == 0 {
				return readCRC, ReadWriteError{}
			}
		}
		if res.ReadErr != nil {
			return readCRC, res
		}
		buf := src.buf[:cap(src.buf)]
		bufSize := copy(buf, src.buf[src.end:])
		var read int
		read, res.ReadErr = src.Read(buf[bufSize:])
		bufSize += read
		src.buf = buf[:bufSize]
		src.begin = 0
		if src.enc != nil {
			decrypt := roundDownPow2(bufSize, src.blockSize)
			src.enc.CryptBlocks(buf[:decrypt], buf[:decrypt])
			src.end = decrypt
		} else {
			src.end = bufSize
		}
		if read <= 0 { // infinite loop guard
			res.ReadErr = errZeroRead
			return readCRC, res
		}
	}
}

func (src *cryptoReader) discard(n int) error {
	if n == 0 {
		return nil
	}
	// discard decrypted
	if m := src.end - src.begin; m > 0 {
		if m > n {
			m = n
		}
		src.begin += m
		n -= m
		if n == 0 {
			return nil
		}
	}
	// discard encrypted
	buf := src.buf[:cap(src.buf)]
	m := len(src.buf) - src.end
	src.begin, src.end = 0, 0
	if m > 0 {
		if m > n {
			m = n
		}
		src.buf = buf[:copy(buf, src.buf[src.end+m:])]
		n -= m
		if n == 0 {
			return nil
		}
	}
	// read and discard
	var err error
	for {
		var read int
		if read, err = src.r.Read(buf); read < n {
			if err != nil {
				buf = buf[:read]
				break // read error
			}
			if read <= 0 {
				buf = buf[:0]
				err = errZeroRead
				break // infinite loop
			}
			n -= read
		} else {
			buf = buf[:copy(buf, buf[n:read])]
			break // success
		}
	}
	// restore invariant by decrypting the read buffer
	src.buf = buf
	if src.enc != nil {
		decrypt := roundDownPow2(len(buf), src.blockSize)
		src.enc.CryptBlocks(buf[:decrypt], buf[:decrypt])
		src.end = decrypt
	} else {
		src.end = len(buf)
	}
	return err
}

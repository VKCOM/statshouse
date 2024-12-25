package rpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

var errZeroRead = fmt.Errorf("read returned zero bytes without an error")

type ReadWriteError struct {
	ReadErr  error
	WriteErr error
}

type ForwardPacketsResult struct {
	ReadWriteError
	ServerWantsFin bool
	ClientWantsFin bool
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

func ForwardPackets(ctx context.Context, dst, src *PacketConn) (res ForwardPacketsResult) {
	for i := 0; ctx.Err() == nil && res.Error() == nil; i++ {
		res = ForwardPacket(ctx, dst, src)
	}
	return res
}

func ForwardPacket(ctx context.Context, dst, src *PacketConn) (res ForwardPacketsResult) {
	var header packetHeader
	src.readMu.Lock()
	_, isBuiltin, _, err := src.readPacketHeaderUnlocked(&header, DefaultPacketTimeout)
	src.readMu.Unlock()
	if err != nil {
		res.ReadErr = err
		return res
	}
	if isBuiltin {
		res.WriteErr = src.WritePacketBuiltin(time.Duration(0))
	} else {
		switch header.tip {
		case tl.RpcClientWantsFin{}.TLTag():
			res.ClientWantsFin = true
		case tl.RpcServerWantsFin{}.TLTag():
			res.ServerWantsFin = true
		}
		res.ReadWriteError = forwardPacket(dst, src, header)
	}
	return res
}

var forwardPacketTrailer = [][]byte{{}, {0}, {0, 0}, {0, 0, 0}}

func forwardPacket(dst, src *PacketConn, header packetHeader) (res ReadWriteError) {
	dst.writeMu.Lock()
	defer dst.writeMu.Unlock()
	// write header
	srcBodySize := header.length - packetOverhead
	dstBodySize := srcBodySize
	// legacy RPC protocol used to align packet body length to 4 bytes boundary, including
	// padding into checksum; packet length is guaranteed to be aligned by 4 bytes then
	var legacyWriteAlignTo4 uint32
	if dst.protocolVersion == 0 {
		legacyWriteAlignTo4 = -srcBodySize & 3
		dstBodySize += legacyWriteAlignTo4
	}
	if err := dst.writePacketHeaderUnlocked(header.tip, int(dstBodySize), time.Duration(0)); err != nil {
		res.WriteErr = err
		return res
	}
	// write body
	if res = copyBodySkipCRCAndCryptoPadding(dst, src, srcBodySize); res.Error() != nil {
		return res
	}
	if 0 < legacyWriteAlignTo4 && legacyWriteAlignTo4 < 4 {
		// legacy RPC protocol, write body padding and CRC
		trailer := forwardPacketTrailer[legacyWriteAlignTo4]
		if _, err := dst.w.Write(trailer); err != nil {
			res.WriteErr = err
			return res
		}
		dst.updateWriteCRC(trailer)
		dst.headerWriteBuf = binary.LittleEndian.AppendUint32(dst.headerWriteBuf, dst.writeCRC)
	} else {
		// write CRC and padding
		dst.writePacketTrailerUnlocked()
	}
	res.WriteErr = dst.FlushUnlocked()
	return res
}

func copyBodySkipCRCAndCryptoPadding(dst, src *PacketConn, bodySize uint32) (res ReadWriteError) {
	src.readMu.Lock()
	defer src.readMu.Unlock()
	// copy body
	res = packetConnCopy(dst, src, int(bodySize))
	if res.Error() != nil {
		return res
	}
	// skip CRC
	if err := src.r.discard(4); err != nil {
		res.ReadErr = err
		return res
	}
	// skip crypto padding
	if src.w.isEncrypted() {
		res.ReadErr = src.r.discard(int(-bodySize & 3))
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

func packetConnCopy(dst, src *PacketConn, n int) ReadWriteError {
	return cryptoCopy(dst.w, src.r, n, dst.updateWriteCRC)
}

func cryptoCopy(dst *cryptoWriter, src *cryptoReader, n int, cb func([]byte)) (res ReadWriteError) {
	if n == 0 {
		return ReadWriteError{}
	}
	for {
		if m := src.end - src.begin; m > 0 {
			if m > n {
				m = n
			}
			s := src.buf[src.begin : src.begin+m]
			m, res.WriteErr = dst.Write(s)
			if res.WriteErr != nil {
				return res
			}
			if cb != nil {
				cb(s)
			}
			src.begin += m
			n -= m
			if n == 0 {
				return ReadWriteError{}
			}
		}
		if res.ReadErr != nil {
			return res
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
			return res
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

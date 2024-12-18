package rpc

import (
	"context"
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
	}
	return res
}

func forwardPacket(dst, src *PacketConn, header packetHeader) (res ReadWriteError) {
	dst.writeMu.Lock()
	defer dst.writeMu.Unlock()
	// write header
	bodySize := int(header.length) - packetOverhead
	if err := dst.writePacketHeaderUnlocked(header.tip, bodySize, time.Duration(0)); err != nil {
		res.WriteErr = err
		return res
	}
	// write body
	if res = copyBody(dst, src, header.length); res.Error() != nil {
		return res
	}
	// write CRC. padding and flush
	dst.writePacketTrailerUnlocked()
	res.WriteErr = dst.FlushUnlocked()
	return res
}

func copyBody(dst, src *PacketConn, headerLen uint32) (res ReadWriteError) {
	src.readMu.Lock()
	defer src.readMu.Unlock()
	// copy body
	bodySize := int(headerLen) - packetOverhead
	res = cryptoCopy(dst, src, bodySize)
	if res.Error() != nil {
		return res
	}
	// discard source CRC
	if err := src.r.discard(4); err != nil {
		res.ReadErr = err
		return res
	}
	// align read
	if src.w.isEncrypted() {
		res.ReadErr = src.r.discard(int(-uint(headerLen) & 3))
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

func cryptoCopy(dst, src *PacketConn, n int) (res ReadWriteError) {
	if n == 0 {
		return ReadWriteError{}
	}
	for {
		if m := src.r.end - src.r.begin; m > 0 {
			if m > n {
				m = n
			}
			s := src.r.buf[src.r.begin : src.r.begin+m]
			m, res.WriteErr = dst.w.Write(s)
			if res.WriteErr != nil {
				return res
			}
			dst.updateWriteCRC(s)
			src.r.begin += m
			n -= m
			if n == 0 {
				return ReadWriteError{}
			}
		}
		if res.ReadErr != nil {
			return res
		}
		buf := src.r.buf[:cap(src.r.buf)]
		bufSize := copy(buf, src.r.buf[src.r.end:])
		var read int
		read, res.ReadErr = src.r.Read(buf[bufSize:])
		bufSize += read
		src.r.buf = buf[:bufSize]
		src.r.begin = 0
		if src.r.enc != nil {
			decrypt := roundDownPow2(bufSize, src.r.blockSize)
			src.r.enc.CryptBlocks(buf[:decrypt], buf[:decrypt])
			src.r.end = decrypt
		} else {
			src.r.end = bufSize
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

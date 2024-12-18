package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

var errZeroRead = fmt.Errorf("read returned zero bytes without an error")

func (pc *PacketConn) KeyID() [4]byte {
	return pc.keyID
}

func ForwardPackets(ctx context.Context, dst, src *PacketConn) (error, error) {
	for i := 0; ctx.Err() == nil; i++ {
		var header packetHeader
		src.readMu.Lock()
		_, isBuiltin, _, err := src.readPacketHeaderUnlocked(&header, DefaultPacketTimeout)
		src.readMu.Unlock()
		var netErr net.Error
		if err != nil {
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			return nil, err // read error
		}
		if isBuiltin {
			if err = src.WritePacketBuiltin(time.Duration(0)); err != nil {
				return err, nil // write error
			}
			continue
		}
		dst.writeMu.Lock()
		// write header
		bodySize := int(header.length) - packetOverhead
		if err = dst.writePacketHeaderUnlocked(header.tip, bodySize, time.Duration(0)); err != nil {
			dst.writeMu.Unlock()
			return err, nil // write error
		}
		src.readMu.Lock()
		// copy body
		writeErr, readErr := cryptoCopy(dst.w, src.r, bodySize+4) // body and CRC
		// align read
		readWriteOK := readErr == nil && writeErr == nil
		if readWriteOK && src.w.isEncrypted() {
			src.r.discard(int(-uint(header.length) & 3))
		}
		src.readMu.Unlock()
		// align write
		if readWriteOK {
			dst.FlushUnlocked()
		}
		dst.writeMu.Unlock()
		if readErr != nil || writeErr != nil {
			return writeErr, readErr
		}
	}
	return nil, nil
}

func (req *HandlerContext) WriteReponseAndFlush(conn *PacketConn, err error) error {
	req.extraStart = len(req.Response)
	err = req.prepareResponseBody(err)
	if err != nil {
		return err
	}
	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()
	err = req.writeReponseUnlocked(conn)
	if err != nil {
		return err
	}
	conn.FlushUnlocked()
	return nil
}

func (req *HandlerContext) ForwardAndFlush(conn *PacketConn, tip uint32, timeout time.Duration) error {
	switch tip {
	case tl.RpcCancelReq{}.TLTag(), tl.RpcClientWantsFin{}.TLTag():
		conn.writeMu.Lock()
		defer conn.writeMu.Unlock()
		err := writeCustomPacketUnlocked(conn, tip, req.Request, timeout)
		if err != nil {
			return err
		}
		return conn.FlushUnlocked()
	case tl.RpcInvokeReqHeader{}.TLTag():
		fwd := Request{
			Body:    req.Request,
			Extra:   req.RequestExtra,
			queryID: req.QueryID,
		}
		if err := preparePacket(&fwd); err != nil {
			return err
		}
		conn.writeMu.Lock()
		defer conn.writeMu.Unlock()
		if err := writeRequestUnlocked(conn, &fwd, timeout); err != nil {
			return err
		}
		return conn.FlushUnlocked()
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

func cryptoCopy(dst *cryptoWriter, src *cryptoReader, n int) (error, error) {
	if n == 0 {
		return nil, nil
	}
	var readErr error
	for {
		if m := src.end - src.begin; m > 0 {
			if m > n {
				m = n
			}
			m, err := dst.Write(src.buf[src.begin : src.begin+m])
			if err != nil {
				return err, nil // write error
			}
			src.begin += m
			n -= m
			if n == 0 {
				return nil, nil
			}
		}
		if readErr != nil {
			return nil, readErr // read error
		}
		buf := src.buf[:cap(src.buf)]
		bufSize := copy(buf, src.buf[src.end:])
		var read int
		read, readErr = src.r.Read(buf[bufSize:])
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
		if read <= 0 {
			return nil, errZeroRead // guard against infinite loop
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

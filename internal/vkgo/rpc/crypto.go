// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"crypto/cipher"
	"fmt"
	"io"
)

type cryptoReader struct {
	r         io.Reader
	enc       cipher.BlockMode
	blockSize int
	buf       []byte
	begin     int
	end       int
}

var errZeroRead = fmt.Errorf("read returned zero bytes without an error")

func newCryptoReader(r io.Reader, bufSize int) *cryptoReader {
	if bufSize < 1 {
		bufSize = 1
	}
	return &cryptoReader{
		r:         r,
		blockSize: 1,
		buf:       make([]byte, 0, bufSize),
	}
}

func (r *cryptoReader) encrypt(enc cipher.BlockMode) {
	if r.enc != nil {
		panic("cryptoReader: changing encryption on the fly is not supported")
	}
	r.enc = enc
	r.blockSize = enc.BlockSize()
	if cap(r.buf) < r.blockSize {
		r.buf = append(make([]byte, 0, r.blockSize), r.buf...)
	}
	decrypt := roundDownPow2(len(r.buf)-r.begin, r.blockSize)
	r.end = r.begin + decrypt
	r.enc.CryptBlocks(r.buf[r.begin:r.end], r.buf[r.begin:r.end])
}

func (r *cryptoReader) Read(p []byte) (int, error) {
	n := copy(p, r.buf[r.begin:r.end])
	r.begin += n
	if n == len(p) {
		return n, nil
	}
	useBuf := roundDownPow2(len(p[n:]), r.blockSize) < roundDownPow2(cap(r.buf), r.blockSize)
	target := p[n:]
	if useBuf {
		target = r.buf[:cap(r.buf)]
	}
	tail := copy(target, r.buf[r.end:])
	m, err := r.r.Read(target[tail:])
	target = target[:tail+m]
	decrypt := roundDownPow2(len(target), r.blockSize)
	if r.enc != nil {
		r.enc.CryptBlocks(target[:decrypt], target[:decrypt])
	}
	if useBuf {
		c := copy(p[n:], target[:decrypt])
		n += c
		r.buf = target
		r.begin = c
		r.end = decrypt
	} else {
		n += decrypt
		c := copy(r.buf[:cap(r.buf)], target[decrypt:])
		r.buf = r.buf[:c]
		r.begin = 0
		r.end = 0
	}
	return n, err
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

type flusher interface {
	Flush() error
}

type cryptoWriter struct {
	w         io.Writer
	f         flusher
	enc       cipher.BlockMode
	blockSize int
	buf       []byte
	encStart  int
}

func newCryptoWriter(w io.Writer, bufSize int) *cryptoWriter {
	var f flusher
	if wf, ok := w.(flusher); ok {
		f = wf
	}
	if bufSize < 1 {
		bufSize = 1
	}
	return &cryptoWriter{
		w:         w,
		f:         f,
		blockSize: 1,
		buf:       make([]byte, 0, bufSize),
	}
}

func (w *cryptoWriter) encrypt(enc cipher.BlockMode) {
	if w.enc != nil {
		panic("cryptoWriter: changing encryption on the fly is not supported")
	}
	w.enc = enc
	w.encStart = len(w.buf)
	w.blockSize = enc.BlockSize()
	if cap(w.buf) < w.blockSize {
		w.buf = append(make([]byte, 0, w.blockSize), w.buf...)
	}
	if w.blockSize&(w.blockSize-1) != 0 { // Padding and roundDownPow2 functions expect this
		panic("cryptoWriter: block size not power of 2")
	}
}

func (w *cryptoWriter) isEncrypted() bool {
	return w.blockSize > 1
}

func (w *cryptoWriter) Write(p []byte) (int, error) {
	total := len(p)
	for {
		n := copy(w.buf[len(w.buf):cap(w.buf)], p)
		w.buf = w.buf[:len(w.buf)+n]
		p = p[n:]
		if len(p) == 0 {
			return total, nil
		}
		if _, err := w.flush(); err != nil {
			return 0, err
		}
	}
}

func (w *cryptoWriter) flush() (shouldFlushSocket bool, _ error) {
	n := w.encStart + roundDownPow2(len(w.buf)-w.encStart, w.blockSize)
	if n == 0 {
		return false, nil
	}
	// we may perform single extra socket flush when encryption was just turned on (encStart != 0)
	if w.enc != nil {
		w.enc.CryptBlocks(w.buf[w.encStart:n], w.buf[w.encStart:n])
		w.encStart = 0
	}
	if _, err := w.w.Write(w.buf[:n]); err != nil {
		w.buf = w.buf[:0]
		return true, err
	}
	tail := copy(w.buf, w.buf[n:])
	w.buf = w.buf[:tail]
	return true, nil
}

func (w *cryptoWriter) Flush() error {
	shouldFlushSocket, err := w.flush()
	if err != nil {
		return err
	}
	if !shouldFlushSocket || w.f == nil {
		return nil
	}
	return w.f.Flush()
}

func (w *cryptoWriter) Padding(afterNext int) int {
	return -(len(w.buf) + afterNext - w.encStart) & (w.blockSize - 1)
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

func roundDownPow2(i int, multiple int) int {
	return i & -multiple
}

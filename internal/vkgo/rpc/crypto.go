// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"crypto/cipher"
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
		if err := w.flush(); err != nil {
			return 0, err
		}
	}
}

func (w *cryptoWriter) flush() error {
	n := w.encStart + roundDownPow2(len(w.buf)-w.encStart, w.blockSize)
	if w.enc != nil {
		w.enc.CryptBlocks(w.buf[w.encStart:n], w.buf[w.encStart:n])
		w.encStart = 0
	}
	if _, err := w.w.Write(w.buf[:n]); err != nil {
		w.buf = w.buf[:0]
		return err
	}
	tail := copy(w.buf, w.buf[n:])
	w.buf = w.buf[:tail]
	return nil
}

func (w *cryptoWriter) Flush() error {
	if err := w.flush(); err != nil {
		return err
	}
	if w.f == nil {
		return nil
	}
	return w.f.Flush()
}

func roundDownPow2(i int, multiple int) int {
	return i & -multiple
}

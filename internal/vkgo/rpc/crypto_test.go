// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"io"
	"testing"

	"pgregory.net/rapid"
)

type cryptoRWMachine struct {
	buf      *bytes.Buffer
	r        *cryptoReader
	w        *cryptoWriter
	enc      cipher.BlockMode
	read     *bytes.Buffer
	written  *bytes.Buffer
	flushed  int
	encStart int
}

func (c *cryptoRWMachine) init(t *rapid.T) {
	rb := rapid.IntRange(0, 4*des.BlockSize).Draw(t, "rb")
	wb := rapid.IntRange(0, 4*des.BlockSize).Draw(t, "wb")

	c.buf = &bytes.Buffer{}
	c.r = newCryptoReader(c.buf, rb)
	c.w = newCryptoWriter(c.buf, wb)
	c.read = &bytes.Buffer{}
	c.written = &bytes.Buffer{}
}

func (c *cryptoRWMachine) Check(t *rapid.T) {
	nr, nw := c.read.Len(), c.written.Len()

	if nr > nw {
		t.Fatalf("read %v bytes, written %v bytes", nr, nw)
	}
	if !bytes.Equal(c.read.Bytes(), c.written.Bytes()[:nr]) {
		t.Fatalf("read %q, but written %q", c.read.Bytes(), c.written.Bytes()[:nr])
	}
}

func (c *cryptoRWMachine) Encrypt(t *rapid.T) {
	if c.enc != nil {
		t.Skip("already encrypted")
	}

	key := rapid.SliceOfN(rapid.Byte(), 8, 8).Draw(t, "key")
	e, err := des.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}

	c.encStart = c.written.Len()
	iv := rapid.SliceOfN(rapid.Byte(), des.BlockSize, des.BlockSize).Draw(t, "iv")
	c.w.encrypt(cipher.NewCBCEncrypter(e, iv))
	c.enc = cipher.NewCBCDecrypter(e, iv)
}

func (c *cryptoRWMachine) Read(t *rapid.T) {
	n := rapid.IntRange(0, 32768).Draw(t, "n")
	if c.encStart >= c.read.Len() && c.encStart < c.read.Len()+n && c.r.enc == nil {
		n = c.encStart - c.read.Len()
	}
	p := make([]byte, n)

	shouldReadFlushed := c.read.Len()+n >= c.flushed

	m, err := c.r.Read(p)
	if err != nil && err != io.EOF {
		t.Fatalf("read failed: %v", err)
	}
	if m > len(p) {
		t.Fatalf("long? read: %v instead of max %v", m, len(p))
	}
	if shouldReadFlushed && c.read.Len()+m < c.flushed {
		t.Fatalf("read only %v total, with %v flushed", c.read.Len()+m, c.flushed)
	}

	c.read.Write(p[:m])

	if c.w.enc != nil && c.r.enc == nil && c.encStart == c.read.Len() {
		c.r.encrypt(c.enc)
	}
}

func (c *cryptoRWMachine) Write(t *rapid.T) {
	p := rapid.SliceOf(rapid.Byte()).Draw(t, "p")
	q := append([]byte(nil), p...)

	n, err := c.w.Write(p)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != len(p) {
		t.Fatalf("short write: %v instead of %v", n, len(p))
	}
	if !bytes.Equal(p, q) {
		t.Fatalf("write buffer modified: %q instead of %q", p, q)
	}

	c.written.Write(p)
}

func (c *cryptoRWMachine) Flush(t *rapid.T) {
	err := c.w.Flush()
	if err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	if c.enc != nil {
		c.flushed = c.encStart + roundDownPow2(c.written.Len()-c.encStart, c.w.blockSize)
	} else {
		c.flushed = c.written.Len()
	}
}

func TestCryptoRWRoundtrip(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		var m cryptoRWMachine
		m.init(t)
		t.Repeat(rapid.StateMachineActions(&m))
	})
}

func BenchmarkCryptoWriter_Write(b *testing.B) {
	w := newCryptoWriter(io.Discard, 0)
	e, err := aes.NewCipher(make([]byte, 16))
	if err != nil {
		b.Fatal(err)
	}
	w.encrypt(cipher.NewCBCEncrypter(e, make([]byte, e.BlockSize())))
	b.ResetTimer()

	var msg [64]byte
	for i := 0; i < b.N; i++ {
		_, err = w.Write(msg[:])
		if err != nil {
			b.Fatal(err)
		}
	}
}

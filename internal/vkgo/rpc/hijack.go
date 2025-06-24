// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"net"
	"sync"
)

type HijackListener struct {
	ListenAddr net.Addr

	mu   sync.Mutex
	cond *sync.Cond // wakes Accept of socketHijacks

	closed        bool
	socketHijacks []net.Conn
}

var _ net.Listener = &HijackListener{}

func NewHijackListener(listenAddr net.Addr) *HijackListener {
	h := &HijackListener{ListenAddr: listenAddr}
	h.cond = sync.NewCond(&h.mu)
	return h
}

type HijackConnection struct {
	Magic []byte
	net.Conn
}

func isByteLetter(c byte) bool {
	return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}

// fast-path inlineable version of unicode.IsPrint for single-byte UTF-8 runes
func bytePrint(c byte) bool {
	return c >= 0x20 && c <= 0x7e
}

// TODO - feel free to improve this function. Magic is [0..12] bytes
func (h *HijackConnection) LooksLikeHTTP() bool {
	if len(h.Magic) == 0 || !isByteLetter(h.Magic[0]) {
		return false
	}
	for _, c := range h.Magic {
		if !bytePrint(c) {
			return false
		}
	}
	return true
}

func (h *HijackConnection) Read(p []byte) (int, error) {
	if len(h.Magic) != 0 {
		n := copy(p, h.Magic)
		h.Magic = h.Magic[n:]
		return n, nil
	}
	return h.Conn.Read(p)
}

func (h *HijackListener) AddConnection(conn net.Conn) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		_ = conn.Close()
		return
	}
	h.socketHijacks = append(h.socketHijacks, conn)
	h.mu.Unlock()
	h.cond.Broadcast()
}

func (h *HijackListener) Accept() (net.Conn, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for !(h.closed || len(h.socketHijacks) != 0) {
		h.cond.Wait()
	}
	if h.closed {
		return nil, net.ErrClosed
	}
	c := h.socketHijacks[0]
	h.socketHijacks = h.socketHijacks[1:] // naive queue is ok for us
	return c, nil
}

func (h *HijackListener) Addr() net.Addr {
	return h.ListenAddr
}

func (h *HijackListener) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	socketHijacks := h.socketHijacks
	h.socketHijacks = nil
	h.mu.Unlock()

	h.cond.Broadcast() // wake up all goroutines waiting on Accept, so they can return errors

	for _, s := range socketHijacks {
		_ = s.Close()
	}
	return nil
}

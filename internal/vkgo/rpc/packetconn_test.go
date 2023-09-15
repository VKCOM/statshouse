// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"bytes"
	"crypto/aes"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"
)

const (
	rwTimeout = 1000 * time.Millisecond // shorter timeout is better for shrinking, but can lead to flaky tests
)

type packetContent struct {
	packetType uint32
	body       []byte
}

type connEx struct {
	pc   *PacketConn
	send []packetContent
	recv []packetContent
}

type packetConnMachine struct {
	c1 *connEx
	c2 *connEx
}

func (p *packetConnMachine) init(t *rapid.T) {
	nc1, nc2 := net.Pipe()
	rb1 := rapid.IntRange(0, 4*aes.BlockSize).Draw(t, "rb1")
	wb1 := rapid.IntRange(0, 4*aes.BlockSize).Draw(t, "wb1")
	rb2 := rapid.IntRange(0, 4*aes.BlockSize).Draw(t, "rb2")
	wb2 := rapid.IntRange(0, 4*aes.BlockSize).Draw(t, "wb2")

	p.c1 = &connEx{
		pc: NewPacketConn(nc1, rb1, wb1, 0),
	}
	p.c2 = &connEx{
		pc: NewPacketConn(nc2, rb2, wb2, 0),
	}

	enc := rapid.Bool().Draw(t, "enc")
	if enc {
		key1 := rapid.SliceOfN(rapid.Byte(), 16, 16).Draw(t, "key1")
		key2 := rapid.SliceOfN(rapid.Byte(), 16, 16).Draw(t, "key2")
		iv1 := rapid.SliceOfN(rapid.Byte(), aes.BlockSize, aes.BlockSize).Draw(t, "iv1")
		iv2 := rapid.SliceOfN(rapid.Byte(), aes.BlockSize, aes.BlockSize).Draw(t, "iv2")

		err := p.c1.pc.encrypt(key1, iv1, key2, iv2)
		if err != nil {
			t.Fatal(err)
		}

		err = p.c2.pc.encrypt(key2, iv2, key1, iv1)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func (p *packetConnMachine) cleanup() {
	_ = p.c1.pc.Close()
	_ = p.c2.pc.Close()
}

func (p *packetConnMachine) Check(t *rapid.T) {
	if i := len(p.c1.send) - 1; i >= 0 {
		if !equalPackets(p.c1.send[i], p.c2.recv[i]) {
			t.Fatalf("c1 send %#v, c2 recv %#v", p.c1.send[i], p.c2.recv[i])
		}
	}
	if i := len(p.c2.send) - 1; i >= 0 {
		if !equalPackets(p.c2.send[i], p.c1.recv[i]) {
			t.Fatalf("c2 send %#v, c1 recv %#v", p.c2.send[i], p.c2.recv[i])
		}
	}
}

func equalPackets(p packetContent, q packetContent) bool {
	return p.packetType == q.packetType && bytes.Equal(p.body, q.body)
}

func (p *packetConnMachine) Send12(t *rapid.T) {
	send(t, p.c1, p.c2)
}

func (p *packetConnMachine) Send21(t *rapid.T) {
	send(t, p.c2, p.c1)
}

func send(t *rapid.T, from *connEx, to *connEx) {
	t.Helper()

	pc := packetContent{
		body: bytes.Repeat(rapid.SliceOf(rapid.Byte()).Draw(t, "body"), 4),
	}
	if from.pc.writeSeqNum == startSeqNum {
		pc.packetType = packetTypeRPCNonce
	} else {
		pc.packetType = rapid.Uint32().Draw(t, "type")
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)

	go func() {
		defer wg.Done()

		err := from.pc.WritePacket(pc.packetType, pc.body, rwTimeout)
		if err != nil {
			err = fmt.Errorf("failed to write packet: %w", err)
		}
		errCh <- err

		from.send = append(from.send, pc)
	}()

	go func() {
		defer wg.Done()

		typ, b, err := to.pc.ReadPacket(nil, rwTimeout)
		if err != nil {
			err = fmt.Errorf("failed to read packet: %w", err)
		}
		errCh <- err

		to.recv = append(to.recv, packetContent{
			packetType: typ,
			body:       b,
		})
	}()

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPacketConn(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		var m packetConnMachine
		m.init(t)
		defer m.cleanup()
		t.Repeat(rapid.StateMachineActions(&m))
	})
}

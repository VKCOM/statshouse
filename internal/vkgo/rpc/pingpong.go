// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"fmt"
	"time"

	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
)

func (pc *PacketConn) WritePacketBuiltinNoFlushUnlocked(timeout time.Duration) error {
	pc.pingMu.Lock()
	defer pc.pingMu.Unlock()
	if !pc.writePing && !pc.writePong {
		return nil
	}

	if pc.writePing {
		if err := pc.WritePacketNoFlushUnlocked(PacketTypeRPCPing, pc.writePingID[:], timeout); err != nil {
			return err
		}
		pc.writePing = false
	}
	if pc.writePong {
		if err := pc.WritePacketNoFlushUnlocked(PacketTypeRPCPong, pc.writePongID[:], timeout); err != nil {
			return err
		}
		pc.writePong = false
	}
	return nil
}

func (pc *PacketConn) WritePacketBuiltin(timeout time.Duration) error {
	pc.pingMu.Lock()
	defer pc.pingMu.Unlock()
	if !pc.writePing && !pc.writePong {
		return nil
	}
	pc.writeMu.Lock()
	defer pc.writeMu.Unlock()

	if pc.writePing {
		if err := pc.WritePacketNoFlushUnlocked(PacketTypeRPCPing, pc.writePingID[:], timeout); err != nil {
			return err
		}
		pc.writePing = false
	}
	if pc.writePong {
		if err := pc.WritePacketNoFlushUnlocked(PacketTypeRPCPong, pc.writePongID[:], timeout); err != nil {
			return err
		}
		pc.writePong = false
	}
	return pc.FlushUnlocked()
}

func (pc *PacketConn) sendPing() bool {
	pc.pingMu.Lock()
	defer pc.pingMu.Unlock()
	if pc.pingSent {
		return false
	}
	pc.pingSent = true // will be cleared by received pong
	pc.currentPingID++
	pc.writePing = true
	_ = basictl.LongWrite(pc.writePingID[:0], pc.currentPingID)
	return true
}

func (pc *PacketConn) onPing(body []byte) error {
	var pingID int64
	if _, err := basictl.LongRead(body, &pingID); err != nil {
		return fmt.Errorf("failed to parse ping packet %w", err)
	}
	pc.pingMu.Lock()
	defer pc.pingMu.Unlock()
	if pc.writePong {
		return fmt.Errorf("received next ping %d before previous pong was written", pingID)
	}
	pc.writePong = true
	_ = basictl.LongWrite(pc.writePongID[:0], pingID)
	return nil
}

func (pc *PacketConn) onPong(body []byte) error {
	var pingID int64
	if _, err := basictl.LongRead(body, &pingID); err != nil {
		return fmt.Errorf("failed to parse pong packet %w", err)
	}
	pc.pingMu.Lock()
	defer pc.pingMu.Unlock()
	if !pc.pingSent {
		return fmt.Errorf("received unexpected pong %d without sending ping", pingID)
	}
	if pingID != pc.currentPingID {
		return fmt.Errorf("received unexpected pong %d for ping %d", pingID, pc.currentPingID)
	}
	pc.pingSent = false
	return nil
}

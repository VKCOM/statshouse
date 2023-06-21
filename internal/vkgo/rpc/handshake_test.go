// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"pgregory.net/rapid"
)

func TestNetPIDRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		pid1 := NetPID{
			Ip:      rapid.Uint32().Draw(t, "ip"),
			PortPid: rapid.Uint32().Draw(t, "portPid"),
			Utime:   rapid.Uint32().Draw(t, "time"),
		}

		b1, _ := pid1.Write(nil)

		var b2 bytes.Buffer
		err := binary.Write(&b2, binary.LittleEndian, pid1)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b1, b2.Bytes()) {
			t.Fatalf("got 0x%x instead of 0x%x", b1, b2.Bytes())
		}

		var pid2 NetPID
		_, err = pid2.Read(b1)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(pid1, pid2) {
			t.Fatalf("read back %#v, expected %#v", pid2, pid1)
		}
	})
}

func TestInvokeExtraRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		extra1 := InvokeReqExtra{}
		if rapid.Bool().Draw(t, "WaitBinlogPos") {
			extra1.SetWaitBinlogPos(rapid.Int64().Draw(t, "WaitBinlogPos"))
		}
		if rapid.Bool().Draw(t, "SetStringForwardKeys") {
			extra1.SetStringForwardKeys(rapid.SliceOfN(rapid.String(), 0, 2).Draw(t, "SetStringForwardKeys"))
			if len(extra1.StringForwardKeys) == 0 {
				extra1.StringForwardKeys = nil
			}
		}
		if rapid.Bool().Draw(t, "SetIntForwardKeys") {
			extra1.SetIntForwardKeys(rapid.SliceOfN(rapid.Int64(), 0, 2).Draw(t, "SetIntForwardKeys"))
			if len(extra1.IntForwardKeys) == 0 {
				extra1.IntForwardKeys = nil
			}
		}
		if rapid.Bool().Draw(t, "StringForward") {
			extra1.SetStringForward(rapid.String().Draw(t, "StringForward"))
		}
		if rapid.Bool().Draw(t, "IntForward") {
			extra1.SetIntForward(rapid.Int64().Draw(t, "IntForward"))
		}
		if rapid.Bool().Draw(t, "CustomTimeoutMs") {
			extra1.SetCustomTimeoutMs(rapid.Int32().Draw(t, "CustomTimeoutMs"))
		}
		if rapid.Bool().Draw(t, "SupportedCompressionVersion") {
			extra1.SetSupportedCompressionVersion(rapid.Int32().Draw(t, "SupportedCompressionVersion"))
		}
		if rapid.Bool().Draw(t, "RandomDelay") {
			extra1.SetRandomDelay(rapid.Float64().Draw(t, "RandomDelay"))
		}
		if rapid.Bool().Draw(t, "ReturnBinlogPos") {
			extra1.SetReturnBinlogPos(true)
		}
		if rapid.Bool().Draw(t, "NoResult") {
			extra1.SetNoResult(true)
		}

		b1, _ := extra1.Write(nil)

		var extra2 InvokeReqExtra
		if _, err := extra2.Read(b1); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(extra1, extra2) {
			t.Fatalf("read back %#v, expected %#v", extra2, extra1)
		}
	})
}

func TestResultExtraRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		pid1 := NetPID{
			Ip:      rapid.Uint32().Draw(t, "ip"),
			PortPid: rapid.Uint32().Draw(t, "portPid"),
			Utime:   rapid.Uint32().Draw(t, "time"),
		}

		extra1 := ReqResultExtra{}
		if rapid.Bool().Draw(t, "BinlogPos") {
			extra1.SetBinlogPos(rapid.Int64().Draw(t, "BinlogPos"))
		}
		if rapid.Bool().Draw(t, "BinlogTime") {
			extra1.SetBinlogTime(rapid.Int64().Draw(t, "BinlogTime"))
		}
		if rapid.Bool().Draw(t, "EnginePID") {
			extra1.SetEnginePid(pid1)
		}
		if rapid.Bool().Draw(t, "SetRequestSize") {
			extra1.SetRequestSize(rapid.Int32().Draw(t, "SetRequestSize"))
		}
		if rapid.Bool().Draw(t, "SetResponseSize") {
			extra1.SetResponseSize(rapid.Int32().Draw(t, "SetResponseSize"))
		}
		if rapid.Bool().Draw(t, "SetFailedSubqueries") {
			extra1.SetFailedSubqueries(rapid.Int32().Draw(t, "SetFailedSubqueries"))
		}
		if rapid.Bool().Draw(t, "SetCompressionVersion") {
			extra1.SetCompressionVersion(rapid.Int32().Draw(t, "SetCompressionVersion"))
		}
		if rapid.Bool().Draw(t, "SetStats") {
			extra1.SetStats(rapid.MapOfN(rapid.String(), rapid.String(), 0, 2).Draw(t, "SetStats"))
			if len(extra1.Stats) == 0 {
				extra1.Stats = nil // canonical form
			}
		}
		if rapid.Bool().Draw(t, "SetViewNumber") {
			extra1.SetViewNumber(rapid.Int64Min(0).Draw(t, "SetViewNumber"))
			if rapid.Bool().Draw(t, "SetEpochNumber") {
				extra1.EpochNumber = rapid.Int64Min(0).Draw(t, "SetEpochNumber")
			}
		}

		b1, _ := extra1.Write(nil)

		var extra2 ReqResultExtra
		if _, err := extra2.Read(b1); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(extra1, extra2) {
			t.Fatalf("read back %#v, expected %#v", extra2, extra1)
		}
	})
}

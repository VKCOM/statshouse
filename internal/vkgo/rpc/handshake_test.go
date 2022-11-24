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

	"github.com/vkcom/statshouse/internal/vkgo/tlrw"
)

// TODO - remove bytes.Buffer version of these structs from tests and code

func TestNetPIDRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		pid1 := NetPID{
			IP:   rapid.Uint32().Draw(t, "ip").(uint32),
			Port: rapid.Uint16().Draw(t, "port").(uint16),
			PID:  rapid.Uint16().Draw(t, "pid").(uint16),
			Time: rapid.Int32().Draw(t, "time").(int32),
		}

		var b1 bytes.Buffer
		pid1.writeToByteBuffer(&b1)
		b3 := pid1.write(nil)
		if !bytes.Equal(b3, b1.Bytes()) {
			t.Fatalf("got 0x%x instead of 0x%x", b3, b1.Bytes())
		}

		var b2 bytes.Buffer
		err := binary.Write(&b2, binary.LittleEndian, pid1)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b1.Bytes(), b2.Bytes()) {
			t.Fatalf("got 0x%x instead of 0x%x", b1.Bytes(), b2.Bytes())
		}

		var pid2 NetPID
		err = pid2.readFromBytesBuffer(&b1)
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
		if rapid.Bool().Draw(t, "WaitBinlogPos").(bool) {
			extra1.SetWaitBinlogPos(rapid.Int64().Draw(t, "WaitBinlogPos").(int64))
		}
		if rapid.Bool().Draw(t, "SetStringForwardKeys").(bool) {
			extra1.SetStringForwardKeys(rapid.SliceOfN(rapid.String(), 0, 2).Draw(t, "SetStringForwardKeys").([]string))
			if len(extra1.StringForwardKeys) == 0 {
				extra1.StringForwardKeys = nil
			}
		}
		if rapid.Bool().Draw(t, "SetIntForwardKeys").(bool) {
			extra1.SetIntForwardKeys(rapid.SliceOfN(rapid.Int64(), 0, 2).Draw(t, "SetIntForwardKeys").([]int64))
			if len(extra1.IntForwardKeys) == 0 {
				extra1.IntForwardKeys = nil
			}
		}
		if rapid.Bool().Draw(t, "StringForward").(bool) {
			extra1.SetStringForward(rapid.String().Draw(t, "StringForward").(string))
		}
		if rapid.Bool().Draw(t, "IntForward").(bool) {
			extra1.SetIntForward(rapid.Int64().Draw(t, "IntForward").(int64))
		}
		if rapid.Bool().Draw(t, "CustomTimeoutMs").(bool) {
			extra1.SetCustomTimeoutMs(rapid.Int32().Draw(t, "CustomTimeoutMs").(int32))
		}
		if rapid.Bool().Draw(t, "SupportedCompressionVersion").(bool) {
			extra1.SetSupportedCompressionVersion(rapid.Int32().Draw(t, "SupportedCompressionVersion").(int32))
		}
		if rapid.Bool().Draw(t, "RandomDelay").(bool) {
			extra1.SetRandomDelay(rapid.Float64().Draw(t, "RandomDelay").(float64))
		}
		if rapid.Bool().Draw(t, "ReturnBinlogPos").(bool) {
			extra1.SetReturnBinlogPos()
		}
		if rapid.Bool().Draw(t, "NoResult").(bool) {
			extra1.SetNoResult()
		}

		var b1 bytes.Buffer
		tlrw.WriteUint32(&b1, extra1.flags)
		_ = extra1.writeToBytesBuffer(&b1)
		b3, _ := extra1.Write(nil)
		if !bytes.Equal(b3, b1.Bytes()) {
			t.Fatalf("got 0x%x instead of 0x%x", b3, b1.Bytes())
		}

		var extra2 InvokeReqExtra
		if _, err := extra2.Read(b1.Bytes()); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(extra1, extra2) {
			t.Fatalf("read back %#v, expected %#v", extra2, extra1)
		}
		var extra3 InvokeReqExtra
		if err := tlrw.ReadUint32(&b1, &extra3.flags); err != nil {
			t.Fatal(err)
		}
		if err := extra3.readFromBytesBuffer(&b1); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(extra1, extra3) {
			t.Fatalf("read back %#v, expected %#v", extra3, extra1)
		}
	})
}

func TestResultExtraRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		pid1 := NetPID{
			IP:   rapid.Uint32().Draw(t, "ip").(uint32),
			Port: rapid.Uint16().Draw(t, "port").(uint16),
			PID:  rapid.Uint16().Draw(t, "pid").(uint16),
			Time: rapid.Int32().Draw(t, "time").(int32),
		}

		extra1 := ReqResultExtra{}
		if rapid.Bool().Draw(t, "BinlogPos").(bool) {
			extra1.SetBinlogPos(rapid.Int64().Draw(t, "BinlogPos").(int64))
		}
		if rapid.Bool().Draw(t, "BinlogTime").(bool) {
			extra1.SetBinlogTime(rapid.Int64().Draw(t, "BinlogTime").(int64))
		}
		if rapid.Bool().Draw(t, "EnginePID").(bool) {
			extra1.SetEnginePID(pid1)
		}
		if rapid.Bool().Draw(t, "SetRequestSize").(bool) {
			extra1.SetRequestSize(rapid.Int32().Draw(t, "SetRequestSize").(int32))
		}
		if rapid.Bool().Draw(t, "SetResponseSize").(bool) {
			extra1.SetResponseSize(rapid.Int32().Draw(t, "SetResponseSize").(int32))
		}
		if rapid.Bool().Draw(t, "SetFailedSubqueries").(bool) {
			extra1.SetFailedSubqueries(rapid.Int32().Draw(t, "SetFailedSubqueries").(int32))
		}
		if rapid.Bool().Draw(t, "SetCompressionVersion").(bool) {
			extra1.SetCompressionVersion(rapid.Int32().Draw(t, "SetCompressionVersion").(int32))
		}
		if rapid.Bool().Draw(t, "SetStats").(bool) {
			extra1.SetStats(rapid.MapOfN(rapid.String(), rapid.String(), 0, 2).Draw(t, "SetStats").(map[string]string))
			if len(extra1.Stats) == 0 {
				extra1.Stats = map[string]string{}
			}
		}

		var b1 bytes.Buffer
		tlrw.WriteUint32(&b1, extra1.flags)
		_ = extra1.writeToBytesBuffer(&b1)
		b3, _ := extra1.Write(nil)
		if len(extra1.Stats) == 0 && !bytes.Equal(b3, b1.Bytes()) { // map serialization is random
			t.Fatalf("got 0x%x instead of 0x%x", b3, b1.Bytes())
		}

		var extra2 ReqResultExtra
		if _, err := extra2.Read(b1.Bytes()); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(extra1, extra2) {
			t.Fatalf("read back %#v, expected %#v", extra2, extra1)
		}
		var extra3 ReqResultExtra
		if err := tlrw.ReadUint32(&b1, &extra3.flags); err != nil {
			t.Fatal(err)
		}
		if err := extra3.readFromBytesBuffer(&b1); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(extra1, extra3) {
			t.Fatalf("read back %#v, expected %#v", extra3, extra1)
		}
	})
}

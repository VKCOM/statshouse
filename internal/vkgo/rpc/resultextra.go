// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"bytes"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/tlrw"
)

// ReqResultExtra описывает следующий комбинатор:
//
//	rpcReqResultExtra {flags:#} binlog_pos:flags.0?%Long binlog_time:flags.1?%Long engine_pid:flags.2?%net.Pid request_size:flags.3?%Int response_size:flags.3?%Int failed_subqueries:flags.4?%Int compression_version:flags.5?%Int stats:flags.6?%(Dictionary %String) = RpcReqResultExtra flags
type ReqResultExtra struct {
	flags uint32

	BinlogPos          int64             // Conditional: {flags}.0
	BinlogTime         int64             // Conditional: {flags}.1
	EnginePID          NetPID            // Conditional: {flags}.2
	RequestSize        int32             // Conditional: {flags}.3
	ResponseSize       int32             // Conditional: {flags}.3
	FailedSubqueries   int32             // Conditional: {flags}.4
	CompressionVersion int32             // Conditional: {flags}.5
	Stats              map[string]string // Conditional: {flags}.6
}

func (e *ReqResultExtra) SetBinlogPos(v int64) {
	e.BinlogPos = v
	e.flags |= 1 << 0
}

func (e *ReqResultExtra) IsSetBinlogPos() bool {
	return e.flags&(1<<0) != 0
}

func (e *ReqResultExtra) SetBinlogTime(v int64) {
	e.BinlogTime = v
	e.flags |= 1 << 1
}

func (e *ReqResultExtra) IsSetBinlogTime() bool {
	return e.flags&(1<<1) != 0
}

func (e *ReqResultExtra) SetEnginePID(v NetPID) {
	e.EnginePID = v
	e.flags |= 1 << 2
}

func (e *ReqResultExtra) IsSetEnginePID() bool {
	return e.flags&(1<<2) != 0
}

func (e *ReqResultExtra) SetRequestSize(v int32) {
	e.RequestSize = v
	e.flags |= 1 << 3
}

func (e *ReqResultExtra) IsSetRequestSize() bool {
	return e.flags&(1<<3) != 0
}

func (e *ReqResultExtra) SetResponseSize(v int32) {
	e.ResponseSize = v
	e.flags |= 1 << 3
}

func (e *ReqResultExtra) IsSetResponseSize() bool {
	return e.flags&(1<<3) != 0
}

func (e *ReqResultExtra) SetFailedSubqueries(v int32) {
	e.FailedSubqueries = v
	e.flags |= 1 << 4
}

func (e *ReqResultExtra) IsSetFailedSubqueries() bool {
	return e.flags&(1<<4) != 0
}

func (e *ReqResultExtra) SetCompressionVersion(v int32) {
	e.CompressionVersion = v
	e.flags |= 1 << 5
}

func (e *ReqResultExtra) IsSetCompressionVersion() bool {
	return e.flags&(1<<5) != 0
}

func (e *ReqResultExtra) SetStats(v map[string]string) {
	e.Stats = v
	e.flags |= 1 << 6
}

func (e *ReqResultExtra) IsSetStats() bool {
	return e.flags&(1<<6) != 0
}

func (e *ReqResultExtra) readFromBytesBuffer(r *bytes.Buffer) error {
	if e.flags&(1<<0) != 0 {
		if err := tlrw.ReadInt64(r, &e.BinlogPos); err != nil {
			return err
		}
	}
	if e.flags&(1<<1) != 0 {
		if err := tlrw.ReadInt64(r, &e.BinlogTime); err != nil {
			return err
		}
	}
	if e.flags&(1<<2) != 0 {
		if err := e.EnginePID.readFromBytesBuffer(r); err != nil {
			return err
		}
	}
	if e.flags&(1<<3) != 0 {
		if err := tlrw.ReadInt32(r, &e.RequestSize); err != nil {
			return err
		}
		if err := tlrw.ReadInt32(r, &e.ResponseSize); err != nil {
			return err
		}
	}
	if e.flags&(1<<4) != 0 {
		if err := tlrw.ReadInt32(r, &e.FailedSubqueries); err != nil {
			return err
		}
	}
	if e.flags&(1<<5) != 0 {
		if err := tlrw.ReadInt32(r, &e.CompressionVersion); err != nil {
			return err
		}
	}
	if e.flags&(1<<6) != 0 {
		var _l uint32
		if err := tlrw.ReadUint32(r, &_l); err != nil {
			return err
		}
		_data := make(map[string]string, _l)
		for i := 0; i < int(_l); i++ {
			var _key string
			if err := tlrw.ReadString(r, &_key); err != nil {
				return err
			}
			var _val string
			if err := tlrw.ReadString(r, &_val); err != nil {
				return err
			}
			_data[_key] = _val
		}
		e.Stats = _data
	}

	return nil
}

func (e *ReqResultExtra) writeToBytesBuffer(w *bytes.Buffer) error {
	if e.flags&(1<<0) != 0 {
		tlrw.WriteInt64(w, e.BinlogPos)
	}
	if e.flags&(1<<1) != 0 {
		tlrw.WriteInt64(w, e.BinlogTime)
	}
	if e.flags&(1<<2) != 0 {
		e.EnginePID.writeToByteBuffer(w)
	}
	if e.flags&(1<<3) != 0 {
		tlrw.WriteInt32(w, e.RequestSize)
		tlrw.WriteInt32(w, e.ResponseSize)
	}
	if e.flags&(1<<4) != 0 {
		tlrw.WriteInt32(w, e.FailedSubqueries)
	}
	if e.flags&(1<<5) != 0 {
		tlrw.WriteInt32(w, e.CompressionVersion)
	}
	if e.flags&(1<<6) != 0 {
		tlrw.WriteUint32(w, uint32(len(e.Stats)))
		for _key, _val := range e.Stats {
			if err := tlrw.WriteString(w, _key); err != nil {
				return err
			}
			if err := tlrw.WriteString(w, _val); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *ReqResultExtra) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &e.flags); err != nil {
		return w, err
	}
	if e.flags&(1<<0) != 0 {
		if w, err = basictl.LongRead(w, &e.BinlogPos); err != nil {
			return w, err
		}
	} else {
		e.BinlogPos = 0
	}
	if e.flags&(1<<1) != 0 {
		if w, err = basictl.LongRead(w, &e.BinlogTime); err != nil {
			return w, err
		}
	} else {
		e.BinlogTime = 0
	}
	if e.flags&(1<<2) != 0 {
		if w, err = e.EnginePID.read(w); err != nil {
			return w, err
		}
	} else {
		e.EnginePID = NetPID{}
	}
	if e.flags&(1<<3) != 0 {
		if w, err = basictl.IntRead(w, &e.RequestSize); err != nil {
			return w, err
		}
	} else {
		e.RequestSize = 0
	}
	if e.flags&(1<<3) != 0 {
		if w, err = basictl.IntRead(w, &e.ResponseSize); err != nil {
			return w, err
		}
	} else {
		e.ResponseSize = 0
	}
	if e.flags&(1<<4) != 0 {
		if w, err = basictl.IntRead(w, &e.FailedSubqueries); err != nil {
			return w, err
		}
	} else {
		e.FailedSubqueries = 0
	}
	if e.flags&(1<<5) != 0 {
		if w, err = basictl.IntRead(w, &e.CompressionVersion); err != nil {
			return w, err
		}
	} else {
		e.CompressionVersion = 0
	}
	if e.flags&(1<<6) != 0 {
		if w, err = vectorDictionaryFieldStringRead(w, &e.Stats); err != nil {
			return w, err
		}
	} else {
		vectorDictionaryFieldStringReset(e.Stats)
	}
	return w, nil
}

func (e *ReqResultExtra) Write(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, e.flags)
	if e.flags&(1<<0) != 0 {
		w = basictl.LongWrite(w, e.BinlogPos)
	}
	if e.flags&(1<<1) != 0 {
		w = basictl.LongWrite(w, e.BinlogTime)
	}
	if e.flags&(1<<2) != 0 {
		w = e.EnginePID.write(w)
	}
	if e.flags&(1<<3) != 0 {
		w = basictl.IntWrite(w, e.RequestSize)
	}
	if e.flags&(1<<3) != 0 {
		w = basictl.IntWrite(w, e.ResponseSize)
	}
	if e.flags&(1<<4) != 0 {
		w = basictl.IntWrite(w, e.FailedSubqueries)
	}
	if e.flags&(1<<5) != 0 {
		w = basictl.IntWrite(w, e.CompressionVersion)
	}
	if e.flags&(1<<6) != 0 {
		if w, err = vectorDictionaryFieldStringWrite(w, e.Stats); err != nil {
			return w, err
		}
	}
	return w, nil
}

func vectorDictionaryFieldStringReset(m map[string]string) {
	for k := range m {
		delete(m, k)
	}
}

func vectorDictionaryFieldStringRead(w []byte, m *map[string]string) (_ []byte, err error) {
	var l uint32
	if w, err = basictl.NatRead(w, &l); err != nil { // No sanity check required for map
		return w, err
	}
	var data map[string]string
	if *m == nil {
		data = make(map[string]string, l)
		*m = data
	} else {
		data = *m
		for k := range data {
			delete(data, k)
		}
	}
	for i := 0; i < int(l); i++ {
		var elem DictionaryFieldString
		if w, err = elem.Read(w); err != nil {
			return w, err
		}
		data[elem.Key] = elem.Value
	}
	return w, nil
}

func vectorDictionaryFieldStringWrite(w []byte, m map[string]string) (_ []byte, err error) {
	w = basictl.NatWrite(w, uint32(len(m)))
	for key, val := range m {
		elem := DictionaryFieldString{Key: key, Value: val}
		if w, err = elem.Write(w); err != nil {
			return w, err
		}
	}
	return w, nil
}

type DictionaryFieldString struct {
	Key   string
	Value string
}

func (DictionaryFieldString) TLName() string { return "dictionaryField" }
func (DictionaryFieldString) TLTag() uint32  { return 0x239c1b62 }

func (e *DictionaryFieldString) Reset() {
	e.Key = ""
	e.Value = ""
}

func (e *DictionaryFieldString) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.StringRead(w, &e.Key); err != nil {
		return w, err
	}
	return basictl.StringRead(w, &e.Value)
}

func (e *DictionaryFieldString) Write(w []byte) (_ []byte, err error) {
	if w, err = basictl.StringWrite(w, e.Key); err != nil {
		return w, err
	}
	return basictl.StringWrite(w, e.Value)
}

func (e *DictionaryFieldString) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x239c1b62); err != nil {
		return w, err
	}
	return e.Read(w)
}

func (e *DictionaryFieldString) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x239c1b62)
	return e.Write(w)
}

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

// InvokeReqExtra описывает следующий комбинатор:
//
//	rpcInvokeReqExtra {flags:#}
//		return_binlog_pos:flags.0?%True
//		return_binlog_time:flags.1?%True
//		return_pid:flags.2?%True
//		return_request_sizes:flags.3?%True
//		return_failed_subqueries:flags.4?%True
//		return_query_stats:flags.6?%True
//		no_result:flags.7?%True
//		wait_binlog_pos:flags.16?%Long
//		string_forward_keys:flags.18?%(Vector %String)
//		int_forward_keys:flags.19?%(Vector %Long)
//		string_forward:flags.20?%String
//		int_forward:flags.21?%Long
//		custom_timeout_ms:flags.23?%Int
//		supported_compression_version:flags.25?%Int
//		random_delay:flags.26?%Double
//		= RpcInvokeReqExtra flags
type InvokeReqExtra struct {
	flags uint32

	WaitBinlogPos               int64    // Conditional: {flags}.16
	StringForwardKeys           []string // Conditional: {flags}.18
	IntForwardKeys              []int64  // Conditional: {flags}.19
	StringForward               string   // Conditional: {flags}.20
	IntForward                  int64    // Conditional: {flags}.21
	CustomTimeoutMs             int32    // Conditional: {flags}.23
	SupportedCompressionVersion int32    // Conditional: {flags}.25
	RandomDelay                 float64  // Conditional: {flags}.26

	FailIfNoConnection bool // Experimental. Not serialized. Requests fail immediately when connection fails, so that switch to fallback is faster
}

func (e *InvokeReqExtra) SetReturnBinlogPos() {
	e.flags |= 1 << 0
}

func (e *InvokeReqExtra) IsSetReturnBinlogPos() bool {
	return e.flags&(1<<0) != 0
}

func (e *InvokeReqExtra) SetReturnBinlogTime() {
	e.flags |= 1 << 1
}

func (e *InvokeReqExtra) IsSetReturnBinlogTime() bool {
	return e.flags&(1<<1) != 0
}

func (e *InvokeReqExtra) SetReturnPid() {
	e.flags |= 1 << 2
}

func (e *InvokeReqExtra) IsSetReturnPid() bool {
	return e.flags&(1<<2) != 0
}

func (e *InvokeReqExtra) SetReturnRequestSizes() {
	e.flags |= 1 << 3
}

func (e *InvokeReqExtra) IsSetReturnRequestSizes() bool {
	return e.flags&(1<<3) != 0
}

func (e *InvokeReqExtra) SetReturnFailedSubqueries() {
	e.flags |= 1 << 4
}

func (e *InvokeReqExtra) IsSetReturnFailedSubqueries() bool {
	return e.flags&(1<<4) != 0
}

func (e *InvokeReqExtra) SetReturnQueryStats() {
	e.flags |= 1 << 6
}

func (e *InvokeReqExtra) IsSetReturnQueryStats() bool {
	return e.flags&(1<<6) != 0
}

func (e *InvokeReqExtra) SetNoResult() {
	e.flags |= 1 << 7
}

func (e *InvokeReqExtra) IsSetNoResult() bool {
	return e.flags&(1<<7) != 0
}

func (e *InvokeReqExtra) SetWaitBinlogPos(v int64) {
	e.WaitBinlogPos = v
	e.flags |= 1 << 16
}

func (e *InvokeReqExtra) IsSetWaitBinlogPos() bool {
	return e.flags&(1<<16) != 0
}

func (e *InvokeReqExtra) SetStringForwardKeys(v []string) {
	e.StringForwardKeys = v
	e.flags |= 1 << 18
}

func (e *InvokeReqExtra) IsSetStringForwardKeys() bool {
	return e.flags&(1<<18) != 0
}

func (e *InvokeReqExtra) SetIntForwardKeys(v []int64) {
	e.IntForwardKeys = v
	e.flags |= 1 << 19
}

func (e *InvokeReqExtra) IsSetIntForwardKeys() bool {
	return e.flags&(1<<19) != 0
}

func (e *InvokeReqExtra) SetStringForward(v string) {
	e.StringForward = v
	e.flags |= 1 << 20
}

func (e *InvokeReqExtra) IsSetStringForward() bool {
	return e.flags&(1<<20) != 0
}

func (e *InvokeReqExtra) SetIntForward(v int64) {
	e.IntForward = v
	e.flags |= 1 << 21
}

func (e *InvokeReqExtra) IsSetIntForward() bool {
	return e.flags&(1<<21) != 0
}

func (e *InvokeReqExtra) SetCustomTimeoutMs(v int32) {
	e.CustomTimeoutMs = v
	e.flags |= 1 << 23
}

func (e *InvokeReqExtra) IsSetCustomTimeoutMs() bool {
	return e.flags&(1<<23) != 0
}

func (e *InvokeReqExtra) SetSupportedCompressionVersion(v int32) {
	e.SupportedCompressionVersion = v
	e.flags |= 1 << 25
}

func (e *InvokeReqExtra) IsSetSupportedCompressionVersion() bool {
	return e.flags&(1<<25) != 0
}

func (e *InvokeReqExtra) SetRandomDelay(v float64) {
	e.RandomDelay = v
	e.flags |= 1 << 26
}

func (e *InvokeReqExtra) IsSetRandomDelay() bool {
	return e.flags&(1<<26) != 0
}

func (e *InvokeReqExtra) readFromBytesBuffer(r *bytes.Buffer) error {
	if e.flags&(1<<16) != 0 {
		if err := tlrw.ReadInt64(r, &e.WaitBinlogPos); err != nil {
			return err
		}
	}
	if e.flags&(1<<18) != 0 {
		var _l int
		if err := tlrw.ReadLength32(r, &_l, 4); err != nil {
			return err
		}
		var _data []string
		if cap(e.StringForwardKeys) < _l {
			_data = make([]string, _l)
		} else {
			_data = e.StringForwardKeys[:_l]
		}
		for _i := range _data {
			if err := tlrw.ReadString(r, &_data[_i]); err != nil {
				return err
			}
		}
		e.StringForwardKeys = _data
	}
	if e.flags&(1<<19) != 0 {
		var _l int
		if err := tlrw.ReadLength32(r, &_l, 4); err != nil {
			return err
		}
		var _data []int64
		if cap(e.IntForwardKeys) < _l {
			_data = make([]int64, _l)
		} else {
			_data = e.IntForwardKeys[:_l]
		}
		for _i := range _data {
			if err := tlrw.ReadInt64(r, &_data[_i]); err != nil {
				return err
			}
		}
		e.IntForwardKeys = _data
	}
	if e.flags&(1<<20) != 0 {
		if err := tlrw.ReadString(r, &e.StringForward); err != nil {
			return err
		}
	}
	if e.flags&(1<<21) != 0 {
		if err := tlrw.ReadInt64(r, &e.IntForward); err != nil {
			return err
		}
	}
	if e.flags&(1<<23) != 0 {
		if err := tlrw.ReadInt32(r, &e.CustomTimeoutMs); err != nil {
			return err
		}
	}
	if e.flags&(1<<25) != 0 {
		if err := tlrw.ReadInt32(r, &e.SupportedCompressionVersion); err != nil {
			return err
		}
	}
	if e.flags&(1<<26) != 0 {
		if err := tlrw.ReadFloat64(r, &e.RandomDelay); err != nil {
			return err
		}
	}

	return nil
}

func (e *InvokeReqExtra) writeToBytesBuffer(w *bytes.Buffer) error {
	if e.flags&(1<<16) != 0 {
		tlrw.WriteInt64(w, e.WaitBinlogPos)
	}
	if e.flags&(1<<18) != 0 {
		tlrw.WriteUint32(w, uint32(len(e.StringForwardKeys)))
		for _, _elem := range e.StringForwardKeys {
			if err := tlrw.WriteString(w, _elem); err != nil {
				return err
			}
		}
	}
	if e.flags&(1<<19) != 0 {
		tlrw.WriteUint32(w, uint32(len(e.IntForwardKeys)))
		for _, _elem := range e.IntForwardKeys {
			tlrw.WriteInt64(w, _elem)
		}
	}
	if e.flags&(1<<20) != 0 {
		if err := tlrw.WriteString(w, e.StringForward); err != nil {
			return err
		}
	}
	if e.flags&(1<<21) != 0 {
		tlrw.WriteInt64(w, e.IntForward)
	}
	if e.flags&(1<<23) != 0 {
		tlrw.WriteInt32(w, e.CustomTimeoutMs)
	}
	if e.flags&(1<<25) != 0 {
		tlrw.WriteInt32(w, e.SupportedCompressionVersion)
	}
	if e.flags&(1<<26) != 0 {
		tlrw.WriteFloat64(w, e.RandomDelay)
	}

	return nil
}

func (e *InvokeReqExtra) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &e.flags); err != nil {
		return w, err
	}
	if e.flags&(1<<16) != 0 {
		if w, err = basictl.LongRead(w, &e.WaitBinlogPos); err != nil {
			return w, err
		}
	} else {
		e.WaitBinlogPos = 0
	}
	if e.flags&(1<<18) != 0 {
		if w, err = vectorStringRead(w, &e.StringForwardKeys); err != nil {
			return w, err
		}
	} else {
		e.StringForwardKeys = e.StringForwardKeys[:0]
	}
	if e.flags&(1<<19) != 0 {
		if w, err = vectorLongRead(w, &e.IntForwardKeys); err != nil {
			return w, err
		}
	} else {
		e.IntForwardKeys = e.IntForwardKeys[:0]
	}
	if e.flags&(1<<20) != 0 {
		if w, err = basictl.StringRead(w, &e.StringForward); err != nil {
			return w, err
		}
	} else {
		e.StringForward = ""
	}
	if e.flags&(1<<21) != 0 {
		if w, err = basictl.LongRead(w, &e.IntForward); err != nil {
			return w, err
		}
	} else {
		e.IntForward = 0
	}
	if e.flags&(1<<23) != 0 {
		if w, err = basictl.IntRead(w, &e.CustomTimeoutMs); err != nil {
			return w, err
		}
	} else {
		e.CustomTimeoutMs = 0
	}
	if e.flags&(1<<25) != 0 {
		if w, err = basictl.IntRead(w, &e.SupportedCompressionVersion); err != nil {
			return w, err
		}
	} else {
		e.SupportedCompressionVersion = 0
	}
	if e.flags&(1<<26) != 0 {
		if w, err = basictl.DoubleRead(w, &e.RandomDelay); err != nil {
			return w, err
		}
	} else {
		e.RandomDelay = 0
	}
	return w, nil
}

func (e *InvokeReqExtra) Write(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, e.flags)
	if e.flags&(1<<16) != 0 {
		w = basictl.LongWrite(w, e.WaitBinlogPos)
	}
	if e.flags&(1<<18) != 0 {
		if w, err = vectorStringWrite(w, e.StringForwardKeys); err != nil {
			return w, err
		}
	}
	if e.flags&(1<<19) != 0 {
		if w, err = vectorLongWrite(w, e.IntForwardKeys); err != nil {
			return w, err
		}
	}
	if e.flags&(1<<20) != 0 {
		if w, err = basictl.StringWrite(w, e.StringForward); err != nil {
			return w, err
		}
	}
	if e.flags&(1<<21) != 0 {
		w = basictl.LongWrite(w, e.IntForward)
	}
	if e.flags&(1<<23) != 0 {
		w = basictl.IntWrite(w, e.CustomTimeoutMs)
	}
	if e.flags&(1<<25) != 0 {
		w = basictl.IntWrite(w, e.SupportedCompressionVersion)
	}
	if e.flags&(1<<26) != 0 {
		w = basictl.DoubleWrite(w, e.RandomDelay)
	}
	return w, nil
}

func vectorStringRead(w []byte, vec *[]string) (_ []byte, err error) {
	var l uint32
	if w, err = basictl.NatRead(w, &l); err != nil {
		return w, err
	}
	if err = basictl.CheckLengthSanity(w, l, 4); err != nil {
		return w, err
	}
	if uint32(cap(*vec)) < l {
		*vec = make([]string, l)
	} else {
		*vec = (*vec)[:l]
	}
	for i := range *vec {
		if w, err = basictl.StringRead(w, &(*vec)[i]); err != nil {
			return w, err
		}
	}
	return w, nil
}

func vectorStringWrite(w []byte, vec []string) (_ []byte, err error) {
	w = basictl.NatWrite(w, uint32(len(vec)))
	for _, elem := range vec {
		if w, err = basictl.StringWrite(w, elem); err != nil {
			return w, err
		}
	}
	return w, nil
}

func vectorLongRead(w []byte, vec *[]int64) (_ []byte, err error) {
	var l uint32
	if w, err = basictl.NatRead(w, &l); err != nil {
		return w, err
	}
	if err = basictl.CheckLengthSanity(w, l, 4); err != nil {
		return w, err
	}
	if uint32(cap(*vec)) < l {
		*vec = make([]int64, l)
	} else {
		*vec = (*vec)[:l]
	}
	for i := range *vec {
		if w, err = basictl.LongRead(w, &(*vec)[i]); err != nil {
			return w, err
		}
	}
	return w, nil
}

func vectorLongWrite(w []byte, vec []int64) (_ []byte, err error) {
	w = basictl.NatWrite(w, uint32(len(vec)))
	for _, elem := range vec {
		w = basictl.LongWrite(w, elem)
	}
	return w, nil
}

// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Code generated by vktl/cmd/tlgen2; DO NOT EDIT.
package internal

import (
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
)

var _ = basictl.NatWrite

type KvEnginePut struct {
	Key   int64
	Value int64
}

func (KvEnginePut) TLName() string { return "kv_engine.put" }
func (KvEnginePut) TLTag() uint32  { return 0x2c7349ba }

func (item *KvEnginePut) Reset() {
	item.Key = 0
	item.Value = 0
}

func (item *KvEnginePut) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.LongRead(w, &item.Key); err != nil {
		return w, err
	}
	return basictl.LongRead(w, &item.Value)
}

func (item *KvEnginePut) Write(w []byte) (_ []byte, err error) {
	w = basictl.LongWrite(w, item.Key)
	return basictl.LongWrite(w, item.Value), nil
}

func (item *KvEnginePut) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x2c7349ba); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *KvEnginePut) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x2c7349ba)
	return item.Write(w)
}

func (item *KvEnginePut) ReadResult(w []byte, ret *KvEngineChangeResponse) (_ []byte, err error) {
	return ret.ReadBoxed(w)
}

func (item *KvEnginePut) WriteResult(w []byte, ret KvEngineChangeResponse) (_ []byte, err error) {
	return ret.WriteBoxed(w)
}

func (item *KvEnginePut) ReadResultJSON(j interface{}, ret *KvEngineChangeResponse) error {
	if err := KvEngineChangeResponse__ReadJSON(ret, j); err != nil {
		return err
	}
	return nil
}

func (item *KvEnginePut) WriteResultJSON(w []byte, ret KvEngineChangeResponse) (_ []byte, err error) {
	if w, err = ret.WriteJSON(w); err != nil {
		return w, err
	}
	return w, nil
}

func (item *KvEnginePut) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret KvEngineChangeResponse
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *KvEnginePut) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	j, err := JsonBytesToInterface(r)
	if err != nil {
		return r, w, ErrorInvalidJSON("kv_engine.put", err.Error())
	}
	var ret KvEngineChangeResponse
	if err = item.ReadResultJSON(j, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item KvEnginePut) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func KvEnginePut__ReadJSON(item *KvEnginePut, j interface{}) error { return item.readJSON(j) }
func (item *KvEnginePut) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("kv_engine.put", "expected json object")
	}
	_jKey := _jm["key"]
	delete(_jm, "key")
	if err := JsonReadInt64(_jKey, &item.Key); err != nil {
		return err
	}
	_jValue := _jm["value"]
	delete(_jm, "value")
	if err := JsonReadInt64(_jValue, &item.Value); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("kv_engine.put", k)
	}
	return nil
}

func (item *KvEnginePut) WriteJSON(w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.Key != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"key":`...)
		w = basictl.JSONWriteInt64(w, item.Key)
	}
	if item.Value != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"value":`...)
		w = basictl.JSONWriteInt64(w, item.Value)
	}
	return append(w, '}'), nil
}

func (item *KvEnginePut) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *KvEnginePut) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("kv_engine.put", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("kv_engine.put", err.Error())
	}
	return nil
}
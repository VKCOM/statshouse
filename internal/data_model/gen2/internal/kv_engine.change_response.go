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

type KvEngineChangeResponse struct {
	Meta     KvEngineMetaInfo
	NewValue int64
}

func (KvEngineChangeResponse) TLName() string { return "kv_engine.change_response" }
func (KvEngineChangeResponse) TLTag() uint32  { return 0x73eaa764 }

func (item *KvEngineChangeResponse) Reset() {
	item.Meta.Reset()
	item.NewValue = 0
}

func (item *KvEngineChangeResponse) Read(w []byte) (_ []byte, err error) {
	if w, err = item.Meta.Read(w); err != nil {
		return w, err
	}
	return basictl.LongRead(w, &item.NewValue)
}

func (item *KvEngineChangeResponse) Write(w []byte) (_ []byte, err error) {
	if w, err = item.Meta.Write(w); err != nil {
		return w, err
	}
	return basictl.LongWrite(w, item.NewValue), nil
}

func (item *KvEngineChangeResponse) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x73eaa764); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *KvEngineChangeResponse) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x73eaa764)
	return item.Write(w)
}

func (item KvEngineChangeResponse) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func KvEngineChangeResponse__ReadJSON(item *KvEngineChangeResponse, j interface{}) error {
	return item.readJSON(j)
}
func (item *KvEngineChangeResponse) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("kv_engine.change_response", "expected json object")
	}
	_jMeta := _jm["meta"]
	delete(_jm, "meta")
	_jNewValue := _jm["new_value"]
	delete(_jm, "new_value")
	if err := JsonReadInt64(_jNewValue, &item.NewValue); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("kv_engine.change_response", k)
	}
	if err := KvEngineMetaInfo__ReadJSON(&item.Meta, _jMeta); err != nil {
		return err
	}
	return nil
}

func (item *KvEngineChangeResponse) WriteJSON(w []byte) (_ []byte, err error) {
	w = append(w, '{')
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"meta":`...)
	if w, err = item.Meta.WriteJSON(w); err != nil {
		return w, err
	}
	if item.NewValue != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"new_value":`...)
		w = basictl.JSONWriteInt64(w, item.NewValue)
	}
	return append(w, '}'), nil
}

func (item *KvEngineChangeResponse) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *KvEngineChangeResponse) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("kv_engine.change_response", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("kv_engine.change_response", err.Error())
	}
	return nil
}
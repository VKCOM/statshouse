// Copyright 2022 V Kontakte LLC
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

var _ = True{}

type StatshouseTestConnection struct {
	FieldsMask        uint32
	ShardReplica      int32 // Conditional: item.FieldsMask.5
	ShardReplicaTotal int32 // Conditional: item.FieldsMask.5
	// IngressProxy True // Conditional: item.FieldsMask.6
	Payload            string
	ResponseSize       int32
	ResponseTimeoutSec int32
}

func (StatshouseTestConnection) TLName() string { return "statshouse.testConnection" }
func (StatshouseTestConnection) TLTag() uint32  { return 0x3285ff58 }

func (item *StatshouseTestConnection) SetShardReplica(v int32) {
	item.ShardReplica = v
	item.FieldsMask |= 1 << 5
}
func (item *StatshouseTestConnection) ClearShardReplica() {
	item.ShardReplica = 0
	item.FieldsMask &^= 1 << 5
}
func (item *StatshouseTestConnection) IsSetShardReplica() bool { return item.FieldsMask&(1<<5) != 0 }

func (item *StatshouseTestConnection) SetShardReplicaTotal(v int32) {
	item.ShardReplicaTotal = v
	item.FieldsMask |= 1 << 5
}
func (item *StatshouseTestConnection) ClearShardReplicaTotal() {
	item.ShardReplicaTotal = 0
	item.FieldsMask &^= 1 << 5
}
func (item *StatshouseTestConnection) IsSetShardReplicaTotal() bool {
	return item.FieldsMask&(1<<5) != 0
}

func (item *StatshouseTestConnection) SetIngressProxy(v bool) {
	if v {
		item.FieldsMask |= 1 << 6
	} else {
		item.FieldsMask &^= 1 << 6
	}
}
func (item *StatshouseTestConnection) IsSetIngressProxy() bool { return item.FieldsMask&(1<<6) != 0 }

func (item *StatshouseTestConnection) Reset() {
	item.FieldsMask = 0
	item.ShardReplica = 0
	item.ShardReplicaTotal = 0
	item.Payload = ""
	item.ResponseSize = 0
	item.ResponseTimeoutSec = 0
}

func (item *StatshouseTestConnection) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	if item.FieldsMask&(1<<5) != 0 {
		if w, err = basictl.IntRead(w, &item.ShardReplica); err != nil {
			return w, err
		}
	} else {
		item.ShardReplica = 0
	}
	if item.FieldsMask&(1<<5) != 0 {
		if w, err = basictl.IntRead(w, &item.ShardReplicaTotal); err != nil {
			return w, err
		}
	} else {
		item.ShardReplicaTotal = 0
	}
	if w, err = basictl.StringRead(w, &item.Payload); err != nil {
		return w, err
	}
	if w, err = basictl.IntRead(w, &item.ResponseSize); err != nil {
		return w, err
	}
	return basictl.IntRead(w, &item.ResponseTimeoutSec)
}

func (item *StatshouseTestConnection) Write(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, item.FieldsMask)
	if item.FieldsMask&(1<<5) != 0 {
		w = basictl.IntWrite(w, item.ShardReplica)
	}
	if item.FieldsMask&(1<<5) != 0 {
		w = basictl.IntWrite(w, item.ShardReplicaTotal)
	}
	if w, err = basictl.StringWrite(w, item.Payload); err != nil {
		return w, err
	}
	w = basictl.IntWrite(w, item.ResponseSize)
	return basictl.IntWrite(w, item.ResponseTimeoutSec), nil
}

func (item *StatshouseTestConnection) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x3285ff58); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *StatshouseTestConnection) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x3285ff58)
	return item.Write(w)
}

func (item *StatshouseTestConnection) ReadResult(w []byte, ret *string) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xb5286e24); err != nil {
		return w, err
	}
	return basictl.StringRead(w, ret)
}

func (item *StatshouseTestConnection) WriteResult(w []byte, ret string) (_ []byte, err error) {
	w = basictl.NatWrite(w, 0xb5286e24)
	return basictl.StringWrite(w, ret)
}

func (item *StatshouseTestConnection) ReadResultJSON(j interface{}, ret *string) error {
	if err := JsonReadString(j, ret); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseTestConnection) WriteResultJSON(w []byte, ret string) (_ []byte, err error) {
	w = basictl.JSONWriteString(w, ret)
	return w, nil
}

func (item *StatshouseTestConnection) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret string
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *StatshouseTestConnection) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	j, err := JsonBytesToInterface(r)
	if err != nil {
		return r, w, ErrorInvalidJSON("statshouse.testConnection", err.Error())
	}
	var ret string
	if err = item.ReadResultJSON(j, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item StatshouseTestConnection) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func StatshouseTestConnection__ReadJSON(item *StatshouseTestConnection, j interface{}) error {
	return item.readJSON(j)
}
func (item *StatshouseTestConnection) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("statshouse.testConnection", "expected json object")
	}
	_jFieldsMask := _jm["fields_mask"]
	delete(_jm, "fields_mask")
	if err := JsonReadUint32(_jFieldsMask, &item.FieldsMask); err != nil {
		return err
	}
	_jShardReplica := _jm["shard_replica"]
	delete(_jm, "shard_replica")
	_jShardReplicaTotal := _jm["shard_replica_total"]
	delete(_jm, "shard_replica_total")
	_jIngressProxy := _jm["ingress_proxy"]
	delete(_jm, "ingress_proxy")
	_jPayload := _jm["payload"]
	delete(_jm, "payload")
	if err := JsonReadString(_jPayload, &item.Payload); err != nil {
		return err
	}
	_jResponseSize := _jm["response_size"]
	delete(_jm, "response_size")
	if err := JsonReadInt32(_jResponseSize, &item.ResponseSize); err != nil {
		return err
	}
	_jResponseTimeoutSec := _jm["response_timeout_sec"]
	delete(_jm, "response_timeout_sec")
	if err := JsonReadInt32(_jResponseTimeoutSec, &item.ResponseTimeoutSec); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("statshouse.testConnection", k)
	}
	if _jShardReplica != nil {
		item.FieldsMask |= 1 << 5
	}
	if _jShardReplicaTotal != nil {
		item.FieldsMask |= 1 << 5
	}
	if _jIngressProxy != nil {
		_bit := false
		if err := JsonReadBool(_jIngressProxy, &_bit); err != nil {
			return err
		}
		if _bit {
			item.FieldsMask |= 1 << 6
		} else {
			item.FieldsMask &^= 1 << 6
		}
	}
	if _jShardReplica != nil {
		if err := JsonReadInt32(_jShardReplica, &item.ShardReplica); err != nil {
			return err
		}
	} else {
		item.ShardReplica = 0
	}
	if _jShardReplicaTotal != nil {
		if err := JsonReadInt32(_jShardReplicaTotal, &item.ShardReplicaTotal); err != nil {
			return err
		}
	} else {
		item.ShardReplicaTotal = 0
	}
	return nil
}

func (item *StatshouseTestConnection) WriteJSON(w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.FieldsMask != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"fields_mask":`...)
		w = basictl.JSONWriteUint32(w, item.FieldsMask)
	}
	if item.FieldsMask&(1<<5) != 0 {
		if item.ShardReplica != 0 {
			w = basictl.JSONAddCommaIfNeeded(w)
			w = append(w, `"shard_replica":`...)
			w = basictl.JSONWriteInt32(w, item.ShardReplica)
		}
	}
	if item.FieldsMask&(1<<5) != 0 {
		if item.ShardReplicaTotal != 0 {
			w = basictl.JSONAddCommaIfNeeded(w)
			w = append(w, `"shard_replica_total":`...)
			w = basictl.JSONWriteInt32(w, item.ShardReplicaTotal)
		}
	}
	if item.FieldsMask&(1<<6) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"ingress_proxy":true`...)
	}
	if len(item.Payload) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"payload":`...)
		w = basictl.JSONWriteString(w, item.Payload)
	}
	if item.ResponseSize != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"response_size":`...)
		w = basictl.JSONWriteInt32(w, item.ResponseSize)
	}
	if item.ResponseTimeoutSec != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"response_timeout_sec":`...)
		w = basictl.JSONWriteInt32(w, item.ResponseTimeoutSec)
	}
	return append(w, '}'), nil
}

func (item *StatshouseTestConnection) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *StatshouseTestConnection) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("statshouse.testConnection", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("statshouse.testConnection", err.Error())
	}
	return nil
}

var _ = True{}

type StatshouseTestConnectionBytes struct {
	FieldsMask        uint32
	ShardReplica      int32 // Conditional: item.FieldsMask.5
	ShardReplicaTotal int32 // Conditional: item.FieldsMask.5
	// IngressProxy True // Conditional: item.FieldsMask.6
	Payload            []byte
	ResponseSize       int32
	ResponseTimeoutSec int32
}

func (StatshouseTestConnectionBytes) TLName() string { return "statshouse.testConnection" }
func (StatshouseTestConnectionBytes) TLTag() uint32  { return 0x3285ff58 }

func (item *StatshouseTestConnectionBytes) SetShardReplica(v int32) {
	item.ShardReplica = v
	item.FieldsMask |= 1 << 5
}
func (item *StatshouseTestConnectionBytes) ClearShardReplica() {
	item.ShardReplica = 0
	item.FieldsMask &^= 1 << 5
}
func (item *StatshouseTestConnectionBytes) IsSetShardReplica() bool {
	return item.FieldsMask&(1<<5) != 0
}

func (item *StatshouseTestConnectionBytes) SetShardReplicaTotal(v int32) {
	item.ShardReplicaTotal = v
	item.FieldsMask |= 1 << 5
}
func (item *StatshouseTestConnectionBytes) ClearShardReplicaTotal() {
	item.ShardReplicaTotal = 0
	item.FieldsMask &^= 1 << 5
}
func (item *StatshouseTestConnectionBytes) IsSetShardReplicaTotal() bool {
	return item.FieldsMask&(1<<5) != 0
}

func (item *StatshouseTestConnectionBytes) SetIngressProxy(v bool) {
	if v {
		item.FieldsMask |= 1 << 6
	} else {
		item.FieldsMask &^= 1 << 6
	}
}
func (item *StatshouseTestConnectionBytes) IsSetIngressProxy() bool {
	return item.FieldsMask&(1<<6) != 0
}

func (item *StatshouseTestConnectionBytes) Reset() {
	item.FieldsMask = 0
	item.ShardReplica = 0
	item.ShardReplicaTotal = 0
	item.Payload = item.Payload[:0]
	item.ResponseSize = 0
	item.ResponseTimeoutSec = 0
}

func (item *StatshouseTestConnectionBytes) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	if item.FieldsMask&(1<<5) != 0 {
		if w, err = basictl.IntRead(w, &item.ShardReplica); err != nil {
			return w, err
		}
	} else {
		item.ShardReplica = 0
	}
	if item.FieldsMask&(1<<5) != 0 {
		if w, err = basictl.IntRead(w, &item.ShardReplicaTotal); err != nil {
			return w, err
		}
	} else {
		item.ShardReplicaTotal = 0
	}
	if w, err = basictl.StringReadBytes(w, &item.Payload); err != nil {
		return w, err
	}
	if w, err = basictl.IntRead(w, &item.ResponseSize); err != nil {
		return w, err
	}
	return basictl.IntRead(w, &item.ResponseTimeoutSec)
}

func (item *StatshouseTestConnectionBytes) Write(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, item.FieldsMask)
	if item.FieldsMask&(1<<5) != 0 {
		w = basictl.IntWrite(w, item.ShardReplica)
	}
	if item.FieldsMask&(1<<5) != 0 {
		w = basictl.IntWrite(w, item.ShardReplicaTotal)
	}
	if w, err = basictl.StringWriteBytes(w, item.Payload); err != nil {
		return w, err
	}
	w = basictl.IntWrite(w, item.ResponseSize)
	return basictl.IntWrite(w, item.ResponseTimeoutSec), nil
}

func (item *StatshouseTestConnectionBytes) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x3285ff58); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *StatshouseTestConnectionBytes) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x3285ff58)
	return item.Write(w)
}

func (item *StatshouseTestConnectionBytes) ReadResult(w []byte, ret *[]byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xb5286e24); err != nil {
		return w, err
	}
	return basictl.StringReadBytes(w, ret)
}

func (item *StatshouseTestConnectionBytes) WriteResult(w []byte, ret []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, 0xb5286e24)
	return basictl.StringWriteBytes(w, ret)
}

func (item *StatshouseTestConnectionBytes) ReadResultJSON(j interface{}, ret *[]byte) error {
	if err := JsonReadStringBytes(j, ret); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseTestConnectionBytes) WriteResultJSON(w []byte, ret []byte) (_ []byte, err error) {
	w = basictl.JSONWriteStringBytes(w, ret)
	return w, nil
}

func (item *StatshouseTestConnectionBytes) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret []byte
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *StatshouseTestConnectionBytes) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	j, err := JsonBytesToInterface(r)
	if err != nil {
		return r, w, ErrorInvalidJSON("statshouse.testConnection", err.Error())
	}
	var ret []byte
	if err = item.ReadResultJSON(j, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item StatshouseTestConnectionBytes) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func StatshouseTestConnectionBytes__ReadJSON(item *StatshouseTestConnectionBytes, j interface{}) error {
	return item.readJSON(j)
}
func (item *StatshouseTestConnectionBytes) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("statshouse.testConnection", "expected json object")
	}
	_jFieldsMask := _jm["fields_mask"]
	delete(_jm, "fields_mask")
	if err := JsonReadUint32(_jFieldsMask, &item.FieldsMask); err != nil {
		return err
	}
	_jShardReplica := _jm["shard_replica"]
	delete(_jm, "shard_replica")
	_jShardReplicaTotal := _jm["shard_replica_total"]
	delete(_jm, "shard_replica_total")
	_jIngressProxy := _jm["ingress_proxy"]
	delete(_jm, "ingress_proxy")
	_jPayload := _jm["payload"]
	delete(_jm, "payload")
	if err := JsonReadStringBytes(_jPayload, &item.Payload); err != nil {
		return err
	}
	_jResponseSize := _jm["response_size"]
	delete(_jm, "response_size")
	if err := JsonReadInt32(_jResponseSize, &item.ResponseSize); err != nil {
		return err
	}
	_jResponseTimeoutSec := _jm["response_timeout_sec"]
	delete(_jm, "response_timeout_sec")
	if err := JsonReadInt32(_jResponseTimeoutSec, &item.ResponseTimeoutSec); err != nil {
		return err
	}
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("statshouse.testConnection", k)
	}
	if _jShardReplica != nil {
		item.FieldsMask |= 1 << 5
	}
	if _jShardReplicaTotal != nil {
		item.FieldsMask |= 1 << 5
	}
	if _jIngressProxy != nil {
		_bit := false
		if err := JsonReadBool(_jIngressProxy, &_bit); err != nil {
			return err
		}
		if _bit {
			item.FieldsMask |= 1 << 6
		} else {
			item.FieldsMask &^= 1 << 6
		}
	}
	if _jShardReplica != nil {
		if err := JsonReadInt32(_jShardReplica, &item.ShardReplica); err != nil {
			return err
		}
	} else {
		item.ShardReplica = 0
	}
	if _jShardReplicaTotal != nil {
		if err := JsonReadInt32(_jShardReplicaTotal, &item.ShardReplicaTotal); err != nil {
			return err
		}
	} else {
		item.ShardReplicaTotal = 0
	}
	return nil
}

func (item *StatshouseTestConnectionBytes) WriteJSON(w []byte) (_ []byte, err error) {
	w = append(w, '{')
	if item.FieldsMask != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"fields_mask":`...)
		w = basictl.JSONWriteUint32(w, item.FieldsMask)
	}
	if item.FieldsMask&(1<<5) != 0 {
		if item.ShardReplica != 0 {
			w = basictl.JSONAddCommaIfNeeded(w)
			w = append(w, `"shard_replica":`...)
			w = basictl.JSONWriteInt32(w, item.ShardReplica)
		}
	}
	if item.FieldsMask&(1<<5) != 0 {
		if item.ShardReplicaTotal != 0 {
			w = basictl.JSONAddCommaIfNeeded(w)
			w = append(w, `"shard_replica_total":`...)
			w = basictl.JSONWriteInt32(w, item.ShardReplicaTotal)
		}
	}
	if item.FieldsMask&(1<<6) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"ingress_proxy":true`...)
	}
	if len(item.Payload) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"payload":`...)
		w = basictl.JSONWriteStringBytes(w, item.Payload)
	}
	if item.ResponseSize != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"response_size":`...)
		w = basictl.JSONWriteInt32(w, item.ResponseSize)
	}
	if item.ResponseTimeoutSec != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"response_timeout_sec":`...)
		w = basictl.JSONWriteInt32(w, item.ResponseTimeoutSec)
	}
	return append(w, '}'), nil
}

func (item *StatshouseTestConnectionBytes) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *StatshouseTestConnectionBytes) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("statshouse.testConnection", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("statshouse.testConnection", err.Error())
	}
	return nil
}

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

type StatshouseSendKeepAlive struct {
	FieldsMask        uint32
	ShardReplica      int32 // Conditional: item.FieldsMask.5
	ShardReplicaTotal int32 // Conditional: item.FieldsMask.5
	// IngressProxy True // Conditional: item.FieldsMask.6
	HostName                string
	SourceEnv               int32 // Conditional: item.FieldsMask.0
	BuildArch               int32 // Conditional: item.FieldsMask.2
	ShardReplicaLegacy      int32 // Conditional: item.FieldsMask.1
	ShardReplicaLegacyTotal int32 // Conditional: item.FieldsMask.1
}

func (StatshouseSendKeepAlive) TLName() string { return "statshouse.sendKeepAlive" }
func (StatshouseSendKeepAlive) TLTag() uint32  { return 0x3285ff53 }

func (item *StatshouseSendKeepAlive) SetShardReplica(v int32) {
	item.ShardReplica = v
	item.FieldsMask |= 1 << 5
}
func (item *StatshouseSendKeepAlive) ClearShardReplica() {
	item.ShardReplica = 0
	item.FieldsMask &^= 1 << 5
}
func (item *StatshouseSendKeepAlive) IsSetShardReplica() bool { return item.FieldsMask&(1<<5) != 0 }

func (item *StatshouseSendKeepAlive) SetShardReplicaTotal(v int32) {
	item.ShardReplicaTotal = v
	item.FieldsMask |= 1 << 5
}
func (item *StatshouseSendKeepAlive) ClearShardReplicaTotal() {
	item.ShardReplicaTotal = 0
	item.FieldsMask &^= 1 << 5
}
func (item *StatshouseSendKeepAlive) IsSetShardReplicaTotal() bool {
	return item.FieldsMask&(1<<5) != 0
}

func (item *StatshouseSendKeepAlive) SetIngressProxy(v bool) {
	if v {
		item.FieldsMask |= 1 << 6
	} else {
		item.FieldsMask &^= 1 << 6
	}
}
func (item *StatshouseSendKeepAlive) IsSetIngressProxy() bool { return item.FieldsMask&(1<<6) != 0 }

func (item *StatshouseSendKeepAlive) SetSourceEnv(v int32) {
	item.SourceEnv = v
	item.FieldsMask |= 1 << 0
}
func (item *StatshouseSendKeepAlive) ClearSourceEnv() {
	item.SourceEnv = 0
	item.FieldsMask &^= 1 << 0
}
func (item *StatshouseSendKeepAlive) IsSetSourceEnv() bool { return item.FieldsMask&(1<<0) != 0 }

func (item *StatshouseSendKeepAlive) SetBuildArch(v int32) {
	item.BuildArch = v
	item.FieldsMask |= 1 << 2
}
func (item *StatshouseSendKeepAlive) ClearBuildArch() {
	item.BuildArch = 0
	item.FieldsMask &^= 1 << 2
}
func (item *StatshouseSendKeepAlive) IsSetBuildArch() bool { return item.FieldsMask&(1<<2) != 0 }

func (item *StatshouseSendKeepAlive) SetShardReplicaLegacy(v int32) {
	item.ShardReplicaLegacy = v
	item.FieldsMask |= 1 << 1
}
func (item *StatshouseSendKeepAlive) ClearShardReplicaLegacy() {
	item.ShardReplicaLegacy = 0
	item.FieldsMask &^= 1 << 1
}
func (item *StatshouseSendKeepAlive) IsSetShardReplicaLegacy() bool {
	return item.FieldsMask&(1<<1) != 0
}

func (item *StatshouseSendKeepAlive) SetShardReplicaLegacyTotal(v int32) {
	item.ShardReplicaLegacyTotal = v
	item.FieldsMask |= 1 << 1
}
func (item *StatshouseSendKeepAlive) ClearShardReplicaLegacyTotal() {
	item.ShardReplicaLegacyTotal = 0
	item.FieldsMask &^= 1 << 1
}
func (item *StatshouseSendKeepAlive) IsSetShardReplicaLegacyTotal() bool {
	return item.FieldsMask&(1<<1) != 0
}

func (item *StatshouseSendKeepAlive) Reset() {
	item.FieldsMask = 0
	item.ShardReplica = 0
	item.ShardReplicaTotal = 0
	item.HostName = ""
	item.SourceEnv = 0
	item.BuildArch = 0
	item.ShardReplicaLegacy = 0
	item.ShardReplicaLegacyTotal = 0
}

func (item *StatshouseSendKeepAlive) Read(w []byte) (_ []byte, err error) {
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
	if w, err = basictl.StringRead(w, &item.HostName); err != nil {
		return w, err
	}
	if item.FieldsMask&(1<<0) != 0 {
		if w, err = basictl.IntRead(w, &item.SourceEnv); err != nil {
			return w, err
		}
	} else {
		item.SourceEnv = 0
	}
	if item.FieldsMask&(1<<2) != 0 {
		if w, err = basictl.IntRead(w, &item.BuildArch); err != nil {
			return w, err
		}
	} else {
		item.BuildArch = 0
	}
	if item.FieldsMask&(1<<1) != 0 {
		if w, err = basictl.IntRead(w, &item.ShardReplicaLegacy); err != nil {
			return w, err
		}
	} else {
		item.ShardReplicaLegacy = 0
	}
	if item.FieldsMask&(1<<1) != 0 {
		if w, err = basictl.IntRead(w, &item.ShardReplicaLegacyTotal); err != nil {
			return w, err
		}
	} else {
		item.ShardReplicaLegacyTotal = 0
	}
	return w, nil
}

func (item *StatshouseSendKeepAlive) Write(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, item.FieldsMask)
	if item.FieldsMask&(1<<5) != 0 {
		w = basictl.IntWrite(w, item.ShardReplica)
	}
	if item.FieldsMask&(1<<5) != 0 {
		w = basictl.IntWrite(w, item.ShardReplicaTotal)
	}
	if w, err = basictl.StringWrite(w, item.HostName); err != nil {
		return w, err
	}
	if item.FieldsMask&(1<<0) != 0 {
		w = basictl.IntWrite(w, item.SourceEnv)
	}
	if item.FieldsMask&(1<<2) != 0 {
		w = basictl.IntWrite(w, item.BuildArch)
	}
	if item.FieldsMask&(1<<1) != 0 {
		w = basictl.IntWrite(w, item.ShardReplicaLegacy)
	}
	if item.FieldsMask&(1<<1) != 0 {
		w = basictl.IntWrite(w, item.ShardReplicaLegacyTotal)
	}
	return w, nil
}

func (item *StatshouseSendKeepAlive) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x3285ff53); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *StatshouseSendKeepAlive) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x3285ff53)
	return item.Write(w)
}

func (item *StatshouseSendKeepAlive) ReadResult(w []byte, ret *string) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xb5286e24); err != nil {
		return w, err
	}
	return basictl.StringRead(w, ret)
}

func (item *StatshouseSendKeepAlive) WriteResult(w []byte, ret string) (_ []byte, err error) {
	w = basictl.NatWrite(w, 0xb5286e24)
	return basictl.StringWrite(w, ret)
}

func (item *StatshouseSendKeepAlive) ReadResultJSON(j interface{}, ret *string) error {
	if err := JsonReadString(j, ret); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseSendKeepAlive) WriteResultJSON(w []byte, ret string) (_ []byte, err error) {
	w = basictl.JSONWriteString(w, ret)
	return w, nil
}

func (item *StatshouseSendKeepAlive) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret string
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *StatshouseSendKeepAlive) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	j, err := JsonBytesToInterface(r)
	if err != nil {
		return r, w, ErrorInvalidJSON("statshouse.sendKeepAlive", err.Error())
	}
	var ret string
	if err = item.ReadResultJSON(j, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item StatshouseSendKeepAlive) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func StatshouseSendKeepAlive__ReadJSON(item *StatshouseSendKeepAlive, j interface{}) error {
	return item.readJSON(j)
}
func (item *StatshouseSendKeepAlive) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("statshouse.sendKeepAlive", "expected json object")
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
	_jHostName := _jm["host_name"]
	delete(_jm, "host_name")
	if err := JsonReadString(_jHostName, &item.HostName); err != nil {
		return err
	}
	_jSourceEnv := _jm["source_env"]
	delete(_jm, "source_env")
	_jBuildArch := _jm["build_arch"]
	delete(_jm, "build_arch")
	_jShardReplicaLegacy := _jm["shard_replica_legacy"]
	delete(_jm, "shard_replica_legacy")
	_jShardReplicaLegacyTotal := _jm["shard_replica_legacy_total"]
	delete(_jm, "shard_replica_legacy_total")
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("statshouse.sendKeepAlive", k)
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
	if _jSourceEnv != nil {
		item.FieldsMask |= 1 << 0
	}
	if _jBuildArch != nil {
		item.FieldsMask |= 1 << 2
	}
	if _jShardReplicaLegacy != nil {
		item.FieldsMask |= 1 << 1
	}
	if _jShardReplicaLegacyTotal != nil {
		item.FieldsMask |= 1 << 1
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
	if _jSourceEnv != nil {
		if err := JsonReadInt32(_jSourceEnv, &item.SourceEnv); err != nil {
			return err
		}
	} else {
		item.SourceEnv = 0
	}
	if _jBuildArch != nil {
		if err := JsonReadInt32(_jBuildArch, &item.BuildArch); err != nil {
			return err
		}
	} else {
		item.BuildArch = 0
	}
	if _jShardReplicaLegacy != nil {
		if err := JsonReadInt32(_jShardReplicaLegacy, &item.ShardReplicaLegacy); err != nil {
			return err
		}
	} else {
		item.ShardReplicaLegacy = 0
	}
	if _jShardReplicaLegacyTotal != nil {
		if err := JsonReadInt32(_jShardReplicaLegacyTotal, &item.ShardReplicaLegacyTotal); err != nil {
			return err
		}
	} else {
		item.ShardReplicaLegacyTotal = 0
	}
	return nil
}

func (item *StatshouseSendKeepAlive) WriteJSON(w []byte) (_ []byte, err error) {
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
	if len(item.HostName) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"host_name":`...)
		w = basictl.JSONWriteString(w, item.HostName)
	}
	if item.FieldsMask&(1<<0) != 0 {
		if item.SourceEnv != 0 {
			w = basictl.JSONAddCommaIfNeeded(w)
			w = append(w, `"source_env":`...)
			w = basictl.JSONWriteInt32(w, item.SourceEnv)
		}
	}
	if item.FieldsMask&(1<<2) != 0 {
		if item.BuildArch != 0 {
			w = basictl.JSONAddCommaIfNeeded(w)
			w = append(w, `"build_arch":`...)
			w = basictl.JSONWriteInt32(w, item.BuildArch)
		}
	}
	if item.FieldsMask&(1<<1) != 0 {
		if item.ShardReplicaLegacy != 0 {
			w = basictl.JSONAddCommaIfNeeded(w)
			w = append(w, `"shard_replica_legacy":`...)
			w = basictl.JSONWriteInt32(w, item.ShardReplicaLegacy)
		}
	}
	if item.FieldsMask&(1<<1) != 0 {
		if item.ShardReplicaLegacyTotal != 0 {
			w = basictl.JSONAddCommaIfNeeded(w)
			w = append(w, `"shard_replica_legacy_total":`...)
			w = basictl.JSONWriteInt32(w, item.ShardReplicaLegacyTotal)
		}
	}
	return append(w, '}'), nil
}

func (item *StatshouseSendKeepAlive) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *StatshouseSendKeepAlive) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("statshouse.sendKeepAlive", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("statshouse.sendKeepAlive", err.Error())
	}
	return nil
}

var _ = True{}

type StatshouseSendKeepAliveBytes struct {
	FieldsMask        uint32
	ShardReplica      int32 // Conditional: item.FieldsMask.5
	ShardReplicaTotal int32 // Conditional: item.FieldsMask.5
	// IngressProxy True // Conditional: item.FieldsMask.6
	HostName                []byte
	SourceEnv               int32 // Conditional: item.FieldsMask.0
	BuildArch               int32 // Conditional: item.FieldsMask.2
	ShardReplicaLegacy      int32 // Conditional: item.FieldsMask.1
	ShardReplicaLegacyTotal int32 // Conditional: item.FieldsMask.1
}

func (StatshouseSendKeepAliveBytes) TLName() string { return "statshouse.sendKeepAlive" }
func (StatshouseSendKeepAliveBytes) TLTag() uint32  { return 0x3285ff53 }

func (item *StatshouseSendKeepAliveBytes) SetShardReplica(v int32) {
	item.ShardReplica = v
	item.FieldsMask |= 1 << 5
}
func (item *StatshouseSendKeepAliveBytes) ClearShardReplica() {
	item.ShardReplica = 0
	item.FieldsMask &^= 1 << 5
}
func (item *StatshouseSendKeepAliveBytes) IsSetShardReplica() bool {
	return item.FieldsMask&(1<<5) != 0
}

func (item *StatshouseSendKeepAliveBytes) SetShardReplicaTotal(v int32) {
	item.ShardReplicaTotal = v
	item.FieldsMask |= 1 << 5
}
func (item *StatshouseSendKeepAliveBytes) ClearShardReplicaTotal() {
	item.ShardReplicaTotal = 0
	item.FieldsMask &^= 1 << 5
}
func (item *StatshouseSendKeepAliveBytes) IsSetShardReplicaTotal() bool {
	return item.FieldsMask&(1<<5) != 0
}

func (item *StatshouseSendKeepAliveBytes) SetIngressProxy(v bool) {
	if v {
		item.FieldsMask |= 1 << 6
	} else {
		item.FieldsMask &^= 1 << 6
	}
}
func (item *StatshouseSendKeepAliveBytes) IsSetIngressProxy() bool {
	return item.FieldsMask&(1<<6) != 0
}

func (item *StatshouseSendKeepAliveBytes) SetSourceEnv(v int32) {
	item.SourceEnv = v
	item.FieldsMask |= 1 << 0
}
func (item *StatshouseSendKeepAliveBytes) ClearSourceEnv() {
	item.SourceEnv = 0
	item.FieldsMask &^= 1 << 0
}
func (item *StatshouseSendKeepAliveBytes) IsSetSourceEnv() bool { return item.FieldsMask&(1<<0) != 0 }

func (item *StatshouseSendKeepAliveBytes) SetBuildArch(v int32) {
	item.BuildArch = v
	item.FieldsMask |= 1 << 2
}
func (item *StatshouseSendKeepAliveBytes) ClearBuildArch() {
	item.BuildArch = 0
	item.FieldsMask &^= 1 << 2
}
func (item *StatshouseSendKeepAliveBytes) IsSetBuildArch() bool { return item.FieldsMask&(1<<2) != 0 }

func (item *StatshouseSendKeepAliveBytes) SetShardReplicaLegacy(v int32) {
	item.ShardReplicaLegacy = v
	item.FieldsMask |= 1 << 1
}
func (item *StatshouseSendKeepAliveBytes) ClearShardReplicaLegacy() {
	item.ShardReplicaLegacy = 0
	item.FieldsMask &^= 1 << 1
}
func (item *StatshouseSendKeepAliveBytes) IsSetShardReplicaLegacy() bool {
	return item.FieldsMask&(1<<1) != 0
}

func (item *StatshouseSendKeepAliveBytes) SetShardReplicaLegacyTotal(v int32) {
	item.ShardReplicaLegacyTotal = v
	item.FieldsMask |= 1 << 1
}
func (item *StatshouseSendKeepAliveBytes) ClearShardReplicaLegacyTotal() {
	item.ShardReplicaLegacyTotal = 0
	item.FieldsMask &^= 1 << 1
}
func (item *StatshouseSendKeepAliveBytes) IsSetShardReplicaLegacyTotal() bool {
	return item.FieldsMask&(1<<1) != 0
}

func (item *StatshouseSendKeepAliveBytes) Reset() {
	item.FieldsMask = 0
	item.ShardReplica = 0
	item.ShardReplicaTotal = 0
	item.HostName = item.HostName[:0]
	item.SourceEnv = 0
	item.BuildArch = 0
	item.ShardReplicaLegacy = 0
	item.ShardReplicaLegacyTotal = 0
}

func (item *StatshouseSendKeepAliveBytes) Read(w []byte) (_ []byte, err error) {
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
	if w, err = basictl.StringReadBytes(w, &item.HostName); err != nil {
		return w, err
	}
	if item.FieldsMask&(1<<0) != 0 {
		if w, err = basictl.IntRead(w, &item.SourceEnv); err != nil {
			return w, err
		}
	} else {
		item.SourceEnv = 0
	}
	if item.FieldsMask&(1<<2) != 0 {
		if w, err = basictl.IntRead(w, &item.BuildArch); err != nil {
			return w, err
		}
	} else {
		item.BuildArch = 0
	}
	if item.FieldsMask&(1<<1) != 0 {
		if w, err = basictl.IntRead(w, &item.ShardReplicaLegacy); err != nil {
			return w, err
		}
	} else {
		item.ShardReplicaLegacy = 0
	}
	if item.FieldsMask&(1<<1) != 0 {
		if w, err = basictl.IntRead(w, &item.ShardReplicaLegacyTotal); err != nil {
			return w, err
		}
	} else {
		item.ShardReplicaLegacyTotal = 0
	}
	return w, nil
}

func (item *StatshouseSendKeepAliveBytes) Write(w []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, item.FieldsMask)
	if item.FieldsMask&(1<<5) != 0 {
		w = basictl.IntWrite(w, item.ShardReplica)
	}
	if item.FieldsMask&(1<<5) != 0 {
		w = basictl.IntWrite(w, item.ShardReplicaTotal)
	}
	if w, err = basictl.StringWriteBytes(w, item.HostName); err != nil {
		return w, err
	}
	if item.FieldsMask&(1<<0) != 0 {
		w = basictl.IntWrite(w, item.SourceEnv)
	}
	if item.FieldsMask&(1<<2) != 0 {
		w = basictl.IntWrite(w, item.BuildArch)
	}
	if item.FieldsMask&(1<<1) != 0 {
		w = basictl.IntWrite(w, item.ShardReplicaLegacy)
	}
	if item.FieldsMask&(1<<1) != 0 {
		w = basictl.IntWrite(w, item.ShardReplicaLegacyTotal)
	}
	return w, nil
}

func (item *StatshouseSendKeepAliveBytes) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x3285ff53); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *StatshouseSendKeepAliveBytes) WriteBoxed(w []byte) ([]byte, error) {
	w = basictl.NatWrite(w, 0x3285ff53)
	return item.Write(w)
}

func (item *StatshouseSendKeepAliveBytes) ReadResult(w []byte, ret *[]byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xb5286e24); err != nil {
		return w, err
	}
	return basictl.StringReadBytes(w, ret)
}

func (item *StatshouseSendKeepAliveBytes) WriteResult(w []byte, ret []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, 0xb5286e24)
	return basictl.StringWriteBytes(w, ret)
}

func (item *StatshouseSendKeepAliveBytes) ReadResultJSON(j interface{}, ret *[]byte) error {
	if err := JsonReadStringBytes(j, ret); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseSendKeepAliveBytes) WriteResultJSON(w []byte, ret []byte) (_ []byte, err error) {
	w = basictl.JSONWriteStringBytes(w, ret)
	return w, nil
}

func (item *StatshouseSendKeepAliveBytes) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret []byte
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *StatshouseSendKeepAliveBytes) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	j, err := JsonBytesToInterface(r)
	if err != nil {
		return r, w, ErrorInvalidJSON("statshouse.sendKeepAlive", err.Error())
	}
	var ret []byte
	if err = item.ReadResultJSON(j, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item StatshouseSendKeepAliveBytes) String() string {
	w, err := item.WriteJSON(nil)
	if err != nil {
		return err.Error()
	}
	return string(w)
}

func StatshouseSendKeepAliveBytes__ReadJSON(item *StatshouseSendKeepAliveBytes, j interface{}) error {
	return item.readJSON(j)
}
func (item *StatshouseSendKeepAliveBytes) readJSON(j interface{}) error {
	_jm, _ok := j.(map[string]interface{})
	if j != nil && !_ok {
		return ErrorInvalidJSON("statshouse.sendKeepAlive", "expected json object")
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
	_jHostName := _jm["host_name"]
	delete(_jm, "host_name")
	if err := JsonReadStringBytes(_jHostName, &item.HostName); err != nil {
		return err
	}
	_jSourceEnv := _jm["source_env"]
	delete(_jm, "source_env")
	_jBuildArch := _jm["build_arch"]
	delete(_jm, "build_arch")
	_jShardReplicaLegacy := _jm["shard_replica_legacy"]
	delete(_jm, "shard_replica_legacy")
	_jShardReplicaLegacyTotal := _jm["shard_replica_legacy_total"]
	delete(_jm, "shard_replica_legacy_total")
	for k := range _jm {
		return ErrorInvalidJSONExcessElement("statshouse.sendKeepAlive", k)
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
	if _jSourceEnv != nil {
		item.FieldsMask |= 1 << 0
	}
	if _jBuildArch != nil {
		item.FieldsMask |= 1 << 2
	}
	if _jShardReplicaLegacy != nil {
		item.FieldsMask |= 1 << 1
	}
	if _jShardReplicaLegacyTotal != nil {
		item.FieldsMask |= 1 << 1
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
	if _jSourceEnv != nil {
		if err := JsonReadInt32(_jSourceEnv, &item.SourceEnv); err != nil {
			return err
		}
	} else {
		item.SourceEnv = 0
	}
	if _jBuildArch != nil {
		if err := JsonReadInt32(_jBuildArch, &item.BuildArch); err != nil {
			return err
		}
	} else {
		item.BuildArch = 0
	}
	if _jShardReplicaLegacy != nil {
		if err := JsonReadInt32(_jShardReplicaLegacy, &item.ShardReplicaLegacy); err != nil {
			return err
		}
	} else {
		item.ShardReplicaLegacy = 0
	}
	if _jShardReplicaLegacyTotal != nil {
		if err := JsonReadInt32(_jShardReplicaLegacyTotal, &item.ShardReplicaLegacyTotal); err != nil {
			return err
		}
	} else {
		item.ShardReplicaLegacyTotal = 0
	}
	return nil
}

func (item *StatshouseSendKeepAliveBytes) WriteJSON(w []byte) (_ []byte, err error) {
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
	if len(item.HostName) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"host_name":`...)
		w = basictl.JSONWriteStringBytes(w, item.HostName)
	}
	if item.FieldsMask&(1<<0) != 0 {
		if item.SourceEnv != 0 {
			w = basictl.JSONAddCommaIfNeeded(w)
			w = append(w, `"source_env":`...)
			w = basictl.JSONWriteInt32(w, item.SourceEnv)
		}
	}
	if item.FieldsMask&(1<<2) != 0 {
		if item.BuildArch != 0 {
			w = basictl.JSONAddCommaIfNeeded(w)
			w = append(w, `"build_arch":`...)
			w = basictl.JSONWriteInt32(w, item.BuildArch)
		}
	}
	if item.FieldsMask&(1<<1) != 0 {
		if item.ShardReplicaLegacy != 0 {
			w = basictl.JSONAddCommaIfNeeded(w)
			w = append(w, `"shard_replica_legacy":`...)
			w = basictl.JSONWriteInt32(w, item.ShardReplicaLegacy)
		}
	}
	if item.FieldsMask&(1<<1) != 0 {
		if item.ShardReplicaLegacyTotal != 0 {
			w = basictl.JSONAddCommaIfNeeded(w)
			w = append(w, `"shard_replica_legacy_total":`...)
			w = basictl.JSONWriteInt32(w, item.ShardReplicaLegacyTotal)
		}
	}
	return append(w, '}'), nil
}

func (item *StatshouseSendKeepAliveBytes) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil)
}

func (item *StatshouseSendKeepAliveBytes) UnmarshalJSON(b []byte) error {
	j, err := JsonBytesToInterface(b)
	if err != nil {
		return ErrorInvalidJSON("statshouse.sendKeepAlive", err.Error())
	}
	if err = item.readJSON(j); err != nil {
		return ErrorInvalidJSON("statshouse.sendKeepAlive", err.Error())
	}
	return nil
}

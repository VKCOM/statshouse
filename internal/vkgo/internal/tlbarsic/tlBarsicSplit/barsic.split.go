// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Code generated by vktl/cmd/tlgen2; DO NOT EDIT.
package tlBarsicSplit

import (
	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/internal"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tl/tlTrue"
)

var _ = basictl.NatWrite
var _ = internal.ErrorInvalidEnumTag

type BarsicSplit struct {
	FieldsMask   uint32
	Offset       int64
	ShardId      string
	EpochNumber  int64
	ViewNumber   int64
	SnapshotMeta string
}

func (BarsicSplit) TLName() string { return "barsic.split" }
func (BarsicSplit) TLTag() uint32  { return 0xaf4c92d8 }

func (item *BarsicSplit) Reset() {
	item.FieldsMask = 0
	item.Offset = 0
	item.ShardId = ""
	item.EpochNumber = 0
	item.ViewNumber = 0
	item.SnapshotMeta = ""
}

func (item *BarsicSplit) FillRandom(rg *basictl.RandGenerator) {
	item.FieldsMask = basictl.RandomUint(rg)
	item.Offset = basictl.RandomLong(rg)
	item.ShardId = basictl.RandomString(rg)
	item.EpochNumber = basictl.RandomLong(rg)
	item.ViewNumber = basictl.RandomLong(rg)
	item.SnapshotMeta = basictl.RandomString(rg)
}

func (item *BarsicSplit) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	if w, err = basictl.LongRead(w, &item.Offset); err != nil {
		return w, err
	}
	if w, err = basictl.StringRead(w, &item.ShardId); err != nil {
		return w, err
	}
	if w, err = basictl.LongRead(w, &item.EpochNumber); err != nil {
		return w, err
	}
	if w, err = basictl.LongRead(w, &item.ViewNumber); err != nil {
		return w, err
	}
	return basictl.StringRead(w, &item.SnapshotMeta)
}

func (item *BarsicSplit) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *BarsicSplit) Write(w []byte) []byte {
	w = basictl.NatWrite(w, item.FieldsMask)
	w = basictl.LongWrite(w, item.Offset)
	w = basictl.StringWrite(w, item.ShardId)
	w = basictl.LongWrite(w, item.EpochNumber)
	w = basictl.LongWrite(w, item.ViewNumber)
	w = basictl.StringWrite(w, item.SnapshotMeta)
	return w
}

func (item *BarsicSplit) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xaf4c92d8); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *BarsicSplit) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *BarsicSplit) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0xaf4c92d8)
	return item.Write(w)
}

func (item *BarsicSplit) ReadResult(w []byte, ret *tlTrue.True) (_ []byte, err error) {
	return ret.ReadBoxed(w)
}

func (item *BarsicSplit) WriteResult(w []byte, ret tlTrue.True) (_ []byte, err error) {
	w = ret.WriteBoxed(w)
	return w, nil
}

func (item *BarsicSplit) ReadResultJSON(legacyTypeNames bool, in *basictl.JsonLexer, ret *tlTrue.True) error {
	if err := ret.ReadJSON(legacyTypeNames, in); err != nil {
		return err
	}
	return nil
}

func (item *BarsicSplit) WriteResultJSON(w []byte, ret tlTrue.True) (_ []byte, err error) {
	return item.writeResultJSON(true, false, w, ret)
}

func (item *BarsicSplit) writeResultJSON(newTypeNames bool, short bool, w []byte, ret tlTrue.True) (_ []byte, err error) {
	w = ret.WriteJSONOpt(newTypeNames, short, w)
	return w, nil
}

func (item *BarsicSplit) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret tlTrue.True
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *BarsicSplit) ReadResultWriteResultJSONOpt(newTypeNames bool, short bool, r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret tlTrue.True
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.writeResultJSON(newTypeNames, short, w, ret)
	return r, w, err
}

func (item *BarsicSplit) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	var ret tlTrue.True
	err := item.ReadResultJSON(true, &basictl.JsonLexer{Data: r}, &ret)
	if err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item BarsicSplit) String() string {
	return string(item.WriteJSON(nil))
}

func (item *BarsicSplit) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propFieldsMaskPresented bool
	var propOffsetPresented bool
	var propShardIdPresented bool
	var propEpochNumberPresented bool
	var propViewNumberPresented bool
	var propSnapshotMetaPresented bool

	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return in.Error()
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			switch key {
			case "fields_mask":
				if propFieldsMaskPresented {
					return internal.ErrorInvalidJSONWithDuplicatingKeys("barsic.split", "fields_mask")
				}
				if err := internal.Json2ReadUint32(in, &item.FieldsMask); err != nil {
					return err
				}
				propFieldsMaskPresented = true
			case "offset":
				if propOffsetPresented {
					return internal.ErrorInvalidJSONWithDuplicatingKeys("barsic.split", "offset")
				}
				if err := internal.Json2ReadInt64(in, &item.Offset); err != nil {
					return err
				}
				propOffsetPresented = true
			case "shard_id":
				if propShardIdPresented {
					return internal.ErrorInvalidJSONWithDuplicatingKeys("barsic.split", "shard_id")
				}
				if err := internal.Json2ReadString(in, &item.ShardId); err != nil {
					return err
				}
				propShardIdPresented = true
			case "epoch_number":
				if propEpochNumberPresented {
					return internal.ErrorInvalidJSONWithDuplicatingKeys("barsic.split", "epoch_number")
				}
				if err := internal.Json2ReadInt64(in, &item.EpochNumber); err != nil {
					return err
				}
				propEpochNumberPresented = true
			case "view_number":
				if propViewNumberPresented {
					return internal.ErrorInvalidJSONWithDuplicatingKeys("barsic.split", "view_number")
				}
				if err := internal.Json2ReadInt64(in, &item.ViewNumber); err != nil {
					return err
				}
				propViewNumberPresented = true
			case "snapshot_meta":
				if propSnapshotMetaPresented {
					return internal.ErrorInvalidJSONWithDuplicatingKeys("barsic.split", "snapshot_meta")
				}
				if err := internal.Json2ReadString(in, &item.SnapshotMeta); err != nil {
					return err
				}
				propSnapshotMetaPresented = true
			default:
				return internal.ErrorInvalidJSONExcessElement("barsic.split", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propFieldsMaskPresented {
		item.FieldsMask = 0
	}
	if !propOffsetPresented {
		item.Offset = 0
	}
	if !propShardIdPresented {
		item.ShardId = ""
	}
	if !propEpochNumberPresented {
		item.EpochNumber = 0
	}
	if !propViewNumberPresented {
		item.ViewNumber = 0
	}
	if !propSnapshotMetaPresented {
		item.SnapshotMeta = ""
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *BarsicSplit) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *BarsicSplit) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *BarsicSplit) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexFieldsMask := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"fields_mask":`...)
	w = basictl.JSONWriteUint32(w, item.FieldsMask)
	if (item.FieldsMask != 0) == false {
		w = w[:backupIndexFieldsMask]
	}
	backupIndexOffset := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"offset":`...)
	w = basictl.JSONWriteInt64(w, item.Offset)
	if (item.Offset != 0) == false {
		w = w[:backupIndexOffset]
	}
	backupIndexShardId := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"shard_id":`...)
	w = basictl.JSONWriteString(w, item.ShardId)
	if (len(item.ShardId) != 0) == false {
		w = w[:backupIndexShardId]
	}
	backupIndexEpochNumber := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"epoch_number":`...)
	w = basictl.JSONWriteInt64(w, item.EpochNumber)
	if (item.EpochNumber != 0) == false {
		w = w[:backupIndexEpochNumber]
	}
	backupIndexViewNumber := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"view_number":`...)
	w = basictl.JSONWriteInt64(w, item.ViewNumber)
	if (item.ViewNumber != 0) == false {
		w = w[:backupIndexViewNumber]
	}
	backupIndexSnapshotMeta := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"snapshot_meta":`...)
	w = basictl.JSONWriteString(w, item.SnapshotMeta)
	if (len(item.SnapshotMeta) != 0) == false {
		w = w[:backupIndexSnapshotMeta]
	}
	return append(w, '}')
}

func (item *BarsicSplit) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *BarsicSplit) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return internal.ErrorInvalidJSON("barsic.split", err.Error())
	}
	return nil
}

func (item *BarsicSplit) WriteTL2(w []byte, ctx *basictl.TL2WriteContext) []byte {
	return w
}

func (item *BarsicSplit) ReadTL2(r []byte, ctx *basictl.TL2ReadContext) (_ []byte, err error) {
	return r, internal.ErrorTL2SerializersNotGenerated("barsic.split")
}

type BarsicSplitBytes struct {
	FieldsMask   uint32
	Offset       int64
	ShardId      []byte
	EpochNumber  int64
	ViewNumber   int64
	SnapshotMeta []byte
}

func (BarsicSplitBytes) TLName() string { return "barsic.split" }
func (BarsicSplitBytes) TLTag() uint32  { return 0xaf4c92d8 }

func (item *BarsicSplitBytes) Reset() {
	item.FieldsMask = 0
	item.Offset = 0
	item.ShardId = item.ShardId[:0]
	item.EpochNumber = 0
	item.ViewNumber = 0
	item.SnapshotMeta = item.SnapshotMeta[:0]
}

func (item *BarsicSplitBytes) FillRandom(rg *basictl.RandGenerator) {
	item.FieldsMask = basictl.RandomUint(rg)
	item.Offset = basictl.RandomLong(rg)
	item.ShardId = basictl.RandomStringBytes(rg)
	item.EpochNumber = basictl.RandomLong(rg)
	item.ViewNumber = basictl.RandomLong(rg)
	item.SnapshotMeta = basictl.RandomStringBytes(rg)
}

func (item *BarsicSplitBytes) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	if w, err = basictl.LongRead(w, &item.Offset); err != nil {
		return w, err
	}
	if w, err = basictl.StringReadBytes(w, &item.ShardId); err != nil {
		return w, err
	}
	if w, err = basictl.LongRead(w, &item.EpochNumber); err != nil {
		return w, err
	}
	if w, err = basictl.LongRead(w, &item.ViewNumber); err != nil {
		return w, err
	}
	return basictl.StringReadBytes(w, &item.SnapshotMeta)
}

func (item *BarsicSplitBytes) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *BarsicSplitBytes) Write(w []byte) []byte {
	w = basictl.NatWrite(w, item.FieldsMask)
	w = basictl.LongWrite(w, item.Offset)
	w = basictl.StringWriteBytes(w, item.ShardId)
	w = basictl.LongWrite(w, item.EpochNumber)
	w = basictl.LongWrite(w, item.ViewNumber)
	w = basictl.StringWriteBytes(w, item.SnapshotMeta)
	return w
}

func (item *BarsicSplitBytes) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xaf4c92d8); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *BarsicSplitBytes) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *BarsicSplitBytes) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0xaf4c92d8)
	return item.Write(w)
}

func (item *BarsicSplitBytes) ReadResult(w []byte, ret *tlTrue.True) (_ []byte, err error) {
	return ret.ReadBoxed(w)
}

func (item *BarsicSplitBytes) WriteResult(w []byte, ret tlTrue.True) (_ []byte, err error) {
	w = ret.WriteBoxed(w)
	return w, nil
}

func (item *BarsicSplitBytes) ReadResultJSON(legacyTypeNames bool, in *basictl.JsonLexer, ret *tlTrue.True) error {
	if err := ret.ReadJSON(legacyTypeNames, in); err != nil {
		return err
	}
	return nil
}

func (item *BarsicSplitBytes) WriteResultJSON(w []byte, ret tlTrue.True) (_ []byte, err error) {
	return item.writeResultJSON(true, false, w, ret)
}

func (item *BarsicSplitBytes) writeResultJSON(newTypeNames bool, short bool, w []byte, ret tlTrue.True) (_ []byte, err error) {
	w = ret.WriteJSONOpt(newTypeNames, short, w)
	return w, nil
}

func (item *BarsicSplitBytes) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret tlTrue.True
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *BarsicSplitBytes) ReadResultWriteResultJSONOpt(newTypeNames bool, short bool, r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret tlTrue.True
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.writeResultJSON(newTypeNames, short, w, ret)
	return r, w, err
}

func (item *BarsicSplitBytes) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	var ret tlTrue.True
	err := item.ReadResultJSON(true, &basictl.JsonLexer{Data: r}, &ret)
	if err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item BarsicSplitBytes) String() string {
	return string(item.WriteJSON(nil))
}

func (item *BarsicSplitBytes) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propFieldsMaskPresented bool
	var propOffsetPresented bool
	var propShardIdPresented bool
	var propEpochNumberPresented bool
	var propViewNumberPresented bool
	var propSnapshotMetaPresented bool

	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return in.Error()
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			switch key {
			case "fields_mask":
				if propFieldsMaskPresented {
					return internal.ErrorInvalidJSONWithDuplicatingKeys("barsic.split", "fields_mask")
				}
				if err := internal.Json2ReadUint32(in, &item.FieldsMask); err != nil {
					return err
				}
				propFieldsMaskPresented = true
			case "offset":
				if propOffsetPresented {
					return internal.ErrorInvalidJSONWithDuplicatingKeys("barsic.split", "offset")
				}
				if err := internal.Json2ReadInt64(in, &item.Offset); err != nil {
					return err
				}
				propOffsetPresented = true
			case "shard_id":
				if propShardIdPresented {
					return internal.ErrorInvalidJSONWithDuplicatingKeys("barsic.split", "shard_id")
				}
				if err := internal.Json2ReadStringBytes(in, &item.ShardId); err != nil {
					return err
				}
				propShardIdPresented = true
			case "epoch_number":
				if propEpochNumberPresented {
					return internal.ErrorInvalidJSONWithDuplicatingKeys("barsic.split", "epoch_number")
				}
				if err := internal.Json2ReadInt64(in, &item.EpochNumber); err != nil {
					return err
				}
				propEpochNumberPresented = true
			case "view_number":
				if propViewNumberPresented {
					return internal.ErrorInvalidJSONWithDuplicatingKeys("barsic.split", "view_number")
				}
				if err := internal.Json2ReadInt64(in, &item.ViewNumber); err != nil {
					return err
				}
				propViewNumberPresented = true
			case "snapshot_meta":
				if propSnapshotMetaPresented {
					return internal.ErrorInvalidJSONWithDuplicatingKeys("barsic.split", "snapshot_meta")
				}
				if err := internal.Json2ReadStringBytes(in, &item.SnapshotMeta); err != nil {
					return err
				}
				propSnapshotMetaPresented = true
			default:
				return internal.ErrorInvalidJSONExcessElement("barsic.split", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propFieldsMaskPresented {
		item.FieldsMask = 0
	}
	if !propOffsetPresented {
		item.Offset = 0
	}
	if !propShardIdPresented {
		item.ShardId = item.ShardId[:0]
	}
	if !propEpochNumberPresented {
		item.EpochNumber = 0
	}
	if !propViewNumberPresented {
		item.ViewNumber = 0
	}
	if !propSnapshotMetaPresented {
		item.SnapshotMeta = item.SnapshotMeta[:0]
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *BarsicSplitBytes) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *BarsicSplitBytes) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *BarsicSplitBytes) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexFieldsMask := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"fields_mask":`...)
	w = basictl.JSONWriteUint32(w, item.FieldsMask)
	if (item.FieldsMask != 0) == false {
		w = w[:backupIndexFieldsMask]
	}
	backupIndexOffset := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"offset":`...)
	w = basictl.JSONWriteInt64(w, item.Offset)
	if (item.Offset != 0) == false {
		w = w[:backupIndexOffset]
	}
	backupIndexShardId := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"shard_id":`...)
	w = basictl.JSONWriteStringBytes(w, item.ShardId)
	if (len(item.ShardId) != 0) == false {
		w = w[:backupIndexShardId]
	}
	backupIndexEpochNumber := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"epoch_number":`...)
	w = basictl.JSONWriteInt64(w, item.EpochNumber)
	if (item.EpochNumber != 0) == false {
		w = w[:backupIndexEpochNumber]
	}
	backupIndexViewNumber := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"view_number":`...)
	w = basictl.JSONWriteInt64(w, item.ViewNumber)
	if (item.ViewNumber != 0) == false {
		w = w[:backupIndexViewNumber]
	}
	backupIndexSnapshotMeta := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"snapshot_meta":`...)
	w = basictl.JSONWriteStringBytes(w, item.SnapshotMeta)
	if (len(item.SnapshotMeta) != 0) == false {
		w = w[:backupIndexSnapshotMeta]
	}
	return append(w, '}')
}

func (item *BarsicSplitBytes) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *BarsicSplitBytes) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return internal.ErrorInvalidJSON("barsic.split", err.Error())
	}
	return nil
}

func (item *BarsicSplitBytes) WriteTL2(w []byte, ctx *basictl.TL2WriteContext) []byte {
	return w
}

func (item *BarsicSplitBytes) ReadTL2(r []byte, ctx *basictl.TL2ReadContext) (_ []byte, err error) {
	return r, internal.ErrorTL2SerializersNotGenerated("barsic.split")
}

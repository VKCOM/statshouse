// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Code generated by vktl/cmd/tlgen2; DO NOT EDIT.
package internal

import (
	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
)

var _ = basictl.NatWrite

func BuiltinVectorEngineBinlogPrefixRead(w []byte, vec *[]EngineBinlogPrefix) (_ []byte, err error) {
	var l uint32
	if w, err = basictl.NatRead(w, &l); err != nil {
		return w, err
	}
	if err = basictl.CheckLengthSanity(w, l, 4); err != nil {
		return w, err
	}
	if uint32(cap(*vec)) < l {
		*vec = make([]EngineBinlogPrefix, l)
	} else {
		*vec = (*vec)[:l]
	}
	for i := range *vec {
		if w, err = (*vec)[i].Read(w); err != nil {
			return w, err
		}
	}
	return w, nil
}

func BuiltinVectorEngineBinlogPrefixWrite(w []byte, vec []EngineBinlogPrefix) []byte {
	w = basictl.NatWrite(w, uint32(len(vec)))
	for _, elem := range vec {
		w = elem.Write(w)
	}
	return w
}

func BuiltinVectorEngineBinlogPrefixReadJSON(legacyTypeNames bool, in *basictl.JsonLexer, vec *[]EngineBinlogPrefix) error {
	*vec = (*vec)[:cap(*vec)]
	index := 0
	if in != nil {
		in.Delim('[')
		if !in.Ok() {
			return ErrorInvalidJSON("[]EngineBinlogPrefix", "expected json array")
		}
		for ; !in.IsDelim(']'); index++ {
			if len(*vec) <= index {
				var newValue EngineBinlogPrefix
				*vec = append(*vec, newValue)
				*vec = (*vec)[:cap(*vec)]
			}
			if err := (*vec)[index].ReadJSON(legacyTypeNames, in); err != nil {
				return err
			}
			in.WantComma()
		}
		in.Delim(']')
		if !in.Ok() {
			return ErrorInvalidJSON("[]EngineBinlogPrefix", "expected json array's end")
		}
	}
	*vec = (*vec)[:index]
	return nil
}

func BuiltinVectorEngineBinlogPrefixWriteJSON(w []byte, vec []EngineBinlogPrefix) []byte {
	return BuiltinVectorEngineBinlogPrefixWriteJSONOpt(true, false, w, vec)
}
func BuiltinVectorEngineBinlogPrefixWriteJSONOpt(newTypeNames bool, short bool, w []byte, vec []EngineBinlogPrefix) []byte {
	w = append(w, '[')
	for _, elem := range vec {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = elem.WriteJSONOpt(newTypeNames, short, w)
	}
	return append(w, ']')
}

type EngineBinlogPrefix struct {
	BinlogPrefix   string
	SnapshotPrefix string
}

func (EngineBinlogPrefix) TLName() string { return "engine.binlogPrefix" }
func (EngineBinlogPrefix) TLTag() uint32  { return 0x4c09c894 }

func (item *EngineBinlogPrefix) Reset() {
	item.BinlogPrefix = ""
	item.SnapshotPrefix = ""
}

func (item *EngineBinlogPrefix) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.StringRead(w, &item.BinlogPrefix); err != nil {
		return w, err
	}
	return basictl.StringRead(w, &item.SnapshotPrefix)
}

// This method is general version of Write, use it instead!
func (item *EngineBinlogPrefix) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *EngineBinlogPrefix) Write(w []byte) []byte {
	w = basictl.StringWrite(w, item.BinlogPrefix)
	w = basictl.StringWrite(w, item.SnapshotPrefix)
	return w
}

func (item *EngineBinlogPrefix) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x4c09c894); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *EngineBinlogPrefix) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *EngineBinlogPrefix) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x4c09c894)
	return item.Write(w)
}

func (item EngineBinlogPrefix) String() string {
	return string(item.WriteJSON(nil))
}

func (item *EngineBinlogPrefix) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propBinlogPrefixPresented bool
	var propSnapshotPrefixPresented bool

	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return in.Error()
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			switch key {
			case "binlog_prefix":
				if propBinlogPrefixPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("engine.binlogPrefix", "binlog_prefix")
				}
				if err := Json2ReadString(in, &item.BinlogPrefix); err != nil {
					return err
				}
				propBinlogPrefixPresented = true
			case "snapshot_prefix":
				if propSnapshotPrefixPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("engine.binlogPrefix", "snapshot_prefix")
				}
				if err := Json2ReadString(in, &item.SnapshotPrefix); err != nil {
					return err
				}
				propSnapshotPrefixPresented = true
			default:
				return ErrorInvalidJSONExcessElement("engine.binlogPrefix", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propBinlogPrefixPresented {
		item.BinlogPrefix = ""
	}
	if !propSnapshotPrefixPresented {
		item.SnapshotPrefix = ""
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *EngineBinlogPrefix) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *EngineBinlogPrefix) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *EngineBinlogPrefix) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexBinlogPrefix := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"binlog_prefix":`...)
	w = basictl.JSONWriteString(w, item.BinlogPrefix)
	if (len(item.BinlogPrefix) != 0) == false {
		w = w[:backupIndexBinlogPrefix]
	}
	backupIndexSnapshotPrefix := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"snapshot_prefix":`...)
	w = basictl.JSONWriteString(w, item.SnapshotPrefix)
	if (len(item.SnapshotPrefix) != 0) == false {
		w = w[:backupIndexSnapshotPrefix]
	}
	return append(w, '}')
}

func (item *EngineBinlogPrefix) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *EngineBinlogPrefix) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("engine.binlogPrefix", err.Error())
	}
	return nil
}

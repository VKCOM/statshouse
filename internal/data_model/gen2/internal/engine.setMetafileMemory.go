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

type EngineSetMetafileMemory struct {
	Megabytes int32
}

func (EngineSetMetafileMemory) TLName() string { return "engine.setMetafileMemory" }
func (EngineSetMetafileMemory) TLTag() uint32  { return 0x7bdcf404 }

func (item *EngineSetMetafileMemory) Reset() {
	item.Megabytes = 0
}

func (item *EngineSetMetafileMemory) Read(w []byte) (_ []byte, err error) {
	return basictl.IntRead(w, &item.Megabytes)
}

// This method is general version of Write, use it instead!
func (item *EngineSetMetafileMemory) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *EngineSetMetafileMemory) Write(w []byte) []byte {
	w = basictl.IntWrite(w, item.Megabytes)
	return w
}

func (item *EngineSetMetafileMemory) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x7bdcf404); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *EngineSetMetafileMemory) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *EngineSetMetafileMemory) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x7bdcf404)
	return item.Write(w)
}

func (item *EngineSetMetafileMemory) ReadResult(w []byte, ret *BoolStat) (_ []byte, err error) {
	return ret.ReadBoxed(w)
}

func (item *EngineSetMetafileMemory) WriteResult(w []byte, ret BoolStat) (_ []byte, err error) {
	w = ret.WriteBoxed(w)
	return w, nil
}

func (item *EngineSetMetafileMemory) ReadResultJSON(legacyTypeNames bool, in *basictl.JsonLexer, ret *BoolStat) error {
	if err := ret.ReadJSON(legacyTypeNames, in); err != nil {
		return err
	}
	return nil
}

func (item *EngineSetMetafileMemory) WriteResultJSON(w []byte, ret BoolStat) (_ []byte, err error) {
	return item.writeResultJSON(true, false, w, ret)
}

func (item *EngineSetMetafileMemory) writeResultJSON(newTypeNames bool, short bool, w []byte, ret BoolStat) (_ []byte, err error) {
	w = ret.WriteJSONOpt(newTypeNames, short, w)
	return w, nil
}

func (item *EngineSetMetafileMemory) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret BoolStat
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *EngineSetMetafileMemory) ReadResultWriteResultJSONOpt(newTypeNames bool, short bool, r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret BoolStat
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.writeResultJSON(newTypeNames, short, w, ret)
	return r, w, err
}

func (item *EngineSetMetafileMemory) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	var ret BoolStat
	err := item.ReadResultJSON(true, &basictl.JsonLexer{Data: r}, &ret)
	if err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item EngineSetMetafileMemory) String() string {
	return string(item.WriteJSON(nil))
}

func (item *EngineSetMetafileMemory) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propMegabytesPresented bool

	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return in.Error()
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			switch key {
			case "megabytes":
				if propMegabytesPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("engine.setMetafileMemory", "megabytes")
				}
				if err := Json2ReadInt32(in, &item.Megabytes); err != nil {
					return err
				}
				propMegabytesPresented = true
			default:
				return ErrorInvalidJSONExcessElement("engine.setMetafileMemory", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propMegabytesPresented {
		item.Megabytes = 0
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *EngineSetMetafileMemory) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *EngineSetMetafileMemory) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *EngineSetMetafileMemory) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexMegabytes := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"megabytes":`...)
	w = basictl.JSONWriteInt32(w, item.Megabytes)
	if (item.Megabytes != 0) == false {
		w = w[:backupIndexMegabytes]
	}
	return append(w, '}')
}

func (item *EngineSetMetafileMemory) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *EngineSetMetafileMemory) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("engine.setMetafileMemory", err.Error())
	}
	return nil
}

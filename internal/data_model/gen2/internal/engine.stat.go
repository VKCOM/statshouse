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

type EngineStat struct {
}

func (EngineStat) TLName() string { return "engine.stat" }
func (EngineStat) TLTag() uint32  { return 0xefb3c36b }

func (item *EngineStat) Reset() {}

func (item *EngineStat) Read(w []byte) (_ []byte, err error) { return w, nil }

// This method is general version of Write, use it instead!
func (item *EngineStat) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *EngineStat) Write(w []byte) []byte {
	return w
}

func (item *EngineStat) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xefb3c36b); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *EngineStat) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *EngineStat) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0xefb3c36b)
	return item.Write(w)
}

func (item *EngineStat) ReadResult(w []byte, ret *Stat) (_ []byte, err error) {
	return ret.ReadBoxed(w)
}

func (item *EngineStat) WriteResult(w []byte, ret Stat) (_ []byte, err error) {
	w = ret.WriteBoxed(w)
	return w, nil
}

func (item *EngineStat) ReadResultJSON(legacyTypeNames bool, in *basictl.JsonLexer, ret *Stat) error {
	if err := ret.ReadJSON(legacyTypeNames, in); err != nil {
		return err
	}
	return nil
}

func (item *EngineStat) WriteResultJSON(w []byte, ret Stat) (_ []byte, err error) {
	return item.writeResultJSON(true, false, w, ret)
}

func (item *EngineStat) writeResultJSON(newTypeNames bool, short bool, w []byte, ret Stat) (_ []byte, err error) {
	w = ret.WriteJSONOpt(newTypeNames, short, w)
	return w, nil
}

func (item *EngineStat) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret Stat
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *EngineStat) ReadResultWriteResultJSONOpt(newTypeNames bool, short bool, r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret Stat
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.writeResultJSON(newTypeNames, short, w, ret)
	return r, w, err
}

func (item *EngineStat) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	var ret Stat
	err := item.ReadResultJSON(true, &basictl.JsonLexer{Data: r}, &ret)
	if err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item EngineStat) String() string {
	return string(item.WriteJSON(nil))
}

func (item *EngineStat) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return in.Error()
		}
		for !in.IsDelim('}') {
			return ErrorInvalidJSON("engine.stat", "this object can't have properties")
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *EngineStat) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *EngineStat) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *EngineStat) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	return append(w, '}')
}

func (item *EngineStat) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *EngineStat) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("engine.stat", err.Error())
	}
	return nil
}

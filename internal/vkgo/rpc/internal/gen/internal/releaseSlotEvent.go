// Copyright 2024 V Kontakte LLC
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

type ReleaseSlotEvent struct {
	Key ExactlyOnceUuid
}

func (ReleaseSlotEvent) TLName() string { return "releaseSlotEvent" }
func (ReleaseSlotEvent) TLTag() uint32  { return 0x7f045ccc }

func (item *ReleaseSlotEvent) Reset() {
	item.Key.Reset()
}

func (item *ReleaseSlotEvent) FillRandom(rg *basictl.RandGenerator) {
	item.Key.FillRandom(rg)
}

func (item *ReleaseSlotEvent) Read(w []byte) (_ []byte, err error) {
	return item.Key.Read(w)
}

// This method is general version of Write, use it instead!
func (item *ReleaseSlotEvent) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *ReleaseSlotEvent) Write(w []byte) []byte {
	w = item.Key.Write(w)
	return w
}

func (item *ReleaseSlotEvent) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x7f045ccc); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *ReleaseSlotEvent) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *ReleaseSlotEvent) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x7f045ccc)
	return item.Write(w)
}

func (item ReleaseSlotEvent) String() string {
	return string(item.WriteJSON(nil))
}

func (item *ReleaseSlotEvent) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propKeyPresented bool

	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return in.Error()
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			switch key {
			case "key":
				if propKeyPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("releaseSlotEvent", "key")
				}
				if err := item.Key.ReadJSON(legacyTypeNames, in); err != nil {
					return err
				}
				propKeyPresented = true
			default:
				return ErrorInvalidJSONExcessElement("releaseSlotEvent", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propKeyPresented {
		item.Key.Reset()
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *ReleaseSlotEvent) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *ReleaseSlotEvent) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *ReleaseSlotEvent) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"key":`...)
	w = item.Key.WriteJSONOpt(newTypeNames, short, w)
	return append(w, '}')
}

func (item *ReleaseSlotEvent) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *ReleaseSlotEvent) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("releaseSlotEvent", err.Error())
	}
	return nil
}
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

type String string

func (String) TLName() string { return "string" }
func (String) TLTag() uint32  { return 0xb5286e24 }

func (item *String) Reset() {
	ptr := (*string)(item)
	*ptr = ""
}

func (item *String) Read(w []byte) (_ []byte, err error) {
	ptr := (*string)(item)
	return basictl.StringRead(w, ptr)
}

// This method is general version of Write, use it instead!
func (item *String) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *String) Write(w []byte) []byte {
	ptr := (*string)(item)
	return basictl.StringWrite(w, *ptr)
}

func (item *String) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xb5286e24); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *String) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *String) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0xb5286e24)
	return item.Write(w)
}

func (item String) String() string {
	return string(item.WriteJSON(nil))
}

func (item *String) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	ptr := (*string)(item)
	if err := Json2ReadString(in, ptr); err != nil {
		return err
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *String) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSON(w), nil
}

func (item *String) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}

func (item *String) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	ptr := (*string)(item)
	w = basictl.JSONWriteString(w, *ptr)
	return w
}
func (item *String) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *String) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("string", err.Error())
	}
	return nil
}

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

type ReqResultHeader struct {
	Extra RpcReqResultExtra
}

func (ReqResultHeader) TLName() string { return "reqResultHeader" }
func (ReqResultHeader) TLTag() uint32  { return 0x8cc84ce1 }

func (item *ReqResultHeader) Reset() {
	item.Extra.Reset()
}

func (item *ReqResultHeader) Read(w []byte) (_ []byte, err error) {
	return item.Extra.Read(w)
}

// This method is general version of Write, use it instead!
func (item *ReqResultHeader) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *ReqResultHeader) Write(w []byte) []byte {
	w = item.Extra.Write(w)
	return w
}

func (item *ReqResultHeader) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x8cc84ce1); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *ReqResultHeader) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *ReqResultHeader) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x8cc84ce1)
	return item.Write(w)
}

func (item ReqResultHeader) String() string {
	return string(item.WriteJSON(nil))
}

func (item *ReqResultHeader) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propExtraPresented bool

	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return in.Error()
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			switch key {
			case "extra":
				if propExtraPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("reqResultHeader", "extra")
				}
				if err := item.Extra.ReadJSON(legacyTypeNames, in); err != nil {
					return err
				}
				propExtraPresented = true
			default:
				return ErrorInvalidJSONExcessElement("reqResultHeader", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propExtraPresented {
		item.Extra.Reset()
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *ReqResultHeader) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *ReqResultHeader) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *ReqResultHeader) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"extra":`...)
	w = item.Extra.WriteJSONOpt(newTypeNames, short, w)
	return append(w, '}')
}

func (item *ReqResultHeader) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *ReqResultHeader) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("reqResultHeader", err.Error())
	}
	return nil
}
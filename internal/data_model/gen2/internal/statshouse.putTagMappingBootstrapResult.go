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

type StatshousePutTagMappingBootstrapResult struct {
	CountInserted int32
}

func (StatshousePutTagMappingBootstrapResult) TLName() string {
	return "statshouse.putTagMappingBootstrapResult"
}
func (StatshousePutTagMappingBootstrapResult) TLTag() uint32 { return 0x486affde }

func (item *StatshousePutTagMappingBootstrapResult) Reset() {
	item.CountInserted = 0
}

func (item *StatshousePutTagMappingBootstrapResult) Read(w []byte) (_ []byte, err error) {
	return basictl.IntRead(w, &item.CountInserted)
}

// This method is general version of Write, use it instead!
func (item *StatshousePutTagMappingBootstrapResult) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *StatshousePutTagMappingBootstrapResult) Write(w []byte) []byte {
	w = basictl.IntWrite(w, item.CountInserted)
	return w
}

func (item *StatshousePutTagMappingBootstrapResult) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x486affde); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *StatshousePutTagMappingBootstrapResult) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *StatshousePutTagMappingBootstrapResult) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x486affde)
	return item.Write(w)
}

func (item StatshousePutTagMappingBootstrapResult) String() string {
	return string(item.WriteJSON(nil))
}

func (item *StatshousePutTagMappingBootstrapResult) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propCountInsertedPresented bool

	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return in.Error()
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			switch key {
			case "count_inserted":
				if propCountInsertedPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.putTagMappingBootstrapResult", "count_inserted")
				}
				if err := Json2ReadInt32(in, &item.CountInserted); err != nil {
					return err
				}
				propCountInsertedPresented = true
			default:
				return ErrorInvalidJSONExcessElement("statshouse.putTagMappingBootstrapResult", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propCountInsertedPresented {
		item.CountInserted = 0
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *StatshousePutTagMappingBootstrapResult) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *StatshousePutTagMappingBootstrapResult) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *StatshousePutTagMappingBootstrapResult) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexCountInserted := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"count_inserted":`...)
	w = basictl.JSONWriteInt32(w, item.CountInserted)
	if (item.CountInserted != 0) == false {
		w = w[:backupIndexCountInserted]
	}
	return append(w, '}')
}

func (item *StatshousePutTagMappingBootstrapResult) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *StatshousePutTagMappingBootstrapResult) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("statshouse.putTagMappingBootstrapResult", err.Error())
	}
	return nil
}

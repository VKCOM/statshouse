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

type StatshouseGetTagMappingBootstrap struct {
	FieldsMask uint32
	Header     StatshouseCommonProxyHeader
}

func (StatshouseGetTagMappingBootstrap) TLName() string { return "statshouse.getTagMappingBootstrap" }
func (StatshouseGetTagMappingBootstrap) TLTag() uint32  { return 0x75a7f68e }

func (item *StatshouseGetTagMappingBootstrap) Reset() {
	item.FieldsMask = 0
	item.Header.Reset()
}

func (item *StatshouseGetTagMappingBootstrap) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	return item.Header.Read(w, item.FieldsMask)
}

// This method is general version of Write, use it instead!
func (item *StatshouseGetTagMappingBootstrap) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *StatshouseGetTagMappingBootstrap) Write(w []byte) []byte {
	w = basictl.NatWrite(w, item.FieldsMask)
	w = item.Header.Write(w, item.FieldsMask)
	return w
}

func (item *StatshouseGetTagMappingBootstrap) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x75a7f68e); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *StatshouseGetTagMappingBootstrap) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *StatshouseGetTagMappingBootstrap) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x75a7f68e)
	return item.Write(w)
}

func (item *StatshouseGetTagMappingBootstrap) ReadResult(w []byte, ret *StatshouseGetTagMappingBootstrapResult) (_ []byte, err error) {
	return ret.ReadBoxed(w)
}

func (item *StatshouseGetTagMappingBootstrap) WriteResult(w []byte, ret StatshouseGetTagMappingBootstrapResult) (_ []byte, err error) {
	w = ret.WriteBoxed(w)
	return w, nil
}

func (item *StatshouseGetTagMappingBootstrap) ReadResultJSON(legacyTypeNames bool, in *basictl.JsonLexer, ret *StatshouseGetTagMappingBootstrapResult) error {
	if err := ret.ReadJSON(legacyTypeNames, in); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseGetTagMappingBootstrap) WriteResultJSON(w []byte, ret StatshouseGetTagMappingBootstrapResult) (_ []byte, err error) {
	return item.writeResultJSON(true, false, w, ret)
}

func (item *StatshouseGetTagMappingBootstrap) writeResultJSON(newTypeNames bool, short bool, w []byte, ret StatshouseGetTagMappingBootstrapResult) (_ []byte, err error) {
	w = ret.WriteJSONOpt(newTypeNames, short, w)
	return w, nil
}

func (item *StatshouseGetTagMappingBootstrap) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret StatshouseGetTagMappingBootstrapResult
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *StatshouseGetTagMappingBootstrap) ReadResultWriteResultJSONOpt(newTypeNames bool, short bool, r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret StatshouseGetTagMappingBootstrapResult
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.writeResultJSON(newTypeNames, short, w, ret)
	return r, w, err
}

func (item *StatshouseGetTagMappingBootstrap) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	var ret StatshouseGetTagMappingBootstrapResult
	err := item.ReadResultJSON(true, &basictl.JsonLexer{Data: r}, &ret)
	if err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item StatshouseGetTagMappingBootstrap) String() string {
	return string(item.WriteJSON(nil))
}

func (item *StatshouseGetTagMappingBootstrap) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propFieldsMaskPresented bool
	var rawHeader []byte

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
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.getTagMappingBootstrap", "fields_mask")
				}
				if err := Json2ReadUint32(in, &item.FieldsMask); err != nil {
					return err
				}
				propFieldsMaskPresented = true
			case "header":
				if rawHeader != nil {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.getTagMappingBootstrap", "header")
				}
				rawHeader = in.Raw()
				if !in.Ok() {
					return in.Error()
				}
			default:
				return ErrorInvalidJSONExcessElement("statshouse.getTagMappingBootstrap", key)
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
	var inHeaderPointer *basictl.JsonLexer
	inHeader := basictl.JsonLexer{Data: rawHeader}
	if rawHeader != nil {
		inHeaderPointer = &inHeader
	}
	if err := item.Header.ReadJSON(legacyTypeNames, inHeaderPointer, item.FieldsMask); err != nil {
		return err
	}

	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *StatshouseGetTagMappingBootstrap) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *StatshouseGetTagMappingBootstrap) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *StatshouseGetTagMappingBootstrap) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexFieldsMask := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"fields_mask":`...)
	w = basictl.JSONWriteUint32(w, item.FieldsMask)
	if (item.FieldsMask != 0) == false {
		w = w[:backupIndexFieldsMask]
	}
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"header":`...)
	w = item.Header.WriteJSONOpt(newTypeNames, short, w, item.FieldsMask)
	return append(w, '}')
}

func (item *StatshouseGetTagMappingBootstrap) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *StatshouseGetTagMappingBootstrap) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("statshouse.getTagMappingBootstrap", err.Error())
	}
	return nil
}

type StatshouseGetTagMappingBootstrapBytes struct {
	FieldsMask uint32
	Header     StatshouseCommonProxyHeaderBytes
}

func (StatshouseGetTagMappingBootstrapBytes) TLName() string {
	return "statshouse.getTagMappingBootstrap"
}
func (StatshouseGetTagMappingBootstrapBytes) TLTag() uint32 { return 0x75a7f68e }

func (item *StatshouseGetTagMappingBootstrapBytes) Reset() {
	item.FieldsMask = 0
	item.Header.Reset()
}

func (item *StatshouseGetTagMappingBootstrapBytes) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	return item.Header.Read(w, item.FieldsMask)
}

// This method is general version of Write, use it instead!
func (item *StatshouseGetTagMappingBootstrapBytes) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *StatshouseGetTagMappingBootstrapBytes) Write(w []byte) []byte {
	w = basictl.NatWrite(w, item.FieldsMask)
	w = item.Header.Write(w, item.FieldsMask)
	return w
}

func (item *StatshouseGetTagMappingBootstrapBytes) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x75a7f68e); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *StatshouseGetTagMappingBootstrapBytes) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *StatshouseGetTagMappingBootstrapBytes) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x75a7f68e)
	return item.Write(w)
}

func (item *StatshouseGetTagMappingBootstrapBytes) ReadResult(w []byte, ret *StatshouseGetTagMappingBootstrapResultBytes) (_ []byte, err error) {
	return ret.ReadBoxed(w)
}

func (item *StatshouseGetTagMappingBootstrapBytes) WriteResult(w []byte, ret StatshouseGetTagMappingBootstrapResultBytes) (_ []byte, err error) {
	w = ret.WriteBoxed(w)
	return w, nil
}

func (item *StatshouseGetTagMappingBootstrapBytes) ReadResultJSON(legacyTypeNames bool, in *basictl.JsonLexer, ret *StatshouseGetTagMappingBootstrapResultBytes) error {
	if err := ret.ReadJSON(legacyTypeNames, in); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseGetTagMappingBootstrapBytes) WriteResultJSON(w []byte, ret StatshouseGetTagMappingBootstrapResultBytes) (_ []byte, err error) {
	return item.writeResultJSON(true, false, w, ret)
}

func (item *StatshouseGetTagMappingBootstrapBytes) writeResultJSON(newTypeNames bool, short bool, w []byte, ret StatshouseGetTagMappingBootstrapResultBytes) (_ []byte, err error) {
	w = ret.WriteJSONOpt(newTypeNames, short, w)
	return w, nil
}

func (item *StatshouseGetTagMappingBootstrapBytes) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret StatshouseGetTagMappingBootstrapResultBytes
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *StatshouseGetTagMappingBootstrapBytes) ReadResultWriteResultJSONOpt(newTypeNames bool, short bool, r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret StatshouseGetTagMappingBootstrapResultBytes
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.writeResultJSON(newTypeNames, short, w, ret)
	return r, w, err
}

func (item *StatshouseGetTagMappingBootstrapBytes) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	var ret StatshouseGetTagMappingBootstrapResultBytes
	err := item.ReadResultJSON(true, &basictl.JsonLexer{Data: r}, &ret)
	if err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item StatshouseGetTagMappingBootstrapBytes) String() string {
	return string(item.WriteJSON(nil))
}

func (item *StatshouseGetTagMappingBootstrapBytes) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propFieldsMaskPresented bool
	var rawHeader []byte

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
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.getTagMappingBootstrap", "fields_mask")
				}
				if err := Json2ReadUint32(in, &item.FieldsMask); err != nil {
					return err
				}
				propFieldsMaskPresented = true
			case "header":
				if rawHeader != nil {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.getTagMappingBootstrap", "header")
				}
				rawHeader = in.Raw()
				if !in.Ok() {
					return in.Error()
				}
			default:
				return ErrorInvalidJSONExcessElement("statshouse.getTagMappingBootstrap", key)
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
	var inHeaderPointer *basictl.JsonLexer
	inHeader := basictl.JsonLexer{Data: rawHeader}
	if rawHeader != nil {
		inHeaderPointer = &inHeader
	}
	if err := item.Header.ReadJSON(legacyTypeNames, inHeaderPointer, item.FieldsMask); err != nil {
		return err
	}

	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *StatshouseGetTagMappingBootstrapBytes) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *StatshouseGetTagMappingBootstrapBytes) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *StatshouseGetTagMappingBootstrapBytes) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexFieldsMask := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"fields_mask":`...)
	w = basictl.JSONWriteUint32(w, item.FieldsMask)
	if (item.FieldsMask != 0) == false {
		w = w[:backupIndexFieldsMask]
	}
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"header":`...)
	w = item.Header.WriteJSONOpt(newTypeNames, short, w, item.FieldsMask)
	return append(w, '}')
}

func (item *StatshouseGetTagMappingBootstrapBytes) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *StatshouseGetTagMappingBootstrapBytes) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("statshouse.getTagMappingBootstrap", err.Error())
	}
	return nil
}

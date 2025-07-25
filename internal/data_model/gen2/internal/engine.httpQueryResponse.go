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

type EngineHttpQueryResponse struct {
	FieldsMask        uint32
	ReturnCode        int32             // Conditional: item.FieldsMask.0
	Data              string            // Conditional: item.FieldsMask.1
	ContentType       string            // Conditional: item.FieldsMask.2
	AdditionalHeaders map[string]string // Conditional: item.FieldsMask.3
}

func (EngineHttpQueryResponse) TLName() string { return "engine.httpQueryResponse" }
func (EngineHttpQueryResponse) TLTag() uint32  { return 0x284852fc }

func (item *EngineHttpQueryResponse) SetReturnCode(v int32) {
	item.ReturnCode = v
	item.FieldsMask |= 1 << 0
}
func (item *EngineHttpQueryResponse) ClearReturnCode() {
	item.ReturnCode = 0
	item.FieldsMask &^= 1 << 0
}
func (item EngineHttpQueryResponse) IsSetReturnCode() bool { return item.FieldsMask&(1<<0) != 0 }

func (item *EngineHttpQueryResponse) SetData(v string) {
	item.Data = v
	item.FieldsMask |= 1 << 1
}
func (item *EngineHttpQueryResponse) ClearData() {
	item.Data = ""
	item.FieldsMask &^= 1 << 1
}
func (item EngineHttpQueryResponse) IsSetData() bool { return item.FieldsMask&(1<<1) != 0 }

func (item *EngineHttpQueryResponse) SetContentType(v string) {
	item.ContentType = v
	item.FieldsMask |= 1 << 2
}
func (item *EngineHttpQueryResponse) ClearContentType() {
	item.ContentType = ""
	item.FieldsMask &^= 1 << 2
}
func (item EngineHttpQueryResponse) IsSetContentType() bool { return item.FieldsMask&(1<<2) != 0 }

func (item *EngineHttpQueryResponse) SetAdditionalHeaders(v map[string]string) {
	item.AdditionalHeaders = v
	item.FieldsMask |= 1 << 3
}
func (item *EngineHttpQueryResponse) ClearAdditionalHeaders() {
	BuiltinVectorDictionaryFieldStringReset(item.AdditionalHeaders)
	item.FieldsMask &^= 1 << 3
}
func (item EngineHttpQueryResponse) IsSetAdditionalHeaders() bool { return item.FieldsMask&(1<<3) != 0 }

func (item *EngineHttpQueryResponse) Reset() {
	item.FieldsMask = 0
	item.ReturnCode = 0
	item.Data = ""
	item.ContentType = ""
	BuiltinVectorDictionaryFieldStringReset(item.AdditionalHeaders)
}

func (item *EngineHttpQueryResponse) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	if item.FieldsMask&(1<<0) != 0 {
		if w, err = basictl.IntRead(w, &item.ReturnCode); err != nil {
			return w, err
		}
	} else {
		item.ReturnCode = 0
	}
	if item.FieldsMask&(1<<1) != 0 {
		if w, err = basictl.StringRead(w, &item.Data); err != nil {
			return w, err
		}
	} else {
		item.Data = ""
	}
	if item.FieldsMask&(1<<2) != 0 {
		if w, err = basictl.StringRead(w, &item.ContentType); err != nil {
			return w, err
		}
	} else {
		item.ContentType = ""
	}
	if item.FieldsMask&(1<<3) != 0 {
		if w, err = BuiltinVectorDictionaryFieldStringRead(w, &item.AdditionalHeaders); err != nil {
			return w, err
		}
	} else {
		BuiltinVectorDictionaryFieldStringReset(item.AdditionalHeaders)
	}
	return w, nil
}

// This method is general version of Write, use it instead!
func (item *EngineHttpQueryResponse) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *EngineHttpQueryResponse) Write(w []byte) []byte {
	w = basictl.NatWrite(w, item.FieldsMask)
	if item.FieldsMask&(1<<0) != 0 {
		w = basictl.IntWrite(w, item.ReturnCode)
	}
	if item.FieldsMask&(1<<1) != 0 {
		w = basictl.StringWrite(w, item.Data)
	}
	if item.FieldsMask&(1<<2) != 0 {
		w = basictl.StringWrite(w, item.ContentType)
	}
	if item.FieldsMask&(1<<3) != 0 {
		w = BuiltinVectorDictionaryFieldStringWrite(w, item.AdditionalHeaders)
	}
	return w
}

func (item *EngineHttpQueryResponse) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x284852fc); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *EngineHttpQueryResponse) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *EngineHttpQueryResponse) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x284852fc)
	return item.Write(w)
}

func (item EngineHttpQueryResponse) String() string {
	return string(item.WriteJSON(nil))
}

func (item *EngineHttpQueryResponse) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propFieldsMaskPresented bool
	var propReturnCodePresented bool
	var propDataPresented bool
	var propContentTypePresented bool
	var propAdditionalHeadersPresented bool

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
					return ErrorInvalidJSONWithDuplicatingKeys("engine.httpQueryResponse", "fields_mask")
				}
				if err := Json2ReadUint32(in, &item.FieldsMask); err != nil {
					return err
				}
				propFieldsMaskPresented = true
			case "return_code":
				if propReturnCodePresented {
					return ErrorInvalidJSONWithDuplicatingKeys("engine.httpQueryResponse", "return_code")
				}
				if err := Json2ReadInt32(in, &item.ReturnCode); err != nil {
					return err
				}
				propReturnCodePresented = true
			case "data":
				if propDataPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("engine.httpQueryResponse", "data")
				}
				if err := Json2ReadString(in, &item.Data); err != nil {
					return err
				}
				propDataPresented = true
			case "content_type":
				if propContentTypePresented {
					return ErrorInvalidJSONWithDuplicatingKeys("engine.httpQueryResponse", "content_type")
				}
				if err := Json2ReadString(in, &item.ContentType); err != nil {
					return err
				}
				propContentTypePresented = true
			case "additional_headers":
				if propAdditionalHeadersPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("engine.httpQueryResponse", "additional_headers")
				}
				if err := BuiltinVectorDictionaryFieldStringReadJSON(legacyTypeNames, in, &item.AdditionalHeaders); err != nil {
					return err
				}
				propAdditionalHeadersPresented = true
			default:
				return ErrorInvalidJSONExcessElement("engine.httpQueryResponse", key)
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
	if !propReturnCodePresented {
		item.ReturnCode = 0
	}
	if !propDataPresented {
		item.Data = ""
	}
	if !propContentTypePresented {
		item.ContentType = ""
	}
	if !propAdditionalHeadersPresented {
		BuiltinVectorDictionaryFieldStringReset(item.AdditionalHeaders)
	}
	if propReturnCodePresented {
		item.FieldsMask |= 1 << 0
	}
	if propDataPresented {
		item.FieldsMask |= 1 << 1
	}
	if propContentTypePresented {
		item.FieldsMask |= 1 << 2
	}
	if propAdditionalHeadersPresented {
		item.FieldsMask |= 1 << 3
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *EngineHttpQueryResponse) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *EngineHttpQueryResponse) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *EngineHttpQueryResponse) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexFieldsMask := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"fields_mask":`...)
	w = basictl.JSONWriteUint32(w, item.FieldsMask)
	if (item.FieldsMask != 0) == false {
		w = w[:backupIndexFieldsMask]
	}
	if item.FieldsMask&(1<<0) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"return_code":`...)
		w = basictl.JSONWriteInt32(w, item.ReturnCode)
	}
	if item.FieldsMask&(1<<1) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"data":`...)
		w = basictl.JSONWriteString(w, item.Data)
	}
	if item.FieldsMask&(1<<2) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"content_type":`...)
		w = basictl.JSONWriteString(w, item.ContentType)
	}
	if item.FieldsMask&(1<<3) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"additional_headers":`...)
		w = BuiltinVectorDictionaryFieldStringWriteJSONOpt(newTypeNames, short, w, item.AdditionalHeaders)
	}
	return append(w, '}')
}

func (item *EngineHttpQueryResponse) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *EngineHttpQueryResponse) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("engine.httpQueryResponse", err.Error())
	}
	return nil
}

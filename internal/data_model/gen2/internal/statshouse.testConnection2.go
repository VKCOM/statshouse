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

type StatshouseTestConnection2 struct {
	FieldsMask         uint32
	Header             StatshouseCommonProxyHeader
	Payload            string
	ResponseSize       int32
	ResponseTimeoutSec int32
}

func (StatshouseTestConnection2) TLName() string { return "statshouse.testConnection2" }
func (StatshouseTestConnection2) TLTag() uint32  { return 0x4285ff58 }

func (item *StatshouseTestConnection2) Reset() {
	item.FieldsMask = 0
	item.Header.Reset()
	item.Payload = ""
	item.ResponseSize = 0
	item.ResponseTimeoutSec = 0
}

func (item *StatshouseTestConnection2) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	if w, err = item.Header.Read(w, item.FieldsMask); err != nil {
		return w, err
	}
	if w, err = basictl.StringRead(w, &item.Payload); err != nil {
		return w, err
	}
	if w, err = basictl.IntRead(w, &item.ResponseSize); err != nil {
		return w, err
	}
	return basictl.IntRead(w, &item.ResponseTimeoutSec)
}

// This method is general version of Write, use it instead!
func (item *StatshouseTestConnection2) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *StatshouseTestConnection2) Write(w []byte) []byte {
	w = basictl.NatWrite(w, item.FieldsMask)
	w = item.Header.Write(w, item.FieldsMask)
	w = basictl.StringWrite(w, item.Payload)
	w = basictl.IntWrite(w, item.ResponseSize)
	w = basictl.IntWrite(w, item.ResponseTimeoutSec)
	return w
}

func (item *StatshouseTestConnection2) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x4285ff58); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *StatshouseTestConnection2) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *StatshouseTestConnection2) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x4285ff58)
	return item.Write(w)
}

func (item *StatshouseTestConnection2) ReadResult(w []byte, ret *string) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xb5286e24); err != nil {
		return w, err
	}
	return basictl.StringRead(w, ret)
}

func (item *StatshouseTestConnection2) WriteResult(w []byte, ret string) (_ []byte, err error) {
	w = basictl.NatWrite(w, 0xb5286e24)
	w = basictl.StringWrite(w, ret)
	return w, nil
}

func (item *StatshouseTestConnection2) ReadResultJSON(legacyTypeNames bool, in *basictl.JsonLexer, ret *string) error {
	if err := Json2ReadString(in, ret); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseTestConnection2) WriteResultJSON(w []byte, ret string) (_ []byte, err error) {
	return item.writeResultJSON(true, false, w, ret)
}

func (item *StatshouseTestConnection2) writeResultJSON(newTypeNames bool, short bool, w []byte, ret string) (_ []byte, err error) {
	w = basictl.JSONWriteString(w, ret)
	return w, nil
}

func (item *StatshouseTestConnection2) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret string
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *StatshouseTestConnection2) ReadResultWriteResultJSONOpt(newTypeNames bool, short bool, r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret string
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.writeResultJSON(newTypeNames, short, w, ret)
	return r, w, err
}

func (item *StatshouseTestConnection2) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	var ret string
	err := item.ReadResultJSON(true, &basictl.JsonLexer{Data: r}, &ret)
	if err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item StatshouseTestConnection2) String() string {
	return string(item.WriteJSON(nil))
}

func (item *StatshouseTestConnection2) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propFieldsMaskPresented bool
	var rawHeader []byte
	var propPayloadPresented bool
	var propResponseSizePresented bool
	var propResponseTimeoutSecPresented bool

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
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.testConnection2", "fields_mask")
				}
				if err := Json2ReadUint32(in, &item.FieldsMask); err != nil {
					return err
				}
				propFieldsMaskPresented = true
			case "header":
				if rawHeader != nil {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.testConnection2", "header")
				}
				rawHeader = in.Raw()
				if !in.Ok() {
					return in.Error()
				}
			case "payload":
				if propPayloadPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.testConnection2", "payload")
				}
				if err := Json2ReadString(in, &item.Payload); err != nil {
					return err
				}
				propPayloadPresented = true
			case "response_size":
				if propResponseSizePresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.testConnection2", "response_size")
				}
				if err := Json2ReadInt32(in, &item.ResponseSize); err != nil {
					return err
				}
				propResponseSizePresented = true
			case "response_timeout_sec":
				if propResponseTimeoutSecPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.testConnection2", "response_timeout_sec")
				}
				if err := Json2ReadInt32(in, &item.ResponseTimeoutSec); err != nil {
					return err
				}
				propResponseTimeoutSecPresented = true
			default:
				return ErrorInvalidJSONExcessElement("statshouse.testConnection2", key)
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
	if !propPayloadPresented {
		item.Payload = ""
	}
	if !propResponseSizePresented {
		item.ResponseSize = 0
	}
	if !propResponseTimeoutSecPresented {
		item.ResponseTimeoutSec = 0
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
func (item *StatshouseTestConnection2) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *StatshouseTestConnection2) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *StatshouseTestConnection2) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
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
	backupIndexPayload := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"payload":`...)
	w = basictl.JSONWriteString(w, item.Payload)
	if (len(item.Payload) != 0) == false {
		w = w[:backupIndexPayload]
	}
	backupIndexResponseSize := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"response_size":`...)
	w = basictl.JSONWriteInt32(w, item.ResponseSize)
	if (item.ResponseSize != 0) == false {
		w = w[:backupIndexResponseSize]
	}
	backupIndexResponseTimeoutSec := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"response_timeout_sec":`...)
	w = basictl.JSONWriteInt32(w, item.ResponseTimeoutSec)
	if (item.ResponseTimeoutSec != 0) == false {
		w = w[:backupIndexResponseTimeoutSec]
	}
	return append(w, '}')
}

func (item *StatshouseTestConnection2) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *StatshouseTestConnection2) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("statshouse.testConnection2", err.Error())
	}
	return nil
}

type StatshouseTestConnection2Bytes struct {
	FieldsMask         uint32
	Header             StatshouseCommonProxyHeaderBytes
	Payload            []byte
	ResponseSize       int32
	ResponseTimeoutSec int32
}

func (StatshouseTestConnection2Bytes) TLName() string { return "statshouse.testConnection2" }
func (StatshouseTestConnection2Bytes) TLTag() uint32  { return 0x4285ff58 }

func (item *StatshouseTestConnection2Bytes) Reset() {
	item.FieldsMask = 0
	item.Header.Reset()
	item.Payload = item.Payload[:0]
	item.ResponseSize = 0
	item.ResponseTimeoutSec = 0
}

func (item *StatshouseTestConnection2Bytes) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	if w, err = item.Header.Read(w, item.FieldsMask); err != nil {
		return w, err
	}
	if w, err = basictl.StringReadBytes(w, &item.Payload); err != nil {
		return w, err
	}
	if w, err = basictl.IntRead(w, &item.ResponseSize); err != nil {
		return w, err
	}
	return basictl.IntRead(w, &item.ResponseTimeoutSec)
}

// This method is general version of Write, use it instead!
func (item *StatshouseTestConnection2Bytes) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *StatshouseTestConnection2Bytes) Write(w []byte) []byte {
	w = basictl.NatWrite(w, item.FieldsMask)
	w = item.Header.Write(w, item.FieldsMask)
	w = basictl.StringWriteBytes(w, item.Payload)
	w = basictl.IntWrite(w, item.ResponseSize)
	w = basictl.IntWrite(w, item.ResponseTimeoutSec)
	return w
}

func (item *StatshouseTestConnection2Bytes) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x4285ff58); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *StatshouseTestConnection2Bytes) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *StatshouseTestConnection2Bytes) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x4285ff58)
	return item.Write(w)
}

func (item *StatshouseTestConnection2Bytes) ReadResult(w []byte, ret *[]byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xb5286e24); err != nil {
		return w, err
	}
	return basictl.StringReadBytes(w, ret)
}

func (item *StatshouseTestConnection2Bytes) WriteResult(w []byte, ret []byte) (_ []byte, err error) {
	w = basictl.NatWrite(w, 0xb5286e24)
	w = basictl.StringWriteBytes(w, ret)
	return w, nil
}

func (item *StatshouseTestConnection2Bytes) ReadResultJSON(legacyTypeNames bool, in *basictl.JsonLexer, ret *[]byte) error {
	if err := Json2ReadStringBytes(in, ret); err != nil {
		return err
	}
	return nil
}

func (item *StatshouseTestConnection2Bytes) WriteResultJSON(w []byte, ret []byte) (_ []byte, err error) {
	return item.writeResultJSON(true, false, w, ret)
}

func (item *StatshouseTestConnection2Bytes) writeResultJSON(newTypeNames bool, short bool, w []byte, ret []byte) (_ []byte, err error) {
	w = basictl.JSONWriteStringBytes(w, ret)
	return w, nil
}

func (item *StatshouseTestConnection2Bytes) ReadResultWriteResultJSON(r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret []byte
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.WriteResultJSON(w, ret)
	return r, w, err
}

func (item *StatshouseTestConnection2Bytes) ReadResultWriteResultJSONOpt(newTypeNames bool, short bool, r []byte, w []byte) (_ []byte, _ []byte, err error) {
	var ret []byte
	if r, err = item.ReadResult(r, &ret); err != nil {
		return r, w, err
	}
	w, err = item.writeResultJSON(newTypeNames, short, w, ret)
	return r, w, err
}

func (item *StatshouseTestConnection2Bytes) ReadResultJSONWriteResult(r []byte, w []byte) ([]byte, []byte, error) {
	var ret []byte
	err := item.ReadResultJSON(true, &basictl.JsonLexer{Data: r}, &ret)
	if err != nil {
		return r, w, err
	}
	w, err = item.WriteResult(w, ret)
	return r, w, err
}

func (item StatshouseTestConnection2Bytes) String() string {
	return string(item.WriteJSON(nil))
}

func (item *StatshouseTestConnection2Bytes) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propFieldsMaskPresented bool
	var rawHeader []byte
	var propPayloadPresented bool
	var propResponseSizePresented bool
	var propResponseTimeoutSecPresented bool

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
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.testConnection2", "fields_mask")
				}
				if err := Json2ReadUint32(in, &item.FieldsMask); err != nil {
					return err
				}
				propFieldsMaskPresented = true
			case "header":
				if rawHeader != nil {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.testConnection2", "header")
				}
				rawHeader = in.Raw()
				if !in.Ok() {
					return in.Error()
				}
			case "payload":
				if propPayloadPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.testConnection2", "payload")
				}
				if err := Json2ReadStringBytes(in, &item.Payload); err != nil {
					return err
				}
				propPayloadPresented = true
			case "response_size":
				if propResponseSizePresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.testConnection2", "response_size")
				}
				if err := Json2ReadInt32(in, &item.ResponseSize); err != nil {
					return err
				}
				propResponseSizePresented = true
			case "response_timeout_sec":
				if propResponseTimeoutSecPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.testConnection2", "response_timeout_sec")
				}
				if err := Json2ReadInt32(in, &item.ResponseTimeoutSec); err != nil {
					return err
				}
				propResponseTimeoutSecPresented = true
			default:
				return ErrorInvalidJSONExcessElement("statshouse.testConnection2", key)
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
	if !propPayloadPresented {
		item.Payload = item.Payload[:0]
	}
	if !propResponseSizePresented {
		item.ResponseSize = 0
	}
	if !propResponseTimeoutSecPresented {
		item.ResponseTimeoutSec = 0
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
func (item *StatshouseTestConnection2Bytes) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *StatshouseTestConnection2Bytes) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *StatshouseTestConnection2Bytes) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
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
	backupIndexPayload := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"payload":`...)
	w = basictl.JSONWriteStringBytes(w, item.Payload)
	if (len(item.Payload) != 0) == false {
		w = w[:backupIndexPayload]
	}
	backupIndexResponseSize := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"response_size":`...)
	w = basictl.JSONWriteInt32(w, item.ResponseSize)
	if (item.ResponseSize != 0) == false {
		w = w[:backupIndexResponseSize]
	}
	backupIndexResponseTimeoutSec := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"response_timeout_sec":`...)
	w = basictl.JSONWriteInt32(w, item.ResponseTimeoutSec)
	if (item.ResponseTimeoutSec != 0) == false {
		w = w[:backupIndexResponseTimeoutSec]
	}
	return append(w, '}')
}

func (item *StatshouseTestConnection2Bytes) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *StatshouseTestConnection2Bytes) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("statshouse.testConnection2", err.Error())
	}
	return nil
}

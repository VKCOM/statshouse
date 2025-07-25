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

func BuiltinVectorStatshouseTopElementRead(w []byte, vec *[]StatshouseTopElement) (_ []byte, err error) {
	var l uint32
	if w, err = basictl.NatRead(w, &l); err != nil {
		return w, err
	}
	if err = basictl.CheckLengthSanity(w, l, 4); err != nil {
		return w, err
	}
	if uint32(cap(*vec)) < l {
		*vec = make([]StatshouseTopElement, l)
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

func BuiltinVectorStatshouseTopElementWrite(w []byte, vec []StatshouseTopElement) []byte {
	w = basictl.NatWrite(w, uint32(len(vec)))
	for _, elem := range vec {
		w = elem.Write(w)
	}
	return w
}

func BuiltinVectorStatshouseTopElementReadJSON(legacyTypeNames bool, in *basictl.JsonLexer, vec *[]StatshouseTopElement) error {
	*vec = (*vec)[:cap(*vec)]
	index := 0
	if in != nil {
		in.Delim('[')
		if !in.Ok() {
			return ErrorInvalidJSON("[]StatshouseTopElement", "expected json array")
		}
		for ; !in.IsDelim(']'); index++ {
			if len(*vec) <= index {
				var newValue StatshouseTopElement
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
			return ErrorInvalidJSON("[]StatshouseTopElement", "expected json array's end")
		}
	}
	*vec = (*vec)[:index]
	return nil
}

func BuiltinVectorStatshouseTopElementWriteJSON(w []byte, vec []StatshouseTopElement) []byte {
	return BuiltinVectorStatshouseTopElementWriteJSONOpt(true, false, w, vec)
}
func BuiltinVectorStatshouseTopElementWriteJSONOpt(newTypeNames bool, short bool, w []byte, vec []StatshouseTopElement) []byte {
	w = append(w, '[')
	for _, elem := range vec {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = elem.WriteJSONOpt(newTypeNames, short, w)
	}
	return append(w, ']')
}

func BuiltinVectorStatshouseTopElementBytesRead(w []byte, vec *[]StatshouseTopElementBytes) (_ []byte, err error) {
	var l uint32
	if w, err = basictl.NatRead(w, &l); err != nil {
		return w, err
	}
	if err = basictl.CheckLengthSanity(w, l, 4); err != nil {
		return w, err
	}
	if uint32(cap(*vec)) < l {
		*vec = make([]StatshouseTopElementBytes, l)
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

func BuiltinVectorStatshouseTopElementBytesWrite(w []byte, vec []StatshouseTopElementBytes) []byte {
	w = basictl.NatWrite(w, uint32(len(vec)))
	for _, elem := range vec {
		w = elem.Write(w)
	}
	return w
}

func BuiltinVectorStatshouseTopElementBytesReadJSON(legacyTypeNames bool, in *basictl.JsonLexer, vec *[]StatshouseTopElementBytes) error {
	*vec = (*vec)[:cap(*vec)]
	index := 0
	if in != nil {
		in.Delim('[')
		if !in.Ok() {
			return ErrorInvalidJSON("[]StatshouseTopElementBytes", "expected json array")
		}
		for ; !in.IsDelim(']'); index++ {
			if len(*vec) <= index {
				var newValue StatshouseTopElementBytes
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
			return ErrorInvalidJSON("[]StatshouseTopElementBytes", "expected json array's end")
		}
	}
	*vec = (*vec)[:index]
	return nil
}

func BuiltinVectorStatshouseTopElementBytesWriteJSON(w []byte, vec []StatshouseTopElementBytes) []byte {
	return BuiltinVectorStatshouseTopElementBytesWriteJSONOpt(true, false, w, vec)
}
func BuiltinVectorStatshouseTopElementBytesWriteJSONOpt(newTypeNames bool, short bool, w []byte, vec []StatshouseTopElementBytes) []byte {
	w = append(w, '[')
	for _, elem := range vec {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = elem.WriteJSONOpt(newTypeNames, short, w)
	}
	return append(w, ']')
}

type StatshouseTopElement struct {
	Stag       string
	FieldsMask uint32
	Tag        int32 // Conditional: item.FieldsMask.10
	Value      StatshouseMultiValue
}

func (StatshouseTopElement) TLName() string { return "statshouse.top_element" }
func (StatshouseTopElement) TLTag() uint32  { return 0x9ffdea42 }

func (item *StatshouseTopElement) SetTag(v int32) {
	item.Tag = v
	item.FieldsMask |= 1 << 10
}
func (item *StatshouseTopElement) ClearTag() {
	item.Tag = 0
	item.FieldsMask &^= 1 << 10
}
func (item StatshouseTopElement) IsSetTag() bool { return item.FieldsMask&(1<<10) != 0 }

func (item *StatshouseTopElement) Reset() {
	item.Stag = ""
	item.FieldsMask = 0
	item.Tag = 0
	item.Value.Reset()
}

func (item *StatshouseTopElement) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.StringRead(w, &item.Stag); err != nil {
		return w, err
	}
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	if item.FieldsMask&(1<<10) != 0 {
		if w, err = basictl.IntRead(w, &item.Tag); err != nil {
			return w, err
		}
	} else {
		item.Tag = 0
	}
	return item.Value.Read(w, item.FieldsMask)
}

// This method is general version of Write, use it instead!
func (item *StatshouseTopElement) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *StatshouseTopElement) Write(w []byte) []byte {
	w = basictl.StringWrite(w, item.Stag)
	w = basictl.NatWrite(w, item.FieldsMask)
	if item.FieldsMask&(1<<10) != 0 {
		w = basictl.IntWrite(w, item.Tag)
	}
	w = item.Value.Write(w, item.FieldsMask)
	return w
}

func (item *StatshouseTopElement) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x9ffdea42); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *StatshouseTopElement) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *StatshouseTopElement) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x9ffdea42)
	return item.Write(w)
}

func (item StatshouseTopElement) String() string {
	return string(item.WriteJSON(nil))
}

func (item *StatshouseTopElement) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propStagPresented bool
	var propFieldsMaskPresented bool
	var propTagPresented bool
	var rawValue []byte

	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return in.Error()
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			switch key {
			case "stag":
				if propStagPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.top_element", "stag")
				}
				if err := Json2ReadString(in, &item.Stag); err != nil {
					return err
				}
				propStagPresented = true
			case "fields_mask":
				if propFieldsMaskPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.top_element", "fields_mask")
				}
				if err := Json2ReadUint32(in, &item.FieldsMask); err != nil {
					return err
				}
				propFieldsMaskPresented = true
			case "tag":
				if propTagPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.top_element", "tag")
				}
				if err := Json2ReadInt32(in, &item.Tag); err != nil {
					return err
				}
				propTagPresented = true
			case "value":
				if rawValue != nil {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.top_element", "value")
				}
				rawValue = in.Raw()
				if !in.Ok() {
					return in.Error()
				}
			default:
				return ErrorInvalidJSONExcessElement("statshouse.top_element", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propStagPresented {
		item.Stag = ""
	}
	if !propFieldsMaskPresented {
		item.FieldsMask = 0
	}
	if !propTagPresented {
		item.Tag = 0
	}
	if propTagPresented {
		item.FieldsMask |= 1 << 10
	}
	var inValuePointer *basictl.JsonLexer
	inValue := basictl.JsonLexer{Data: rawValue}
	if rawValue != nil {
		inValuePointer = &inValue
	}
	if err := item.Value.ReadJSON(legacyTypeNames, inValuePointer, item.FieldsMask); err != nil {
		return err
	}

	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *StatshouseTopElement) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *StatshouseTopElement) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *StatshouseTopElement) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexStag := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"stag":`...)
	w = basictl.JSONWriteString(w, item.Stag)
	if (len(item.Stag) != 0) == false {
		w = w[:backupIndexStag]
	}
	backupIndexFieldsMask := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"fields_mask":`...)
	w = basictl.JSONWriteUint32(w, item.FieldsMask)
	if (item.FieldsMask != 0) == false {
		w = w[:backupIndexFieldsMask]
	}
	if item.FieldsMask&(1<<10) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"tag":`...)
		w = basictl.JSONWriteInt32(w, item.Tag)
	}
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"value":`...)
	w = item.Value.WriteJSONOpt(newTypeNames, short, w, item.FieldsMask)
	return append(w, '}')
}

func (item *StatshouseTopElement) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *StatshouseTopElement) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("statshouse.top_element", err.Error())
	}
	return nil
}

type StatshouseTopElementBytes struct {
	Stag       []byte
	FieldsMask uint32
	Tag        int32 // Conditional: item.FieldsMask.10
	Value      StatshouseMultiValueBytes
}

func (StatshouseTopElementBytes) TLName() string { return "statshouse.top_element" }
func (StatshouseTopElementBytes) TLTag() uint32  { return 0x9ffdea42 }

func (item *StatshouseTopElementBytes) SetTag(v int32) {
	item.Tag = v
	item.FieldsMask |= 1 << 10
}
func (item *StatshouseTopElementBytes) ClearTag() {
	item.Tag = 0
	item.FieldsMask &^= 1 << 10
}
func (item StatshouseTopElementBytes) IsSetTag() bool { return item.FieldsMask&(1<<10) != 0 }

func (item *StatshouseTopElementBytes) Reset() {
	item.Stag = item.Stag[:0]
	item.FieldsMask = 0
	item.Tag = 0
	item.Value.Reset()
}

func (item *StatshouseTopElementBytes) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.StringReadBytes(w, &item.Stag); err != nil {
		return w, err
	}
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	if item.FieldsMask&(1<<10) != 0 {
		if w, err = basictl.IntRead(w, &item.Tag); err != nil {
			return w, err
		}
	} else {
		item.Tag = 0
	}
	return item.Value.Read(w, item.FieldsMask)
}

// This method is general version of Write, use it instead!
func (item *StatshouseTopElementBytes) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *StatshouseTopElementBytes) Write(w []byte) []byte {
	w = basictl.StringWriteBytes(w, item.Stag)
	w = basictl.NatWrite(w, item.FieldsMask)
	if item.FieldsMask&(1<<10) != 0 {
		w = basictl.IntWrite(w, item.Tag)
	}
	w = item.Value.Write(w, item.FieldsMask)
	return w
}

func (item *StatshouseTopElementBytes) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x9ffdea42); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *StatshouseTopElementBytes) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *StatshouseTopElementBytes) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x9ffdea42)
	return item.Write(w)
}

func (item StatshouseTopElementBytes) String() string {
	return string(item.WriteJSON(nil))
}

func (item *StatshouseTopElementBytes) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propStagPresented bool
	var propFieldsMaskPresented bool
	var propTagPresented bool
	var rawValue []byte

	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return in.Error()
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			switch key {
			case "stag":
				if propStagPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.top_element", "stag")
				}
				if err := Json2ReadStringBytes(in, &item.Stag); err != nil {
					return err
				}
				propStagPresented = true
			case "fields_mask":
				if propFieldsMaskPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.top_element", "fields_mask")
				}
				if err := Json2ReadUint32(in, &item.FieldsMask); err != nil {
					return err
				}
				propFieldsMaskPresented = true
			case "tag":
				if propTagPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.top_element", "tag")
				}
				if err := Json2ReadInt32(in, &item.Tag); err != nil {
					return err
				}
				propTagPresented = true
			case "value":
				if rawValue != nil {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.top_element", "value")
				}
				rawValue = in.Raw()
				if !in.Ok() {
					return in.Error()
				}
			default:
				return ErrorInvalidJSONExcessElement("statshouse.top_element", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propStagPresented {
		item.Stag = item.Stag[:0]
	}
	if !propFieldsMaskPresented {
		item.FieldsMask = 0
	}
	if !propTagPresented {
		item.Tag = 0
	}
	if propTagPresented {
		item.FieldsMask |= 1 << 10
	}
	var inValuePointer *basictl.JsonLexer
	inValue := basictl.JsonLexer{Data: rawValue}
	if rawValue != nil {
		inValuePointer = &inValue
	}
	if err := item.Value.ReadJSON(legacyTypeNames, inValuePointer, item.FieldsMask); err != nil {
		return err
	}

	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *StatshouseTopElementBytes) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *StatshouseTopElementBytes) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *StatshouseTopElementBytes) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexStag := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"stag":`...)
	w = basictl.JSONWriteStringBytes(w, item.Stag)
	if (len(item.Stag) != 0) == false {
		w = w[:backupIndexStag]
	}
	backupIndexFieldsMask := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"fields_mask":`...)
	w = basictl.JSONWriteUint32(w, item.FieldsMask)
	if (item.FieldsMask != 0) == false {
		w = w[:backupIndexFieldsMask]
	}
	if item.FieldsMask&(1<<10) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"tag":`...)
		w = basictl.JSONWriteInt32(w, item.Tag)
	}
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"value":`...)
	w = item.Value.WriteJSONOpt(newTypeNames, short, w, item.FieldsMask)
	return append(w, '}')
}

func (item *StatshouseTopElementBytes) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *StatshouseTopElementBytes) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("statshouse.top_element", err.Error())
	}
	return nil
}

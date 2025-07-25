// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Code generated by vktl/cmd/tlgen2; DO NOT EDIT.
package internal

import (
	"sort"

	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
)

var _ = basictl.NatWrite

func BuiltinVectorDictionaryFieldLongReset(m map[string]int64) {
	for k := range m {
		delete(m, k)
	}
}

func BuiltinVectorDictionaryFieldLongFillRandom(rg *basictl.RandGenerator, m *map[string]int64) {
	rg.IncreaseDepth()
	l := rg.LimitValue(basictl.RandomUint(rg))
	*m = make(map[string]int64, l)
	for i := 0; i < int(l); i++ {
		var elem DictionaryFieldLong
		elem.FillRandom(rg)
		(*m)[elem.Key] = elem.Value
	}
	rg.DecreaseDepth()
}
func BuiltinVectorDictionaryFieldLongRead(w []byte, m *map[string]int64) (_ []byte, err error) {
	var l uint32
	if w, err = basictl.NatRead(w, &l); err != nil {
		return w, err
	}
	if err = basictl.CheckLengthSanity(w, l, 4); err != nil {
		return w, err
	}
	var data map[string]int64
	if *m == nil {
		if l == 0 {
			return w, nil
		}
		data = make(map[string]int64, l)
		*m = data
	} else {
		data = *m
		for k := range data {
			delete(data, k)
		}
	}
	for i := 0; i < int(l); i++ {
		var elem DictionaryFieldLong
		if w, err = elem.Read(w); err != nil {
			return w, err
		}
		data[elem.Key] = elem.Value
	}
	return w, nil
}

func BuiltinVectorDictionaryFieldLongWrite(w []byte, m map[string]int64) []byte {
	w = basictl.NatWrite(w, uint32(len(m)))
	if len(m) == 0 {
		return w
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		val := m[key]
		elem := DictionaryFieldLong{Key: key, Value: val}
		w = elem.Write(w)
	}
	return w
}

func BuiltinVectorDictionaryFieldLongReadJSON(legacyTypeNames bool, in *basictl.JsonLexer, m *map[string]int64) error {
	var data map[string]int64
	if *m == nil {
		*m = make(map[string]int64, 0)
		data = *m
	} else {
		data = *m
		for k := range data {
			delete(data, k)
		}
	}
	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return ErrorInvalidJSON("map[string]int64", "expected json object")
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			var value int64
			if err := Json2ReadInt64(in, &value); err != nil {
				return err
			}
			data[key] = value
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return ErrorInvalidJSON("map[string]int64", "expected json object's end")
		}
	}
	return nil
}

func BuiltinVectorDictionaryFieldLongWriteJSON(w []byte, m map[string]int64) []byte {
	return BuiltinVectorDictionaryFieldLongWriteJSONOpt(true, false, w, m)
}
func BuiltinVectorDictionaryFieldLongWriteJSONOpt(newTypeNames bool, short bool, w []byte, m map[string]int64) []byte {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	w = append(w, '{')
	for _, key := range keys {
		value := m[key]
		w = basictl.JSONAddCommaIfNeeded(w)
		w = basictl.JSONWriteString(w, key)
		w = append(w, ':')
		w = basictl.JSONWriteInt64(w, value)
	}
	return append(w, '}')
}

func BuiltinVectorDictionaryFieldStringReset(m map[string]string) {
	for k := range m {
		delete(m, k)
	}
}

func BuiltinVectorDictionaryFieldStringFillRandom(rg *basictl.RandGenerator, m *map[string]string) {
	rg.IncreaseDepth()
	l := rg.LimitValue(basictl.RandomUint(rg))
	*m = make(map[string]string, l)
	for i := 0; i < int(l); i++ {
		var elem DictionaryFieldString
		elem.FillRandom(rg)
		(*m)[elem.Key] = elem.Value
	}
	rg.DecreaseDepth()
}
func BuiltinVectorDictionaryFieldStringRead(w []byte, m *map[string]string) (_ []byte, err error) {
	var l uint32
	if w, err = basictl.NatRead(w, &l); err != nil {
		return w, err
	}
	if err = basictl.CheckLengthSanity(w, l, 4); err != nil {
		return w, err
	}
	var data map[string]string
	if *m == nil {
		if l == 0 {
			return w, nil
		}
		data = make(map[string]string, l)
		*m = data
	} else {
		data = *m
		for k := range data {
			delete(data, k)
		}
	}
	for i := 0; i < int(l); i++ {
		var elem DictionaryFieldString
		if w, err = elem.Read(w); err != nil {
			return w, err
		}
		data[elem.Key] = elem.Value
	}
	return w, nil
}

func BuiltinVectorDictionaryFieldStringWrite(w []byte, m map[string]string) []byte {
	w = basictl.NatWrite(w, uint32(len(m)))
	if len(m) == 0 {
		return w
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		val := m[key]
		elem := DictionaryFieldString{Key: key, Value: val}
		w = elem.Write(w)
	}
	return w
}

func BuiltinVectorDictionaryFieldStringReadJSON(legacyTypeNames bool, in *basictl.JsonLexer, m *map[string]string) error {
	var data map[string]string
	if *m == nil {
		*m = make(map[string]string, 0)
		data = *m
	} else {
		data = *m
		for k := range data {
			delete(data, k)
		}
	}
	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return ErrorInvalidJSON("map[string]string", "expected json object")
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			var value string
			if err := Json2ReadString(in, &value); err != nil {
				return err
			}
			data[key] = value
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return ErrorInvalidJSON("map[string]string", "expected json object's end")
		}
	}
	return nil
}

func BuiltinVectorDictionaryFieldStringWriteJSON(w []byte, m map[string]string) []byte {
	return BuiltinVectorDictionaryFieldStringWriteJSONOpt(true, false, w, m)
}
func BuiltinVectorDictionaryFieldStringWriteJSONOpt(newTypeNames bool, short bool, w []byte, m map[string]string) []byte {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	w = append(w, '{')
	for _, key := range keys {
		value := m[key]
		w = basictl.JSONAddCommaIfNeeded(w)
		w = basictl.JSONWriteString(w, key)
		w = append(w, ':')
		w = basictl.JSONWriteString(w, value)
	}
	return append(w, '}')
}

type DictionaryFieldLong struct {
	Key   string
	Value int64
}

func (DictionaryFieldLong) TLName() string { return "dictionaryField" }
func (DictionaryFieldLong) TLTag() uint32  { return 0x239c1b62 }

func (item *DictionaryFieldLong) Reset() {
	item.Key = ""
	item.Value = 0
}

func (item *DictionaryFieldLong) FillRandom(rg *basictl.RandGenerator) {
	item.Key = basictl.RandomString(rg)
	item.Value = basictl.RandomLong(rg)
}

func (item *DictionaryFieldLong) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.StringRead(w, &item.Key); err != nil {
		return w, err
	}
	return basictl.LongRead(w, &item.Value)
}

func (item *DictionaryFieldLong) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *DictionaryFieldLong) Write(w []byte) []byte {
	w = basictl.StringWrite(w, item.Key)
	w = basictl.LongWrite(w, item.Value)
	return w
}

func (item *DictionaryFieldLong) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x239c1b62); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *DictionaryFieldLong) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *DictionaryFieldLong) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x239c1b62)
	return item.Write(w)
}

func (item DictionaryFieldLong) String() string {
	return string(item.WriteJSON(nil))
}

func (item *DictionaryFieldLong) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propKeyPresented bool
	var propValuePresented bool

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
					return ErrorInvalidJSONWithDuplicatingKeys("dictionaryField", "key")
				}
				if err := Json2ReadString(in, &item.Key); err != nil {
					return err
				}
				propKeyPresented = true
			case "value":
				if propValuePresented {
					return ErrorInvalidJSONWithDuplicatingKeys("dictionaryField", "value")
				}
				if err := Json2ReadInt64(in, &item.Value); err != nil {
					return err
				}
				propValuePresented = true
			default:
				return ErrorInvalidJSONExcessElement("dictionaryField", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propKeyPresented {
		item.Key = ""
	}
	if !propValuePresented {
		item.Value = 0
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *DictionaryFieldLong) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *DictionaryFieldLong) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *DictionaryFieldLong) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexKey := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"key":`...)
	w = basictl.JSONWriteString(w, item.Key)
	if (len(item.Key) != 0) == false {
		w = w[:backupIndexKey]
	}
	backupIndexValue := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"value":`...)
	w = basictl.JSONWriteInt64(w, item.Value)
	if (item.Value != 0) == false {
		w = w[:backupIndexValue]
	}
	return append(w, '}')
}

func (item *DictionaryFieldLong) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *DictionaryFieldLong) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("dictionaryField", err.Error())
	}
	return nil
}

type DictionaryFieldString struct {
	Key   string
	Value string
}

func (DictionaryFieldString) TLName() string { return "dictionaryField" }
func (DictionaryFieldString) TLTag() uint32  { return 0x239c1b62 }

func (item *DictionaryFieldString) Reset() {
	item.Key = ""
	item.Value = ""
}

func (item *DictionaryFieldString) FillRandom(rg *basictl.RandGenerator) {
	item.Key = basictl.RandomString(rg)
	item.Value = basictl.RandomString(rg)
}

func (item *DictionaryFieldString) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.StringRead(w, &item.Key); err != nil {
		return w, err
	}
	return basictl.StringRead(w, &item.Value)
}

func (item *DictionaryFieldString) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *DictionaryFieldString) Write(w []byte) []byte {
	w = basictl.StringWrite(w, item.Key)
	w = basictl.StringWrite(w, item.Value)
	return w
}

func (item *DictionaryFieldString) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x239c1b62); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *DictionaryFieldString) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *DictionaryFieldString) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x239c1b62)
	return item.Write(w)
}

func (item DictionaryFieldString) String() string {
	return string(item.WriteJSON(nil))
}

func (item *DictionaryFieldString) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propKeyPresented bool
	var propValuePresented bool

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
					return ErrorInvalidJSONWithDuplicatingKeys("dictionaryField", "key")
				}
				if err := Json2ReadString(in, &item.Key); err != nil {
					return err
				}
				propKeyPresented = true
			case "value":
				if propValuePresented {
					return ErrorInvalidJSONWithDuplicatingKeys("dictionaryField", "value")
				}
				if err := Json2ReadString(in, &item.Value); err != nil {
					return err
				}
				propValuePresented = true
			default:
				return ErrorInvalidJSONExcessElement("dictionaryField", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propKeyPresented {
		item.Key = ""
	}
	if !propValuePresented {
		item.Value = ""
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *DictionaryFieldString) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *DictionaryFieldString) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *DictionaryFieldString) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexKey := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"key":`...)
	w = basictl.JSONWriteString(w, item.Key)
	if (len(item.Key) != 0) == false {
		w = w[:backupIndexKey]
	}
	backupIndexValue := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"value":`...)
	w = basictl.JSONWriteString(w, item.Value)
	if (len(item.Value) != 0) == false {
		w = w[:backupIndexValue]
	}
	return append(w, '}')
}

func (item *DictionaryFieldString) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *DictionaryFieldString) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("dictionaryField", err.Error())
	}
	return nil
}

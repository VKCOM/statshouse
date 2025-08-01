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

func BuiltinVectorMetadataMetricOldRead(w []byte, vec *[]MetadataMetricOld, nat_t uint32) (_ []byte, err error) {
	var l uint32
	if w, err = basictl.NatRead(w, &l); err != nil {
		return w, err
	}
	if err = basictl.CheckLengthSanity(w, l, 4); err != nil {
		return w, err
	}
	if uint32(cap(*vec)) < l {
		*vec = make([]MetadataMetricOld, l)
	} else {
		*vec = (*vec)[:l]
	}
	for i := range *vec {
		if w, err = (*vec)[i].Read(w, nat_t); err != nil {
			return w, err
		}
	}
	return w, nil
}

func BuiltinVectorMetadataMetricOldWrite(w []byte, vec []MetadataMetricOld, nat_t uint32) []byte {
	w = basictl.NatWrite(w, uint32(len(vec)))
	for _, elem := range vec {
		w = elem.Write(w, nat_t)
	}
	return w
}

func BuiltinVectorMetadataMetricOldReadJSON(legacyTypeNames bool, in *basictl.JsonLexer, vec *[]MetadataMetricOld, nat_t uint32) error {
	*vec = (*vec)[:cap(*vec)]
	index := 0
	if in != nil {
		in.Delim('[')
		if !in.Ok() {
			return ErrorInvalidJSON("[]MetadataMetricOld", "expected json array")
		}
		for ; !in.IsDelim(']'); index++ {
			if len(*vec) <= index {
				var newValue MetadataMetricOld
				*vec = append(*vec, newValue)
				*vec = (*vec)[:cap(*vec)]
			}
			if err := (*vec)[index].ReadJSON(legacyTypeNames, in, nat_t); err != nil {
				return err
			}
			in.WantComma()
		}
		in.Delim(']')
		if !in.Ok() {
			return ErrorInvalidJSON("[]MetadataMetricOld", "expected json array's end")
		}
	}
	*vec = (*vec)[:index]
	return nil
}

func BuiltinVectorMetadataMetricOldWriteJSON(w []byte, vec []MetadataMetricOld, nat_t uint32) []byte {
	return BuiltinVectorMetadataMetricOldWriteJSONOpt(true, false, w, vec, nat_t)
}
func BuiltinVectorMetadataMetricOldWriteJSONOpt(newTypeNames bool, short bool, w []byte, vec []MetadataMetricOld, nat_t uint32) []byte {
	w = append(w, '[')
	for _, elem := range vec {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = elem.WriteJSONOpt(newTypeNames, short, w, nat_t)
	}
	return append(w, ']')
}

type MetadataMetricOld struct {
	Id         int64
	Name       string
	EventType  int32  // Conditional: nat_field_mask.2
	Unused     uint32 // Conditional: nat_field_mask.3
	Version    int64
	UpdateTime uint32
	Data       string
}

func (MetadataMetricOld) TLName() string { return "metadata.metricOld" }
func (MetadataMetricOld) TLTag() uint32  { return 0x9286abfa }

func (item *MetadataMetricOld) SetEventType(v int32, nat_field_mask *uint32) {
	item.EventType = v
	if nat_field_mask != nil {
		*nat_field_mask |= 1 << 2
	}
}
func (item *MetadataMetricOld) ClearEventType(nat_field_mask *uint32) {
	item.EventType = 0
	if nat_field_mask != nil {
		*nat_field_mask &^= 1 << 2
	}
}
func (item MetadataMetricOld) IsSetEventType(nat_field_mask uint32) bool {
	return nat_field_mask&(1<<2) != 0
}

func (item *MetadataMetricOld) SetUnused(v uint32, nat_field_mask *uint32) {
	item.Unused = v
	if nat_field_mask != nil {
		*nat_field_mask |= 1 << 3
	}
}
func (item *MetadataMetricOld) ClearUnused(nat_field_mask *uint32) {
	item.Unused = 0
	if nat_field_mask != nil {
		*nat_field_mask &^= 1 << 3
	}
}
func (item MetadataMetricOld) IsSetUnused(nat_field_mask uint32) bool {
	return nat_field_mask&(1<<3) != 0
}

func (item *MetadataMetricOld) Reset() {
	item.Id = 0
	item.Name = ""
	item.EventType = 0
	item.Unused = 0
	item.Version = 0
	item.UpdateTime = 0
	item.Data = ""
}

func (item *MetadataMetricOld) Read(w []byte, nat_field_mask uint32) (_ []byte, err error) {
	if w, err = basictl.LongRead(w, &item.Id); err != nil {
		return w, err
	}
	if w, err = basictl.StringRead(w, &item.Name); err != nil {
		return w, err
	}
	if nat_field_mask&(1<<2) != 0 {
		if w, err = basictl.IntRead(w, &item.EventType); err != nil {
			return w, err
		}
	} else {
		item.EventType = 0
	}
	if nat_field_mask&(1<<3) != 0 {
		if w, err = basictl.NatRead(w, &item.Unused); err != nil {
			return w, err
		}
	} else {
		item.Unused = 0
	}
	if w, err = basictl.LongRead(w, &item.Version); err != nil {
		return w, err
	}
	if w, err = basictl.NatRead(w, &item.UpdateTime); err != nil {
		return w, err
	}
	return basictl.StringRead(w, &item.Data)
}

// This method is general version of Write, use it instead!
func (item *MetadataMetricOld) WriteGeneral(w []byte, nat_field_mask uint32) (_ []byte, err error) {
	return item.Write(w, nat_field_mask), nil
}

func (item *MetadataMetricOld) Write(w []byte, nat_field_mask uint32) []byte {
	w = basictl.LongWrite(w, item.Id)
	w = basictl.StringWrite(w, item.Name)
	if nat_field_mask&(1<<2) != 0 {
		w = basictl.IntWrite(w, item.EventType)
	}
	if nat_field_mask&(1<<3) != 0 {
		w = basictl.NatWrite(w, item.Unused)
	}
	w = basictl.LongWrite(w, item.Version)
	w = basictl.NatWrite(w, item.UpdateTime)
	w = basictl.StringWrite(w, item.Data)
	return w
}

func (item *MetadataMetricOld) ReadBoxed(w []byte, nat_field_mask uint32) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x9286abfa); err != nil {
		return w, err
	}
	return item.Read(w, nat_field_mask)
}

// This method is general version of WriteBoxed, use it instead!
func (item *MetadataMetricOld) WriteBoxedGeneral(w []byte, nat_field_mask uint32) (_ []byte, err error) {
	return item.WriteBoxed(w, nat_field_mask), nil
}

func (item *MetadataMetricOld) WriteBoxed(w []byte, nat_field_mask uint32) []byte {
	w = basictl.NatWrite(w, 0x9286abfa)
	return item.Write(w, nat_field_mask)
}

func (item *MetadataMetricOld) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer, nat_field_mask uint32) error {
	var propIdPresented bool
	var propNamePresented bool
	var propEventTypePresented bool
	var propUnusedPresented bool
	var propVersionPresented bool
	var propUpdateTimePresented bool
	var propDataPresented bool

	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return in.Error()
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			switch key {
			case "id":
				if propIdPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("metadata.metricOld", "id")
				}
				if err := Json2ReadInt64(in, &item.Id); err != nil {
					return err
				}
				propIdPresented = true
			case "name":
				if propNamePresented {
					return ErrorInvalidJSONWithDuplicatingKeys("metadata.metricOld", "name")
				}
				if err := Json2ReadString(in, &item.Name); err != nil {
					return err
				}
				propNamePresented = true
			case "event_type":
				if propEventTypePresented {
					return ErrorInvalidJSONWithDuplicatingKeys("metadata.metricOld", "event_type")
				}
				if nat_field_mask&(1<<2) == 0 {
					return ErrorInvalidJSON("metadata.metricOld", "field 'event_type' is defined, while corresponding implicit fieldmask bit is 0")
				}
				if err := Json2ReadInt32(in, &item.EventType); err != nil {
					return err
				}
				propEventTypePresented = true
			case "unused":
				if propUnusedPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("metadata.metricOld", "unused")
				}
				if nat_field_mask&(1<<3) == 0 {
					return ErrorInvalidJSON("metadata.metricOld", "field 'unused' is defined, while corresponding implicit fieldmask bit is 0")
				}
				if err := Json2ReadUint32(in, &item.Unused); err != nil {
					return err
				}
				propUnusedPresented = true
			case "version":
				if propVersionPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("metadata.metricOld", "version")
				}
				if err := Json2ReadInt64(in, &item.Version); err != nil {
					return err
				}
				propVersionPresented = true
			case "update_time":
				if propUpdateTimePresented {
					return ErrorInvalidJSONWithDuplicatingKeys("metadata.metricOld", "update_time")
				}
				if err := Json2ReadUint32(in, &item.UpdateTime); err != nil {
					return err
				}
				propUpdateTimePresented = true
			case "data":
				if propDataPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("metadata.metricOld", "data")
				}
				if err := Json2ReadString(in, &item.Data); err != nil {
					return err
				}
				propDataPresented = true
			default:
				return ErrorInvalidJSONExcessElement("metadata.metricOld", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propIdPresented {
		item.Id = 0
	}
	if !propNamePresented {
		item.Name = ""
	}
	if !propEventTypePresented {
		item.EventType = 0
	}
	if !propUnusedPresented {
		item.Unused = 0
	}
	if !propVersionPresented {
		item.Version = 0
	}
	if !propUpdateTimePresented {
		item.UpdateTime = 0
	}
	if !propDataPresented {
		item.Data = ""
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *MetadataMetricOld) WriteJSONGeneral(w []byte, nat_field_mask uint32) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w, nat_field_mask), nil
}

func (item *MetadataMetricOld) WriteJSON(w []byte, nat_field_mask uint32) []byte {
	return item.WriteJSONOpt(true, false, w, nat_field_mask)
}
func (item *MetadataMetricOld) WriteJSONOpt(newTypeNames bool, short bool, w []byte, nat_field_mask uint32) []byte {
	w = append(w, '{')
	backupIndexId := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"id":`...)
	w = basictl.JSONWriteInt64(w, item.Id)
	if (item.Id != 0) == false {
		w = w[:backupIndexId]
	}
	backupIndexName := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"name":`...)
	w = basictl.JSONWriteString(w, item.Name)
	if (len(item.Name) != 0) == false {
		w = w[:backupIndexName]
	}
	if nat_field_mask&(1<<2) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"event_type":`...)
		w = basictl.JSONWriteInt32(w, item.EventType)
	}
	if nat_field_mask&(1<<3) != 0 {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = append(w, `"unused":`...)
		w = basictl.JSONWriteUint32(w, item.Unused)
	}
	backupIndexVersion := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"version":`...)
	w = basictl.JSONWriteInt64(w, item.Version)
	if (item.Version != 0) == false {
		w = w[:backupIndexVersion]
	}
	backupIndexUpdateTime := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"update_time":`...)
	w = basictl.JSONWriteUint32(w, item.UpdateTime)
	if (item.UpdateTime != 0) == false {
		w = w[:backupIndexUpdateTime]
	}
	backupIndexData := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"data":`...)
	w = basictl.JSONWriteString(w, item.Data)
	if (len(item.Data) != 0) == false {
		w = w[:backupIndexData]
	}
	return append(w, '}')
}

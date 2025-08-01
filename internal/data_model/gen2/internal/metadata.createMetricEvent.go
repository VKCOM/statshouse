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

type MetadataCreateMetricEvent struct {
	FieldsMask uint32
	Metric     MetadataMetricOld
}

func (MetadataCreateMetricEvent) TLName() string { return "metadata.createMetricEvent" }
func (MetadataCreateMetricEvent) TLTag() uint32  { return 0x12345674 }

func (item *MetadataCreateMetricEvent) Reset() {
	item.FieldsMask = 0
	item.Metric.Reset()
}

func (item *MetadataCreateMetricEvent) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatRead(w, &item.FieldsMask); err != nil {
		return w, err
	}
	return item.Metric.Read(w, item.FieldsMask)
}

// This method is general version of Write, use it instead!
func (item *MetadataCreateMetricEvent) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *MetadataCreateMetricEvent) Write(w []byte) []byte {
	w = basictl.NatWrite(w, item.FieldsMask)
	w = item.Metric.Write(w, item.FieldsMask)
	return w
}

func (item *MetadataCreateMetricEvent) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x12345674); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *MetadataCreateMetricEvent) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *MetadataCreateMetricEvent) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x12345674)
	return item.Write(w)
}

func (item MetadataCreateMetricEvent) String() string {
	return string(item.WriteJSON(nil))
}

func (item *MetadataCreateMetricEvent) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propFieldsMaskPresented bool
	var rawMetric []byte

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
					return ErrorInvalidJSONWithDuplicatingKeys("metadata.createMetricEvent", "fields_mask")
				}
				if err := Json2ReadUint32(in, &item.FieldsMask); err != nil {
					return err
				}
				propFieldsMaskPresented = true
			case "metric":
				if rawMetric != nil {
					return ErrorInvalidJSONWithDuplicatingKeys("metadata.createMetricEvent", "metric")
				}
				rawMetric = in.Raw()
				if !in.Ok() {
					return in.Error()
				}
			default:
				return ErrorInvalidJSONExcessElement("metadata.createMetricEvent", key)
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
	var inMetricPointer *basictl.JsonLexer
	inMetric := basictl.JsonLexer{Data: rawMetric}
	if rawMetric != nil {
		inMetricPointer = &inMetric
	}
	if err := item.Metric.ReadJSON(legacyTypeNames, inMetricPointer, item.FieldsMask); err != nil {
		return err
	}

	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *MetadataCreateMetricEvent) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *MetadataCreateMetricEvent) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *MetadataCreateMetricEvent) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexFieldsMask := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"fields_mask":`...)
	w = basictl.JSONWriteUint32(w, item.FieldsMask)
	if (item.FieldsMask != 0) == false {
		w = w[:backupIndexFieldsMask]
	}
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"metric":`...)
	w = item.Metric.WriteJSONOpt(newTypeNames, short, w, item.FieldsMask)
	return append(w, '}')
}

func (item *MetadataCreateMetricEvent) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *MetadataCreateMetricEvent) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("metadata.createMetricEvent", err.Error())
	}
	return nil
}

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

func BuiltinVectorStatshouseSampleFactorRead(w []byte, vec *[]StatshouseSampleFactor) (_ []byte, err error) {
	var l uint32
	if w, err = basictl.NatRead(w, &l); err != nil {
		return w, err
	}
	if err = basictl.CheckLengthSanity(w, l, 4); err != nil {
		return w, err
	}
	if uint32(cap(*vec)) < l {
		*vec = make([]StatshouseSampleFactor, l)
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

func BuiltinVectorStatshouseSampleFactorWrite(w []byte, vec []StatshouseSampleFactor) []byte {
	w = basictl.NatWrite(w, uint32(len(vec)))
	for _, elem := range vec {
		w = elem.Write(w)
	}
	return w
}

func BuiltinVectorStatshouseSampleFactorReadJSON(legacyTypeNames bool, in *basictl.JsonLexer, vec *[]StatshouseSampleFactor) error {
	*vec = (*vec)[:cap(*vec)]
	index := 0
	if in != nil {
		in.Delim('[')
		if !in.Ok() {
			return ErrorInvalidJSON("[]StatshouseSampleFactor", "expected json array")
		}
		for ; !in.IsDelim(']'); index++ {
			if len(*vec) <= index {
				var newValue StatshouseSampleFactor
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
			return ErrorInvalidJSON("[]StatshouseSampleFactor", "expected json array's end")
		}
	}
	*vec = (*vec)[:index]
	return nil
}

func BuiltinVectorStatshouseSampleFactorWriteJSON(w []byte, vec []StatshouseSampleFactor) []byte {
	return BuiltinVectorStatshouseSampleFactorWriteJSONOpt(true, false, w, vec)
}
func BuiltinVectorStatshouseSampleFactorWriteJSONOpt(newTypeNames bool, short bool, w []byte, vec []StatshouseSampleFactor) []byte {
	w = append(w, '[')
	for _, elem := range vec {
		w = basictl.JSONAddCommaIfNeeded(w)
		w = elem.WriteJSONOpt(newTypeNames, short, w)
	}
	return append(w, ']')
}

type StatshouseSampleFactor struct {
	Metric int32
	Value  float32
}

func (StatshouseSampleFactor) TLName() string { return "statshouse.sample_factor" }
func (StatshouseSampleFactor) TLTag() uint32  { return 0x4f7b7822 }

func (item *StatshouseSampleFactor) Reset() {
	item.Metric = 0
	item.Value = 0
}

func (item *StatshouseSampleFactor) Read(w []byte) (_ []byte, err error) {
	if w, err = basictl.IntRead(w, &item.Metric); err != nil {
		return w, err
	}
	return basictl.FloatRead(w, &item.Value)
}

// This method is general version of Write, use it instead!
func (item *StatshouseSampleFactor) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *StatshouseSampleFactor) Write(w []byte) []byte {
	w = basictl.IntWrite(w, item.Metric)
	w = basictl.FloatWrite(w, item.Value)
	return w
}

func (item *StatshouseSampleFactor) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x4f7b7822); err != nil {
		return w, err
	}
	return item.Read(w)
}

// This method is general version of WriteBoxed, use it instead!
func (item *StatshouseSampleFactor) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *StatshouseSampleFactor) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x4f7b7822)
	return item.Write(w)
}

func (item StatshouseSampleFactor) String() string {
	return string(item.WriteJSON(nil))
}

func (item *StatshouseSampleFactor) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propMetricPresented bool
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
			case "metric":
				if propMetricPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.sample_factor", "metric")
				}
				if err := Json2ReadInt32(in, &item.Metric); err != nil {
					return err
				}
				propMetricPresented = true
			case "value":
				if propValuePresented {
					return ErrorInvalidJSONWithDuplicatingKeys("statshouse.sample_factor", "value")
				}
				if err := Json2ReadFloat32(in, &item.Value); err != nil {
					return err
				}
				propValuePresented = true
			default:
				return ErrorInvalidJSONExcessElement("statshouse.sample_factor", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propMetricPresented {
		item.Metric = 0
	}
	if !propValuePresented {
		item.Value = 0
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *StatshouseSampleFactor) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *StatshouseSampleFactor) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *StatshouseSampleFactor) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexMetric := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"metric":`...)
	w = basictl.JSONWriteInt32(w, item.Metric)
	if (item.Metric != 0) == false {
		w = w[:backupIndexMetric]
	}
	backupIndexValue := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"value":`...)
	w = basictl.JSONWriteFloat32(w, item.Value)
	if (item.Value != 0) == false {
		w = w[:backupIndexValue]
	}
	return append(w, '}')
}

func (item *StatshouseSampleFactor) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *StatshouseSampleFactor) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("statshouse.sample_factor", err.Error())
	}
	return nil
}

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

type NetUdpPacketObsoleteGeneration struct {
	Pid        NetPid
	Generation uint32
}

func (NetUdpPacketObsoleteGeneration) TLName() string { return "netUdpPacket.obsoleteGeneration" }
func (NetUdpPacketObsoleteGeneration) TLTag() uint32  { return 0xb340010b }

func (item *NetUdpPacketObsoleteGeneration) Reset() {
	item.Pid.Reset()
	item.Generation = 0
}

func (item *NetUdpPacketObsoleteGeneration) FillRandom(rg *basictl.RandGenerator) {
	item.Pid.FillRandom(rg)
	item.Generation = basictl.RandomUint(rg)
}

func (item *NetUdpPacketObsoleteGeneration) Read(w []byte) (_ []byte, err error) {
	if w, err = item.Pid.ReadBoxed(w); err != nil {
		return w, err
	}
	return basictl.NatRead(w, &item.Generation)
}

func (item *NetUdpPacketObsoleteGeneration) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *NetUdpPacketObsoleteGeneration) Write(w []byte) []byte {
	w = item.Pid.WriteBoxed(w)
	w = basictl.NatWrite(w, item.Generation)
	return w
}

func (item *NetUdpPacketObsoleteGeneration) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0xb340010b); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *NetUdpPacketObsoleteGeneration) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *NetUdpPacketObsoleteGeneration) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0xb340010b)
	return item.Write(w)
}

func (item NetUdpPacketObsoleteGeneration) String() string {
	return string(item.WriteJSON(nil))
}

func (item *NetUdpPacketObsoleteGeneration) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propPidPresented bool
	var propGenerationPresented bool

	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return in.Error()
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			switch key {
			case "pid":
				if propPidPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("netUdpPacket.obsoleteGeneration", "pid")
				}
				if err := item.Pid.ReadJSON(legacyTypeNames, in); err != nil {
					return err
				}
				propPidPresented = true
			case "generation":
				if propGenerationPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("netUdpPacket.obsoleteGeneration", "generation")
				}
				if err := Json2ReadUint32(in, &item.Generation); err != nil {
					return err
				}
				propGenerationPresented = true
			default:
				return ErrorInvalidJSONExcessElement("netUdpPacket.obsoleteGeneration", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propPidPresented {
		item.Pid.Reset()
	}
	if !propGenerationPresented {
		item.Generation = 0
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *NetUdpPacketObsoleteGeneration) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *NetUdpPacketObsoleteGeneration) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *NetUdpPacketObsoleteGeneration) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"pid":`...)
	w = item.Pid.WriteJSONOpt(newTypeNames, short, w)
	backupIndexGeneration := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"generation":`...)
	w = basictl.JSONWriteUint32(w, item.Generation)
	if (item.Generation != 0) == false {
		w = w[:backupIndexGeneration]
	}
	return append(w, '}')
}

func (item *NetUdpPacketObsoleteGeneration) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *NetUdpPacketObsoleteGeneration) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("netUdpPacket.obsoleteGeneration", err.Error())
	}
	return nil
}

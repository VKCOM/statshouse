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

type RpcPong struct {
	PingId int64
}

func (RpcPong) TLName() string { return "rpcPong" }
func (RpcPong) TLTag() uint32  { return 0x8430eaa7 }

func (item *RpcPong) Reset() {
	item.PingId = 0
}

func (item *RpcPong) FillRandom(rg *basictl.RandGenerator) {
	item.PingId = basictl.RandomLong(rg)
}

func (item *RpcPong) Read(w []byte) (_ []byte, err error) {
	return basictl.LongRead(w, &item.PingId)
}

func (item *RpcPong) WriteGeneral(w []byte) (_ []byte, err error) {
	return item.Write(w), nil
}

func (item *RpcPong) Write(w []byte) []byte {
	w = basictl.LongWrite(w, item.PingId)
	return w
}

func (item *RpcPong) ReadBoxed(w []byte) (_ []byte, err error) {
	if w, err = basictl.NatReadExactTag(w, 0x8430eaa7); err != nil {
		return w, err
	}
	return item.Read(w)
}

func (item *RpcPong) WriteBoxedGeneral(w []byte) (_ []byte, err error) {
	return item.WriteBoxed(w), nil
}

func (item *RpcPong) WriteBoxed(w []byte) []byte {
	w = basictl.NatWrite(w, 0x8430eaa7)
	return item.Write(w)
}

func (item RpcPong) String() string {
	return string(item.WriteJSON(nil))
}

func (item *RpcPong) ReadJSON(legacyTypeNames bool, in *basictl.JsonLexer) error {
	var propPingIdPresented bool

	if in != nil {
		in.Delim('{')
		if !in.Ok() {
			return in.Error()
		}
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(true)
			in.WantColon()
			switch key {
			case "ping_id":
				if propPingIdPresented {
					return ErrorInvalidJSONWithDuplicatingKeys("rpcPong", "ping_id")
				}
				if err := Json2ReadInt64(in, &item.PingId); err != nil {
					return err
				}
				propPingIdPresented = true
			default:
				return ErrorInvalidJSONExcessElement("rpcPong", key)
			}
			in.WantComma()
		}
		in.Delim('}')
		if !in.Ok() {
			return in.Error()
		}
	}
	if !propPingIdPresented {
		item.PingId = 0
	}
	return nil
}

// This method is general version of WriteJSON, use it instead!
func (item *RpcPong) WriteJSONGeneral(w []byte) (_ []byte, err error) {
	return item.WriteJSONOpt(true, false, w), nil
}

func (item *RpcPong) WriteJSON(w []byte) []byte {
	return item.WriteJSONOpt(true, false, w)
}
func (item *RpcPong) WriteJSONOpt(newTypeNames bool, short bool, w []byte) []byte {
	w = append(w, '{')
	backupIndexPingId := len(w)
	w = basictl.JSONAddCommaIfNeeded(w)
	w = append(w, `"ping_id":`...)
	w = basictl.JSONWriteInt64(w, item.PingId)
	if (item.PingId != 0) == false {
		w = w[:backupIndexPingId]
	}
	return append(w, '}')
}

func (item *RpcPong) MarshalJSON() ([]byte, error) {
	return item.WriteJSON(nil), nil
}

func (item *RpcPong) UnmarshalJSON(b []byte) error {
	if err := item.ReadJSON(true, &basictl.JsonLexer{Data: b}); err != nil {
		return ErrorInvalidJSON("rpcPong", err.Error())
	}
	return nil
}

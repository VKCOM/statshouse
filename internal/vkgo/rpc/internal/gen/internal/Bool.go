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

const (
	BoolFalse uint32 = 0xbc799737
	BoolTrue  uint32 = 0x997275b5
)

func BoolReadBoxed(w []byte, v *bool) ([]byte, error) {
	return basictl.ReadBool(w, v, BoolFalse, BoolTrue)
}

func BoolWriteBoxed(w []byte, v bool) []byte {
	if v {
		return basictl.NatWrite(w, 0x997275b5)
	}
	return basictl.NatWrite(w, 0xbc799737)
}

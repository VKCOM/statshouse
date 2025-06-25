// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package mem

import (
	"io"
	"unsafe"

	"github.com/dchest/siphash"
)

// WriteString writes string to writer without an allocation.
func WriteString(w io.Writer, s string) (int, error) {
	return w.Write(toByteSlice(s))
}

// SipHash24 computes SipHash-2-4 of string without an allocation.
func SipHash24(k0 uint64, k1 uint64, s string) uint64 {
	return siphash.Hash(k0, k1, toByteSlice(s))
}

func toByteSlice(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

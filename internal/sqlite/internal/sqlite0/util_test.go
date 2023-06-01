// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite0

import "testing"

func TestUnsafePtrNonNil(t *testing.T) {
	p1 := unsafeSlicePtr(nil)
	if p1 == nil {
		t.Fatalf("got nil from unsafeSlicePtr")
	}
	p2 := unsafeStringPtr("")
	if p2 == nil {
		t.Fatalf("got nil from unsafeStringPtr")
	}
	p3 := unsafeSliceCPtr(nil)
	if p3 == nil {
		t.Fatalf("got nil from unsafeSliceCPtr")
	}
	p4 := unsafeStringCPtr("")
	if p4 == nil {
		t.Fatalf("got nil from unsafeStringCPtr")
	}
}

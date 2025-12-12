// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite0

import "testing"

func TestAppendNil(t *testing.T) {
	var nilBytes []byte
	s1 := append([]byte(nil), nilBytes...)
	if s1 != nil {
		t.Fatalf("nil-nil append is not nil")
	}
	s2 := append(emptyBytes, nilBytes...)
	if s2 == nil {
		t.Fatalf("empty-nil append is nil")
	}
}

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

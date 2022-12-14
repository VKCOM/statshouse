// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package basictl

import (
	"bytes"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

func TestBuf_String(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		var rw []byte
		var errW, errR error

		in := rapid.String().Draw(t, "in")
		rw, errW = StringWrite(rw, in)
		if errW != nil {
			t.Fatalf("failed to write %#v: %v", in, errW)
		}
		if len(rw)%4 != 0 {
			t.Fatalf("size not divisible by 4: %v", len(rw))
		}
		// if n := StringSize(len(in)); n != rw.Len() {
		//	t.Fatalf("invalid size: %v instead of %v", rw.Len(), n)
		// }

		var out string
		rw, errR = StringRead(rw, &out)
		if errR != nil {
			t.Fatalf("failed to read: %v", errR)
		}

		if in != out {
			t.Fatalf("got back %#v after writing %#v", out, in)
		}
		if len(rw) != 0 {
			t.Fatalf("%v unread bytes left", len(rw))
		}
	})
}

func TestBuf_ByteSlice(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		var rw []byte
		var errW, errR error

		in := rapid.SliceOf(rapid.Byte()).Draw(t, "in")
		rw, errW = StringWriteBytes(rw, in)
		if errW != nil {
			t.Fatalf("failed to write %#v: %v", in, errW)
		}
		if len(rw)%4 != 0 {
			t.Fatalf("size not divisible by 4: %v", len(rw))
		}
		// if n := StringSize(len(in)); n != rw.Len() {
		//	t.Fatalf("invalid size: %v instead of %v", rw.Len(), n)
		// }

		out := rapid.SliceOf(rapid.Byte()).Draw(t, "out")
		rw, errR = StringReadBytes(rw, &out)
		if errR != nil {
			t.Fatalf("failed to read: %v", errR)
		}

		if !bytes.Equal(in, out) {
			t.Fatalf("got back %#v after writing %#v", out, in)
		}
		if len(rw) != 0 {
			t.Fatalf("%v unread bytes left", len(rw))
		}
	})
}

// We have no string version of this test because it works too slow.
// But we know StringRead is a wrapper around StringReadBytes, and StringWrite
func TestBuf_ByteSliceHuge(t *testing.T) {
	t.Parallel()

	prefixLen := bigStringLen - 10
	in := []byte(strings.Repeat("-", prefixLen))
	var out []byte
	var rw []byte

	rapid.Check(t, func(t *rapid.T) {
		var errW, errR error

		in = append(in[:prefixLen], rapid.SliceOf(rapid.Byte()).Draw(t, "in")...)
		rw, errW = StringWriteBytes(rw[:0], in)
		if errW != nil {
			t.Fatalf("failed to write %#v: %v", in, errW)
		}
		if len(rw)%4 != 0 {
			t.Fatalf("size not divisible by 4: %v", len(rw))
		}
		// if n := StringSize(len(in)); n != rw.Len() {
		//	t.Fatalf("invalid size: %v instead of %v", rw.Len(), n)
		// }

		out = append(out[:0], rapid.SliceOf(rapid.Byte()).Draw(t, "out")...)
		rw, errR = StringReadBytes(rw, &out)
		if errR != nil {
			t.Fatalf("failed to read: %v", errR)
		}

		if !bytes.Equal(in, out) {
			t.Fatalf("got back %#v after writing %#v", out, in)
		}
		if len(rw) != 0 {
			t.Fatalf("%v unread bytes left", len(rw))
		}
	})
}

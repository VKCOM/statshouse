// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package tlrw

import (
	"bytes"
	"testing"

	"pgregory.net/rapid"
)

func TestBuf_Tag(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		rw := &bytes.Buffer{}

		tag := rapid.Uint32().Draw(t, "tag")
		WriteUint32(rw, tag)
		if rw.Len()%4 != 0 {
			t.Fatalf("size not divisible by 4: %v", rw.Len())
		}

		err := ReadExactTag(rw, tag)
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		if rw.Len() != 0 {
			t.Fatalf("%v unread bytes left", rw.Len())
		}
	})
}

func TestBuf_Uint32(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		rw := &bytes.Buffer{}

		in := rapid.Uint32().Draw(t, "in")
		WriteUint32(rw, in)
		if rw.Len()%4 != 0 {
			t.Fatalf("size not divisible by 4: %v", rw.Len())
		}

		var out uint32
		err := ReadUint32(rw, &out)
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		if in != out {
			t.Fatalf("got back %#v after writing %#v", out, in)
		}
		if rw.Len() != 0 {
			t.Fatalf("%v unread bytes left", rw.Len())
		}
	})
}

func TestBuf_Int32(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		rw := &bytes.Buffer{}

		in := rapid.Int32().Draw(t, "in")
		WriteInt32(rw, in)
		if rw.Len()%4 != 0 {
			t.Fatalf("size not divisible by 4: %v", rw.Len())
		}

		var out int32
		err := ReadInt32(rw, &out)
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		if in != out {
			t.Fatalf("got back %#v after writing %#v", out, in)
		}
		if rw.Len() != 0 {
			t.Fatalf("%v unread bytes left", rw.Len())
		}
	})
}

func TestBuf_Uint64(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		rw := &bytes.Buffer{}

		in := rapid.Uint64().Draw(t, "in")
		WriteUint64(rw, in)
		if rw.Len()%4 != 0 {
			t.Fatalf("size not divisible by 4: %v", rw.Len())
		}

		var out uint64
		err := ReadUint64(rw, &out)
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		if in != out {
			t.Fatalf("got back %#v after writing %#v", out, in)
		}
		if rw.Len() != 0 {
			t.Fatalf("%v unread bytes left", rw.Len())
		}
	})
}

func TestBuf_Int64(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		rw := &bytes.Buffer{}

		in := rapid.Int64().Draw(t, "in")
		WriteInt64(rw, in)
		if rw.Len()%4 != 0 {
			t.Fatalf("size not divisible by 4: %v", rw.Len())
		}

		var out int64
		err := ReadInt64(rw, &out)
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		if in != out {
			t.Fatalf("got back %#v after writing %#v", out, in)
		}
		if rw.Len() != 0 {
			t.Fatalf("%v unread bytes left", rw.Len())
		}
	})
}

func TestBuf_Float32(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		rw := &bytes.Buffer{}

		in := rapid.Float32().Draw(t, "in")
		WriteFloat32(rw, in)
		if rw.Len()%4 != 0 {
			t.Fatalf("size not divisible by 4: %v", rw.Len())
		}

		var out float32
		err := ReadFloat32(rw, &out)
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		if in != out {
			t.Fatalf("got back %#v after writing %#v", out, in)
		}
		if rw.Len() != 0 {
			t.Fatalf("%v unread bytes left", rw.Len())
		}
	})
}

func TestBuf_Float64(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		rw := &bytes.Buffer{}

		in := rapid.Float64().Draw(t, "in")
		WriteFloat64(rw, in)
		if rw.Len()%4 != 0 {
			t.Fatalf("size not divisible by 4: %v", rw.Len())
		}

		var out float64
		err := ReadFloat64(rw, &out)
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		if in != out {
			t.Fatalf("got back %#v after writing %#v", out, in)
		}
		if rw.Len() != 0 {
			t.Fatalf("%v unread bytes left", rw.Len())
		}
	})
}

func TestBuf_String(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		rw := &bytes.Buffer{}

		in := rapid.String().Draw(t, "in")
		errW := WriteString(rw, in)
		if errW != nil {
			t.Fatalf("failed to write %#v: %v", in, errW)
		}
		if rw.Len()%4 != 0 {
			t.Fatalf("size not divisible by 4: %v", rw.Len())
		}
		if n := StringSize(len(in)); n != rw.Len() {
			t.Fatalf("invalid size: %v instead of %v", rw.Len(), n)
		}

		var out string
		errR := ReadString(rw, &out)
		if errR != nil {
			t.Fatalf("failed to read: %v", errR)
		}

		if in != out {
			t.Fatalf("got back %#v after writing %#v", out, in)
		}
		if rw.Len() != 0 {
			t.Fatalf("%v unread bytes left", rw.Len())
		}
	})
}

func TestBuf_ByteSlice(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		rw := &bytes.Buffer{}

		in := rapid.SliceOf(rapid.Byte()).Draw(t, "in")
		errW := WriteStringBytes(rw, in)
		if errW != nil {
			t.Fatalf("failed to write %#v: %v", in, errW)
		}
		if rw.Len()%4 != 0 {
			t.Fatalf("size not divisible by 4: %v", rw.Len())
		}
		if n := StringSize(len(in)); n != rw.Len() {
			t.Fatalf("invalid size: %v instead of %v", rw.Len(), n)
		}

		out := rapid.SliceOf(rapid.Byte()).Draw(t, "out")
		errR := ReadStringBytes(rw, &out)
		if errR != nil {
			t.Fatalf("failed to read: %v", errR)
		}

		if !bytes.Equal(in, out) {
			t.Fatalf("got back %#v after writing %#v", out, in)
		}
		if rw.Len() != 0 {
			t.Fatalf("%v unread bytes left", rw.Len())
		}
	})
}

func paddingLenLegacy(l int) int {
	padding := l + 1
	if l > tinyStringLen {
		padding += 3
	}
	padding %= 4

	if padding > 0 {
		padding = 4 - padding
	}
	return padding
}

func TestPaddingLen(t *testing.T) {
	for i := 0; i < maxStringLen; i++ {
		a := paddingLen(i)
		b := paddingLenLegacy(i)
		if a != b {
			t.Fatal("PaddingLen and paddingLenLegacy differ for length", i, a, b)
		}
	}
}

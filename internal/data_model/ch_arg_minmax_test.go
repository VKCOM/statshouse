// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

type testProtoReader struct {
	buf *bytes.Reader
}

func (r *testProtoReader) ReadByte() (byte, error) {
	return r.buf.ReadByte()
}

func (r *testProtoReader) ReadFull(buf []byte) error {
	_, err := r.buf.Read(buf)
	return err
}

func TestArgMinMaxInt32Float32_Roundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		arg := ArgMinMaxInt32Float32{
			Arg: rapid.Int32().Draw(t, "arg"),
			Val: rapid.Float32().Draw(t, "val"),
		}
		buf := arg.MarshalAppend(nil)
		r := &testProtoReader{buf: bytes.NewReader(buf)}
		var out ArgMinMaxInt32Float32
		require.NoError(t, out.ReadFromProto(r))
		require.Equal(t, arg.Arg, out.Arg)
		require.Equal(t, arg.Val, out.Val)
	})
}

func TestArgMinMaxStringFloat32_Roundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		asString := rapid.String().Draw(t, "asString")
		val := rapid.Float32().Draw(t, "val")
		arg := ArgMinMaxStringFloat32{
			AsString: asString,
			Val:      val,
		}
		buf := arg.MarshallAppend(nil)
		r := &testProtoReader{buf: bytes.NewReader(buf)}
		var out ArgMinMaxStringFloat32
		_, err := out.ReadFromProto(r, make([]byte, 0, 16))
		require.NoError(t, err)
		require.Equal(t, arg.AsString, out.AsString)
		require.Equal(t, arg.Val, out.Val)
	})
}

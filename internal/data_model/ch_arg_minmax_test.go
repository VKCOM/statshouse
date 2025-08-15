// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestArgMinMaxInt32Float32_Roundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		arg := ArgMinMaxInt32Float32{
			Arg: rapid.Int32().Draw(t, "arg"),
			Val: rapid.Float32().Draw(t, "val"),
		}
		buf := arg.MarshalAppend(nil)
		r := bytes.NewReader(buf)
		var out ArgMinMaxInt32Float32
		require.NoError(t, out.ReadFrom(r))
		require.Equal(t, arg.Arg, out.Arg)
		require.Equal(t, arg.Val, out.Val)
	})
}

func TestArgMinMaxStringFloat32_StringArg_Roundtrip(t *testing.T) {
	scratch := make([]byte, 32)
	rapid.Check(t, func(t *rapid.T) {
		asString := rapid.String().Draw(t, "asString")
		val := rapid.Float32().Draw(t, "val")
		asString = strings.Trim(asString, "\x00")
		arg := ArgMinMaxStringFloat32{
			AsString: asString,
			Val:      val,
		}
		buf := arg.MarshallAppend(nil)
		r := bytes.NewReader(buf)
		var out ArgMinMaxStringFloat32
		var err error
		scratch, err = out.ReadFrom(r, scratch[:0])
		require.NoError(t, err)
		require.Equal(t, arg.AsString, out.AsString)
		require.Equal(t, arg.Val, out.Val)
	})
}

func TestArgMinMaxStringFloat32_IntArg_Roundtrip(t *testing.T) {
	scratch := make([]byte, 32)
	rapid.Check(t, func(t *rapid.T) {
		asInt := rapid.Int32().Draw(t, "arg")
		val := rapid.Float32().Draw(t, "val")
		arg := ArgMinMaxStringFloat32{
			AsInt32: asInt,
			Val:     val,
		}
		buf := arg.MarshallAppend(nil)
		r := bytes.NewReader(buf)
		var out ArgMinMaxStringFloat32
		var err error
		scratch, err = out.ReadFrom(r, scratch[:0])
		require.NoError(t, err)
		require.Equal(t, arg.Val, out.Val)
		require.Equal(t, arg.AsInt32, out.AsInt32)
	})
}

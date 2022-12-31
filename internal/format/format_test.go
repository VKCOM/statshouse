// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package format

import (
	"fmt"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"go4.org/mem"
	"pgregory.net/rapid"
)

func TestValidIdent(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.StringMatching(`[a-zA-Z0-9_]*`).Draw(t, "s")
		ok := len(s) > 0 && len(s) <= MaxStringLen && s[0] != '_' && !(s[0] >= '0' && s[0] <= '9')
		require.Equal(t, ok, validIdent(mem.S(s)))
	})
}

func TestInvalidTagValuePrefix(t *testing.T) {
	s := "whatever"
	require.True(t, ValidStringValueLegacy(mem.S(s)))
	require.False(t, ValidStringValueLegacy(mem.S(tagValueCodePrefix+s)))
}

func TestValidStringValue(t *testing.T) {
	s := "what ever"
	require.True(t, ValidStringValueLegacy(mem.S(s)))
	require.True(t, ValidStringValue(mem.S(s)))
	s = " what ever"
	require.False(t, ValidStringValueLegacy(mem.S(s)))
	require.False(t, ValidStringValue(mem.S(s)))
	s = "what ever "
	require.False(t, ValidStringValueLegacy(mem.S(s)))
	require.False(t, ValidStringValue(mem.S(s)))
	s = "what  ever"
	require.True(t, ValidStringValueLegacy(mem.S(s)))
	require.False(t, ValidStringValue(mem.S(s)))

	dst, err := AppendValidStringValue(nil, []byte("what ever"))
	require.NoError(t, err)
	require.Equal(t, string(dst), "what ever")
	dst, err = AppendValidStringValue(nil, []byte("what \n ever\n"))
	require.NoError(t, err)
	require.Equal(t, string(dst), "what ever")
	dst, err = AppendValidStringValue(nil, []byte("\nwhat ever"))
	require.NoError(t, err)
	require.Equal(t, string(dst), "what ever")
	dst, err = AppendValidStringValue(nil, []byte("what      \t ever"))
	require.NoError(t, err)
	require.Equal(t, string(dst), "what ever")
}

func TestAppendValidStringValue_Bytes(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(4, MaxStringLen).Draw(t, "n")
		src := rapid.SliceOf(rapid.Byte()).Draw(t, "src")
		doubleIndex := rapid.IntRange(0, 1+len(src)*2).Draw(t, "doubleIndex") // rapid does not know double spaces are interesting for us
		if doubleIndex < len(src) {
			src[doubleIndex] = ' '
		}
		if doubleIndex+1 < len(src) {
			src[doubleIndex+1] = ' '
		}
		dst, err := appendValidStringValueLegacy(nil, src, n, false)
		dst2, err2 := appendValidStringValue(nil, src, n, false)
		require.False(t, strings.Contains(string(dst2), "  "))
		if err != nil {
			require.True(t, !validStringValueLegacy(mem.B(src), n))
			require.True(t, !validStringValue(mem.B(src), n)) // if value is invalid, collapsing spaces is also invalid
		} else {
			require.Truef(t, validStringValueLegacy(mem.B(dst), n), "dst %q", dst)
			dst_repeat, err_repeat := appendValidStringValueLegacy(nil, dst, n, false)
			require.NoError(t, err_repeat)
			require.Equal(t, string(dst), string(dst_repeat))
		}
		if err2 != nil {
			require.True(t, !validStringValue(mem.B(src), n))
		} else {
			require.Truef(t, validStringValueLegacy(mem.B(dst), n), "dst %q", dst) // collapsed space value is also valid for func which does not check collapsing
			require.Truef(t, validStringValue(mem.B(dst2), n), "dst %q", dst2)
			dst_repeat, err_repeat := appendValidStringValueLegacy(nil, dst2, n, false)
			require.NoError(t, err_repeat)
			require.Equal(t, string(dst2), string(dst_repeat))
		}
		dst, err = appendValidStringValue(nil, src, n, true)
		require.NoError(t, err)
		require.Truef(t, validStringValue(mem.B(dst), n), "dst %q", dst)
		dst_repeat, err := appendValidStringValue(nil, dst, n, true)
		require.NoError(t, err)
		require.Equal(t, string(dst), string(dst_repeat))
	})
}

func TestAppendValidStringValue_String(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(4, MaxStringLen).Draw(t, "n")
		src := rapid.StringOfN(rapid.RuneFrom([]rune{' '}, unicode.PrintRanges...), 1, n, n).Draw(t, "src")
		src = strings.TrimSpace(src)
		if len(src) == 0 {
			t.Skip()
		}
		require.True(t, validStringValueLegacy(mem.S(src), n))
		dst, err := appendValidStringValueLegacy(nil, []byte(src), n, false)
		require.NoError(t, err)
		require.Equal(t, src, string(dst))
	})
}

func TestBytePrint(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		b := rapid.SliceOfN(rapid.Byte(), 1, 1).Draw(t, "b")
		r, _ := utf8.DecodeRune(b)
		if r != utf8.RuneError {
			require.Equal(t, unicode.IsPrint(r), bytePrint(b[0]))
		}
	})
}

func BenchmarkAppendValidStringValueFastPath(b *testing.B) {
	in := make([]byte, MaxStringLen/8)
	for i := range in {
		in[i] = 'a'
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in, _ = AppendValidStringValueLegacy(in[:0], in)
	}
	fmt.Printf("result: %d\n", len(in)) // do not allow to optimize out
}

func BenchmarkAppendValidStringValueCollapseSpacesFastPath(b *testing.B) {
	in := make([]byte, MaxStringLen/8)
	for i := range in {
		in[i] = 'a'
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in, _ = AppendValidStringValue(in[:0], in)
	}
	fmt.Printf("result: %d\n", len(in)) // do not allow to optimize out
}

func BenchmarkAppendValidStringValueSlowPath(b *testing.B) {
	in := make([]byte, MaxStringLen/2)
	for i := range in {
		in[i] = 'a'
	}
	in = append(in, "Ёж"...)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in, _ = AppendValidStringValueLegacy(in[:0], in)
	}
	fmt.Printf("result: %d\n", len(in)) // do not allow to optimize out
}

func BenchmarkAppendValidStringValueCollapseSpacesSlowPath(b *testing.B) {
	in := make([]byte, MaxStringLen/2)
	for i := range in {
		in[i] = 'a'
		if i > 10 && i < 15 {
			in[i] = ' '
		}
	}
	in = append(in, "Ёж"...)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in, _ = AppendValidStringValue(in[:0], in)
	}
	fmt.Printf("result: %d\n", len(in)) // do not allow to optimize out
}

func BenchmarkAppendHexValue(b *testing.B) {
	in := make([]byte, MaxStringLen/2)
	for i := range in {
		in[i] = 'a'
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in = AppendHexStringValue(in[:0], in)
	}
	fmt.Printf("result: %d\n", len(in)) // do not allow to optimize out
}

func TestAllowedResolution(t *testing.T) {
	for i := -10; i < 70; i++ {
		r := AllowedResolution(i)
		divisible := false
		if i > 0 {
			divisible = (60/i)*i == 60
		}
		require.Equal(t, r == i, divisible)
	}
}

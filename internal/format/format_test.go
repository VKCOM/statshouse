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

	"go4.org/mem"
	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"
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
		dst, err := appendValidStringValue(nil, src, n, false)
		require.False(t, strings.Contains(string(dst), "  "))
		if err != nil {
			require.True(t, !validStringValue(mem.B(src), n)) // if value is invalid, collapsing spaces is also invalid
		} else {
			require.Truef(t, validStringValue(mem.B(dst), n), "dst %q", dst)
			dst_repeat, err_repeat := appendValidStringValue(nil, dst, n, false)
			require.NoError(t, err_repeat)
			require.Equal(t, string(dst), string(dst_repeat))
		}
		dst, err = appendValidStringValue(nil, src, n, true)
		require.NoError(t, err)
		require.Truef(t, validStringValue(mem.B(dst), n), "dst %q", dst)
		dst_repeat, err := appendValidStringValue(nil, dst, n, true)
		require.NoError(t, err)
		require.Equal(t, string(dst), string(dst_repeat))
	})
}

/* TODO - do not add more than single space inside
func TestAppendValidStringValue_String(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(4, MaxStringLen).Draw(t, "n")
		src := rapid.StringOfN(rapid.RuneFrom([]rune{' '}, unicode.PrintRanges...), 1, n, n).Draw(t, "src")
		src = strings.TrimSpace(src)
		if len(src) == 0 {
			t.Skip()
		}
		require.True(t, validStringValue(mem.S(src), n))
		dst, err := appendValidStringValue(nil, []byte(src), n, false)
		require.NoError(t, err)
		require.Equal(t, src, string(dst))
	})
}
*/

func TestAppendValidStringValue_UTF16(t *testing.T) {
	// We have a lot of java people converting strings using str.getBytes()
	dst, err := AppendValidStringValue(nil, []byte("b\x00e\x00t\x00a\x00"))
	require.NoError(t, err)
	require.Equal(t, "b�e�t�a�", string(dst))
	dst, err = AppendValidStringValue(nil, []byte("\x00b\x00e\x00t\x00a"))
	require.NoError(t, err)
	require.Equal(t, "�b�e�t�a", string(dst))
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
		in, _ = AppendValidStringValue(in[:0], in)
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
		in, _ = AppendValidStringValue(in[:0], in)
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
		r1 := allowedResolutionSwitch(i)
		r2 := allowedResolutionTable(i)
		require.Equal(t, r1, r2)
		divisible := false
		if i > 0 {
			divisible = (60/i)*i == 60
		}
		require.Equal(t, r1 == i, divisible)
	}
}

func BenchmarkAllowedResolutionSwitch(b *testing.B) {
	allowed := 0
	for i := 0; i < b.N; i++ {
		if allowedResolutionSwitch(i&63) == i&63 {
			allowed++
		}
	}
	fmt.Printf("result: %d\n", allowed) // do not allow to optimize out
}

func BenchmarkAllowedResolutionTable(b *testing.B) {
	allowed := 0
	for i := 0; i < b.N; i++ {
		if allowedResolutionTable(i&63) == i&63 {
			allowed++
		}
	}
	fmt.Printf("result: %d\n", allowed) // do not allow to optimize out
}

func TestNamespaceConst(t *testing.T) {
	require.Equal(t, NamespaceSeparator, string(NamespaceSeparatorRune))
}

func TestValidDashboardName(t *testing.T) {
	tests := []struct {
		name string
		args string
		want bool
	}{
		{"", " ", false},
		{"", " abc", false},
		{"", "a bc", true},
		{"", "[ a bc", true},
		{"", "{ a bc", true},
		{"", "{:} a bc", true},
		{"", "1:{:} a bc", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidDashboardName(tt.args); got != tt.want {
				t.Errorf("ValidDashboardName() = %v, want %v", got, tt.want)
			}
		})
	}
}

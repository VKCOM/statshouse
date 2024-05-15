// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

const (
	argNull = iota
	argZeroBlob
	argBlob
	argBlobUnsafe
	argBlobString
	argText
	argTextUnsafe
	argTextString
	argInt64
	argFloat64
)

type Arg struct {
	name string
	typ  int

	b []byte
	s string
	n int64
	f float64

	slice  bool
	bs     [][]byte
	ss     []string
	ns     []int64
	fs     []float64
	length int
}

func Null(name string) Arg {
	return Arg{name: name, typ: argNull}
}

func ZeroBlob(name string, size int) Arg {
	return Arg{name: name, typ: argZeroBlob, n: int64(size)}
}

func Blob(name string, b []byte) Arg {
	return Arg{name: name, typ: argBlob, b: b}
}

func BlobSlice(name string, bs [][]byte) Arg {
	return Arg{name: name, typ: argBlob, slice: true, bs: bs, length: len(bs)}
}

func BlobUnsafe(name string, b []byte) Arg {
	return Arg{name: name, typ: argBlobUnsafe, b: b}
}

func BlobUnsafeSlice(name string, bs [][]byte) Arg {
	return Arg{name: name, typ: argBlobUnsafe, slice: true, bs: bs, length: len(bs)}
}

func BlobString(name string, s string) Arg {
	return Arg{name: name, typ: argBlobString, s: s}
}

func BlobStringSlice(name string, ss []string) Arg {
	return Arg{name: name, typ: argBlobString, slice: true, ss: ss, length: len(ss)}
}

func Text(name string, b []byte) Arg {
	return Arg{name: name, typ: argText, b: b}
}

func TextSlice(name string, bs [][]byte) Arg {
	return Arg{name: name, typ: argText, slice: true, bs: bs, length: len(bs)}
}

func TextUnsafe(name string, b []byte) Arg {
	return Arg{name: name, typ: argTextUnsafe, b: b}
}

func TextUnsafeSlice(name string, bs [][]byte) Arg {
	return Arg{name: name, typ: argTextUnsafe, slice: true, bs: bs, length: len(bs)}
}

func TextString(name string, s string) Arg {
	return Arg{name: name, typ: argTextString, s: s}
}

func TextStringSlice(name string, ss []string) Arg {
	return Arg{name: name, typ: argTextString, slice: true, ss: ss, length: len(ss)}
}

func Int64(name string, n int64) Arg {
	return Arg{name: name, typ: argInt64, n: n}
}

func Int64Slice(name string, ns []int64) Arg {
	return Arg{name: name, typ: argInt64, slice: true, ns: ns, length: len(ns)}
}

func Float64(name string, f float64) Arg {
	return Arg{name: name, typ: argFloat64, f: f}
}

func Float64Slice(name string, fs []float64) Arg {
	return Arg{name: name, typ: argFloat64, slice: true, fs: fs, length: len(fs)}
}

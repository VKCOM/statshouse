// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlitev2

import (
	"fmt"

	"github.com/vkcom/statshouse/internal/sqlitev2/sqlite0"
)

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

func Text(name string, s string) Arg {
	return Arg{name: name, typ: argText, s: s}
}

func BlobUnsafe(name string, b []byte) Arg {
	return Arg{name: name, typ: argBlobUnsafe, b: b}
}

func BlobString(name string, s string) Arg {
	return Arg{name: name, typ: argBlobString, s: s}
}

// TODO test
func TextString(name string, s string) Arg {
	return Arg{name: name, typ: argTextString, s: s}
}

func Integer(name string, n int64) Arg {
	return Arg{name: name, typ: argInt64, n: n}
}

func Real(name string, f float64) Arg {
	return Arg{name: name, typ: argFloat64, f: f}
}

// Deprecated: DO NOT USE
func IntegerSlice(name string, ns []int64) Arg {
	return Arg{name: name, typ: argInt64, slice: true, ns: ns, length: len(ns)}
}

// Deprecated: DO NOT USE
func BlobSlice(name string, bs [][]byte) Arg {
	return Arg{name: name, typ: argBlob, slice: true, bs: bs, length: len(bs)}
}

// Deprecated: DO NOT USE
func TextStringSlice(name string, ss []string) Arg {
	return Arg{name: name, typ: argTextString, slice: true, ss: ss, length: len(ss)}
}

func bindParam(si *sqlite0.Stmt, builder *queryBuilder, args ...Arg) (*sqlite0.Stmt, error) {
	start := 0
	for _, arg := range args {
		if arg.slice {
			for i := 0; i < arg.length; i++ {
				p := si.ParamBytes(builder.NameBy(start))
				var err error
				switch arg.typ {
				case argBlob:
					err = si.BindBlob(p, arg.bs[i])
				case argBlobUnsafe:
					err = si.BindBlobUnsafe(p, arg.bs[i])
				case argBlobString:
					err = si.BindBlobString(p, arg.ss[i])
				case argText:
					err = si.BindText(p, arg.bs[i])
				case argTextUnsafe:
					err = si.BindTextUnsafe(p, arg.bs[i])
				case argTextString:
					err = si.BindTextString(p, arg.ss[i])
				case argInt64:
					err = si.BindInt64(p, arg.ns[i])
				case argFloat64:
					err = si.BindFloat64(p, arg.fs[i])
				default:
					err = fmt.Errorf("unsupported slice arg type for %q: %v", arg.name, arg.typ)
				}
				if err != nil {
					return nil, err
				}
				start++
			}
		} else {
			p := si.Param(arg.name)
			var err error
			switch arg.typ {
			case argNull:
				err = si.BindNull(p)
			case argZeroBlob:
				err = si.BindZeroBlob(p, int(arg.n))
			case argBlob:
				err = si.BindBlob(p, arg.b)
			case argBlobUnsafe:
				err = si.BindBlobUnsafe(p, arg.b)
			case argBlobString:
				err = si.BindBlobString(p, arg.s)
			case argText:
				err = si.BindText(p, arg.b)
			case argTextUnsafe:
				err = si.BindTextUnsafe(p, arg.b)
			case argTextString:
				err = si.BindTextString(p, arg.s)
			case argInt64:
				err = si.BindInt64(p, arg.n)
			case argFloat64:
				err = si.BindFloat64(p, arg.f)
			default:
				err = fmt.Errorf("unsupported arg type for %q: %v", arg.name, arg.typ)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return si, nil
}

// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlitev2

import "fmt"
import "github.com/vkcom/statshouse/internal/sqlite/internal/sqlite0"

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

func BlobUnsafe(name string, b []byte) Arg {
	return Arg{name: name, typ: argBlobUnsafe, b: b}
}

func BlobString(name string, s string) Arg {
	return Arg{name: name, typ: argBlobString, s: s}
}

func Text(name string, b []byte) Arg {
	return Arg{name: name, typ: argText, b: b}
}

func TextUnsafe(name string, b []byte) Arg {
	return Arg{name: name, typ: argTextUnsafe, b: b}
}

func TextString(name string, s string) Arg {
	return Arg{name: name, typ: argTextString, s: s}
}

func Int64(name string, n int64) Arg {
	return Arg{name: name, typ: argInt64, n: n}
}

func Float64(name string, f float64) Arg {
	return Arg{name: name, typ: argFloat64, f: f}
}

func bindParam(si *sqlite0.Stmt, args ...Arg) (*sqlite0.Stmt, error) {
	for _, arg := range args {
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
	return si, nil
}

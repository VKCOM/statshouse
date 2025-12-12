// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package logz

import (
	"go.uber.org/zap"
)

type Field = zap.Field

var (
	Skip        = zap.Skip
	Binary      = zap.Binary
	Bool        = zap.Bool
	Boolp       = zap.Boolp
	ByteString  = zap.ByteString
	Complex128  = zap.Complex128
	Complex128p = zap.Complex128p
	Complex64   = zap.Complex64
	Complex64p  = zap.Complex64p
	Float64     = zap.Float64
	Float64p    = zap.Float64p
	Float32     = zap.Float32
	Float32p    = zap.Float32p
	Int         = zap.Int
	Intp        = zap.Intp
	Int64       = zap.Int64
	Int64p      = zap.Int64p
	Int32       = zap.Int32
	Int32p      = zap.Int32p
	Int16       = zap.Int16
	Int16p      = zap.Int16p
	Int8        = zap.Int8
	Int8p       = zap.Int8p
	String      = zap.String
	Stringp     = zap.Stringp
	Uint        = zap.Uint
	Uintp       = zap.Uintp
	Uint64      = zap.Uint64
	Uint64p     = zap.Uint64p
	Uint32      = zap.Uint32
	Uint32p     = zap.Uint32p
	Uint16      = zap.Uint16
	Uint16p     = zap.Uint16p
	Uint8       = zap.Uint8
	Uint8p      = zap.Uint8p
	Uintptr     = zap.Uintptr
	Uintptrp    = zap.Uintptrp
	Reflect     = zap.Reflect
	Namespace   = zap.Namespace
	Stringer    = zap.Stringer
	Time        = zap.Time
	Timep       = zap.Timep
	Stack       = zap.Stack
	Duration    = zap.Duration
	Durationp   = zap.Durationp
	Object      = zap.Object
	Any         = zap.Any

	Array       = zap.Array
	Bools       = zap.Bools
	ByteStrings = zap.ByteStrings
	Complex128s = zap.Complex128s
	Complex64s  = zap.Complex64s
	Durations   = zap.Durations
	Float64s    = zap.Float64s
	Float32s    = zap.Float32s
	Ints        = zap.Ints
	Int64s      = zap.Int64s
	Int32s      = zap.Int32s
	Int16s      = zap.Int16s
	Int8s       = zap.Int8s
	Strings     = zap.Strings
	Times       = zap.Times
	Uints       = zap.Uints
	Uint64s     = zap.Uint64s
	Uint32s     = zap.Uint32s
	Uint16s     = zap.Uint16s
	Uint8s      = zap.Uint8s
	Uintptrs    = zap.Uintptrs
	Errors      = zap.Errors
	Err         = zap.Error
)

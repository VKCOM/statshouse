// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package basictl

import (
	"fmt"
	"io"
	"math"
)

type TL2WriteContext struct {
	// buffer for allocations of objects sizes
	SizeBuffer []int
}

type TL2ReadContext struct{}

func TL2UnexpectedByteError(actualByte, expectedByte byte) error {
	return fmt.Errorf("unexpected byte %d, expected: %d", actualByte, expectedByte)
}

func TL2ExpectedZeroError() error {
	return fmt.Errorf("expected zero value")
}

func TL2ExpectedNonZeroError() error {
	return fmt.Errorf("expected non zero value")
}

func TL2Error(format string, a ...any) error {
	return fmt.Errorf("tl2 error: %s", fmt.Sprintf(format, a...))
}

func TL2ReadSize(r []byte, l *int) ([]byte, error) {
	if len(r) == 0 {
		return r, io.ErrUnexpectedEOF
	}
	b0 := r[0]

	switch {
	case b0 <= tinyStringLen:
		*l = int(b0)
		r = r[1:]
	case b0 == bigStringMarker:
		if len(r) < 4 {
			return r, io.ErrUnexpectedEOF
		}
		*l = (int(r[3]) << 16) + (int(r[2]) << 8) + (int(r[1]) << 0)
		r = r[4:]
		if *l <= tinyStringLen {
			return r, fmt.Errorf("non-canonical (big) string format for length: %d", *l)
		}
	default: // hugeStringMarker
		if len(r) < 8 {
			return r, io.ErrUnexpectedEOF
		}
		l64 := (int64(r[7]) << 48) + (int64(r[6]) << 40) + (int64(r[5]) << 32) + (int64(r[4]) << 24) + (int64(r[3]) << 16) + (int64(r[2]) << 8) + (int64(r[1]) << 0)
		if l64 > math.MaxInt {
			return r, fmt.Errorf("string length cannot be represented on 32-bit platform: %d", l64)
		}
		*l = int(l64)
		r = r[8:]
		if *l <= bigStringLen {
			return r, fmt.Errorf("non-canonical (huge) string format for length: %d", *l)
		}
	}
	return r, nil
}

func TL2ParseSize(r []byte) ([]byte, int, error) {
	l := 0
	if len(r) == 0 {
		return r, l, io.ErrUnexpectedEOF
	}
	b0 := r[0]

	switch {
	case b0 <= tinyStringLen:
		l = int(b0)
		r = r[1:]
	case b0 == bigStringMarker:
		if len(r) < 4 {
			return r, l, io.ErrUnexpectedEOF
		}
		l = (int(r[3]) << 16) + (int(r[2]) << 8) + (int(r[1]) << 0)
		r = r[4:]
		if l <= tinyStringLen {
			return r, l, fmt.Errorf("non-canonical (big) string format for length: %d", l)
		}
	default: // hugeStringMarker
		if len(r) < 8 {
			return r, l, io.ErrUnexpectedEOF
		}
		l64 := (int64(r[7]) << 48) + (int64(r[6]) << 40) + (int64(r[5]) << 32) + (int64(r[4]) << 24) + (int64(r[3]) << 16) + (int64(r[2]) << 8) + (int64(r[1]) << 0)
		if l64 > math.MaxInt {
			return r, l, fmt.Errorf("string length cannot be represented on 32-bit platform: %d", l64)
		}
		l = int(l64)
		r = r[8:]
		if l <= bigStringLen {
			return r, l, fmt.Errorf("non-canonical (huge) string format for length: %d", l)
		}
	}
	return r, l, nil
}

func TL2WriteSize(w []byte, l int) []byte {
	switch {
	case l <= tinyStringLen:
		w = append(w, byte(l))
	case l <= bigStringLen:
		w = append(w, bigStringMarker, byte(l), byte(l>>8), byte(l>>16))
	default:
		if l > hugeStringLen { // for correctness only, we do not expect strings so huge
			l = hugeStringLen
		}
		w = append(w, hugeStringMarker, byte(l), byte(l>>8), byte(l>>16), byte(l>>24), byte(l>>32), byte(l>>40), byte(l>>48))
	}
	return w
}

func TL2CalculateSize(l int) int {
	switch {
	case l <= tinyStringLen:
		return 1
	case l <= bigStringLen:
		return 4
	default:
		return 8
	}
}

func StringWriteTL2(w []byte, v string) []byte {
	w = TL2WriteSize(w, len(v))
	w = append(w, v...)
	return w
}

func StringBytesWriteTL2(w []byte, v []byte) []byte {
	w = TL2WriteSize(w, len(v))
	w = append(w, v...)
	return w
}

func StringReadTL2(r []byte, dst *string) (_ []byte, err error) {
	var l int
	if r, err = TL2ReadSize(r, &l); err != nil {
		return r, err
	}
	*dst = string(r[:l])
	return r[l:], nil
}

func StringReadBytesTL2(r []byte, dst *[]byte) (_ []byte, err error) {
	var l int
	if r, err = TL2ReadSize(r, &l); err != nil {
		return r, err
	}
	if cap(*dst) < l {
		*dst = make([]byte, l)
	} else {
		*dst = (*dst)[:l]
	}
	copy(*dst, r)
	return r[l:], nil
}

func ByteReadTL2(r []byte, b *byte) ([]byte, error) {
	if len(r) == 0 {
		return r, io.ErrUnexpectedEOF
	}
	*b = r[0]
	return r[1:], nil
}

func MaybeBoolWriteTL2(w []byte, b bool) []byte {
	if b {
		w = append(w, 2, 1<<0+1<<1, 1)
	} else {
		w = append(w, 2, 1<<0, 1)
	}
	return w
}

func MaybeBoolReadTL2(r []byte, b *bool) (_ []byte, err error) {
	var l int
	if r, err = TL2ReadSize(r, &l); err != nil {
		return r, err
	}
	if l == 0 {
		*b = false
	} else {
		curR := r[:l]
		r = r[l:]

		var block byte
		if curR, err = ByteReadTL2(curR, &block); err != nil {
			return curR, err
		}

		var constructor int
		if curR, err = TL2ReadSize(curR, &constructor); err != nil {
			return curR, err
		}

		if constructor != 1 {
			return curR, TL2Error("unknown constructor %d", constructor)
		}

		*b = (block & (1 << 1)) != 0
	}
	return r, err
}

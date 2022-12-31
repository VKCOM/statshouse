// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package tlrw

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const (
	tinyStringLen   = 253
	bigStringMarker = 0xfe
	maxStringLen    = 1 << 24
)

func WriteUint32(w *bytes.Buffer, v uint32) {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	_, _ = w.Write(tmp[:])
}

func WriteInt32(w *bytes.Buffer, v int32) {
	WriteUint32(w, uint32(v))
}

func WriteUint64(w *bytes.Buffer, v uint64) {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	_, _ = w.Write(tmp[:])
}

func WriteInt64(w *bytes.Buffer, v int64) {
	WriteUint64(w, uint64(v))
}

func WriteFloat32(w *bytes.Buffer, v float32) {
	WriteUint32(w, math.Float32bits(v))
}

func WriteFloat64(w *bytes.Buffer, v float64) {
	WriteUint64(w, math.Float64bits(v))
}

func WriteString(w *bytes.Buffer, v string) error {
	return writeString(w, v, false)
}

func writeString(w *bytes.Buffer, v string, truncate bool) error {
	l := len(v)
	if truncate && l >= maxStringLen {
		l = maxStringLen - 1
		v = v[:l]
	}

	switch {
	case l <= tinyStringLen:
		_ = w.WriteByte(uint8(l))
	case l < maxStringLen:
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], (uint32(l)<<8)|bigStringMarker)
		_, _ = w.Write(tmp[:])
	default:
		return fmt.Errorf("string(%v) exceeds maximum length %v", l, maxStringLen)
	}

	_, _ = w.WriteString(v)

	if padding := paddingLen(l); padding > 0 {
		var zeroes [4]byte
		_, _ = w.Write(zeroes[:padding])
	}

	return nil
}

func WriteStringBytes(w *bytes.Buffer, v []byte) error {
	return writeStringBytes(w, v, false)
}

func writeStringBytes(w *bytes.Buffer, v []byte, truncate bool) error {
	l := len(v)
	if truncate && l >= maxStringLen {
		l = maxStringLen - 1
		v = v[:l]
	}

	switch {
	case l <= tinyStringLen:
		_ = w.WriteByte(uint8(l))
	case l < maxStringLen:
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], (uint32(l)<<8)|bigStringMarker)
		_, _ = w.Write(tmp[:])
	default:
		return fmt.Errorf("string(%v) exceeds maximum length %v", l, maxStringLen)
	}

	_, _ = w.Write(v)

	if padding := paddingLen(l); padding > 0 {
		var zeroes [4]byte
		_, _ = w.Write(zeroes[:padding])
	}

	return nil
}

func StringSize(length int) int {
	switch {
	case length <= tinyStringLen:
		return 1 + length + paddingLen(length)
	case length < maxStringLen:
		return 4 + length + paddingLen(length)
	default:
		return -1
	}
}

func ReadUint32(r *bytes.Buffer, dst *uint32) error {
	b, err := nextN(r, 4)
	if err != nil {
		return err
	}
	*dst = binary.LittleEndian.Uint32(b)
	return nil
}

func ReadInt32(r *bytes.Buffer, dst *int32) error {
	var u uint32
	err := ReadUint32(r, &u)
	*dst = int32(u)
	return err
}

func ReadUint64(r *bytes.Buffer, dst *uint64) error {
	b, err := nextN(r, 8)
	if err != nil {
		return err
	}
	*dst = binary.LittleEndian.Uint64(b)
	return nil
}

func ReadInt64(r *bytes.Buffer, dst *int64) error {
	var u uint64
	err := ReadUint64(r, &u)
	*dst = int64(u)
	return err
}

func ReadFloat32(r *bytes.Buffer, dst *float32) error {
	var bits uint32
	if err := ReadUint32(r, &bits); err != nil {
		return err
	}
	*dst = math.Float32frombits(bits)
	return nil
}

func ReadFloat64(r *bytes.Buffer, dst *float64) error {
	var bits uint64
	if err := ReadUint64(r, &bits); err != nil {
		return err
	}
	*dst = math.Float64frombits(bits)
	return nil
}

func ReadString(r *bytes.Buffer, dst *string) error {
	var b []byte
	err := ReadStringBytes(r, &b)
	*dst = string(b)
	return err
}

// ReadLength32 prevents allocation of buffers if len objects of minObjectSize cannot fit into remaining buffer length
func ReadLength32(r *bytes.Buffer, n *int, minObjectSize int) error {
	var d uint32
	if err := ReadUint32(r, &d); err != nil {
		return err
	}
	if uint64(r.Len()) < uint64(d)*uint64(minObjectSize) { // Must wrap io.ErrUnexpectedEOF
		return fmt.Errorf("invalid length: %d for remaining reader length: %d and min object size %d: %w", d, r.Len(), minObjectSize, io.ErrUnexpectedEOF)
	}
	*n = int(d)
	return nil
}

func ReadStringBytes(r *bytes.Buffer, dst *[]byte) error {
	b0, err := r.ReadByte()
	if err != nil {
		return io.ErrUnexpectedEOF
	}

	var l int
	switch {
	case b0 <= tinyStringLen:
		l = int(b0)
	case b0 == bigStringMarker:
		sz, err := nextN(r, 3)
		if err != nil {
			return err
		}
		l = (int(sz[2]) << 16) + (int(sz[1]) << 8) + (int(sz[0]) << 0)
	default:
		return fmt.Errorf("invalid first string length byte: 0x%x", b0)
	}

	if l > 0 {
		s, err := nextN(r, l)
		if err != nil {
			return err
		}
		// Allocate only after we know there is enough bytes in reader
		if cap(*dst) < l {
			*dst = make([]byte, l)
		} else {
			*dst = (*dst)[:l]
		}
		copy(*dst, s)
	} else {
		*dst = (*dst)[:0]
	}

	if padding := paddingLen(l); padding > 0 {
		if _, err := nextN(r, padding); err != nil {
			return err
		}
	}

	return nil
}

func PeekTag(r *bytes.Buffer) (uint32, error) {
	if r.Len() < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	return binary.LittleEndian.Uint32(r.Bytes()), nil
}

func ReadExactTag(r *bytes.Buffer, tag uint32) error {
	t := uint32(0)
	if err := ReadUint32(r, &t); err != nil {
		return err
	}
	if t != tag {
		return fmt.Errorf("read tag 0x%x instead of 0x%x", t, tag)
	}
	return nil
}

func paddingLen(l int) int {
	if l <= tinyStringLen {
		l++
	}
	return int(-uint(l) % 4)
}

func nextN(r *bytes.Buffer, n int) ([]byte, error) {
	b := r.Next(n)
	if len(b) != n {
		return nil, io.ErrUnexpectedEOF
	}
	return b, nil
}

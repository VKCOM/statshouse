// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package pcache

import "encoding/binary"

type StringValue string

func (s StringValue) MarshalBinary() ([]byte, error) {
	return []byte(s), nil
}

func (s *StringValue) UnmarshalBinary(data []byte) error {
	*s = StringValue(data)
	return nil
}

func StringToValue(s string) Value {
	v := StringValue(s)
	return &v
}

func ValueToString(v Value) string {
	if v == nil {
		return ""
	}
	return string(*v.(*StringValue))
}

type Int32Value int32

func (i Int32Value) MarshalBinary() ([]byte, error) {
	return i32s(int32(i)), nil
}

func (i *Int32Value) UnmarshalBinary(data []byte) error {
	*i = Int32Value(si32(data))
	return nil
}

func Int32ToValue(i int32) Value {
	v := Int32Value(i)
	return &v
}

func ValueToInt32(v Value) int32 {
	if v == nil {
		return 0
	}
	return int32(*v.(*Int32Value))
}

/* If needed - uncomment
type Int64Value int64

func (i Int64Value) MarshalBinary() ([]byte, error) {
	return i64s(int64(i)), nil
}

func (i *Int64Value) UnmarshalBinary(data []byte) error {
	*i = Int64Value(si64(data))
	return nil
}

func Int64ToValue(i int64) Value {
	v := Int64Value(i)
	return &v
}

func ValueToInt64(v Value) int64 {
	if v == nil {
		return 0
	}
	return int64(*v.(*Int64Value))
}
*/

func si32(s []byte) int32 {
	var tmp [4]byte
	copy(tmp[:], s)
	u := binary.LittleEndian.Uint32(tmp[:])
	return int32(u)
}

func i32s(i int32) []byte {
	if i == 0 {
		return nil
	}
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(i))
	return tmp[:]
}

func si64(s []byte) int64 {
	var tmp [8]byte
	copy(tmp[:], s)
	u := binary.LittleEndian.Uint64(tmp[:])
	return int64(u)
}

func i64s(i int64) []byte {
	if i == 0 {
		return nil
	}
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], uint64(i))
	return tmp[:]
}

// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rowbinary

/**
https://clickhouse.yandex/docs/en/formats/rowbinary/
*/

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/hrissan/tdigest"

	pkgUUID "github.com/google/uuid"
)

var (
	encoding = binary.LittleEndian
)

func AppendDateTime(buf []byte, v time.Time) []byte {
	return AppendUint32(buf, uint32(v.Unix()))
}

func AppendDateTime64(buf []byte, v time.Time) []byte {
	return AppendInt64(buf, v.UnixMicro())
}

func AppendBool(buf []byte, v bool) []byte {
	if v {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

func AppendUint8(buf []byte, v uint8) []byte {
	return append(buf, v)
}

func AppendInt8(buf []byte, v int8) []byte {
	return AppendUint8(buf, uint8(v))
}

func AppendUint16(buf []byte, v uint16) []byte {
	var tmp [2]byte
	encoding.PutUint16(tmp[:], v)
	return append(buf, tmp[:]...)
}

func AppendInt16(buf []byte, v int16) []byte {
	return AppendUint16(buf, uint16(v))
}

func AppendUint32(buf []byte, v uint32) []byte {
	var tmp [4]byte
	encoding.PutUint32(tmp[:], v)
	return append(buf, tmp[:]...)
}

func AppendInt32(buf []byte, v int32) []byte {
	return AppendUint32(buf, uint32(v))
}

func AppendUint64(buf []byte, v uint64) []byte {
	var tmp [8]byte
	encoding.PutUint64(tmp[:], v)
	return append(buf, tmp[:]...)
}

func AppendInt64(buf []byte, v int64) []byte {
	var tmp [8]byte
	encoding.PutUint64(tmp[:], uint64(v))
	return append(buf, tmp[:]...)
}

func AppendFloat32(buf []byte, v float32) []byte {
	var tmp [4]byte
	encoding.PutUint32(tmp[:], math.Float32bits(v))
	return append(buf, tmp[:]...)
}

func AppendFloat64(buf []byte, v float64) []byte {
	var tmp [8]byte
	encoding.PutUint64(tmp[:], math.Float64bits(v))
	return append(buf, tmp[:]...)
}

func AppendString(buf []byte, v string) []byte {
	var tmp [12]byte
	n := binary.PutUvarint(tmp[:], uint64(len(v)))
	buf = append(buf, tmp[:n]...)
	return append(buf, v...)
}

func appendNullableType(buf []byte, isNull bool) []byte {
	if isNull {
		return append(buf, 1)
	}
	return append(buf, 0)
}

func AppendNullableString(buf []byte, isNull bool, v string) []byte {
	buf = appendNullableType(buf, isNull)
	return AppendString(buf, v)
}

func AppendEmptyString(buf []byte) []byte {
	return append(buf, 0)
}

func AppendBytes(buf []byte, v []byte) []byte {
	var tmp [12]byte
	n := binary.PutUvarint(tmp[:], uint64(len(v)))
	buf = append(buf, tmp[:n]...)
	return append(buf, v...)
}

func swapBytesForUUID(src []byte) {
	var val uint64
	val = binary.BigEndian.Uint64(src[:8])
	binary.LittleEndian.PutUint64(src[:8], val)
	val = binary.BigEndian.Uint64(src[8:16])
	binary.LittleEndian.PutUint64(src[8:16], val)
}

// Парсит (с помощью github.com/google/uuid) строковое представление UUID ("123e4567-e89b-12d3-a456-426614174000") и добавляет его в буфер
func AppendUUID(buf []byte, uuid string) ([]byte, error) {
	binaryUUID, err := pkgUUID.Parse(uuid)
	if err != nil {
		return buf, err
	}
	swapBytesForUUID(binaryUUID[:])
	return append(buf, binaryUUID[:]...), nil
}

const (
	/*
		В https://github.com/yandex/ClickHouse/blob/112fc71276517adf29939baf1353048e414f4877/dbms/src/AggregateFunctions/QuantileTDigest.h#L82 есть:
		 size_t max_unmerged = 2048;

		В tdigest.go unprocessedSize есть:
		return int(8 * math.Ceil(compression))

		Так что для единообразия задаю тоже самое значение как 2048/8 = 256
	*/
	TDigestCompression = 256
)

// td can be nil
func AppendCentroids(buf []byte, td *tdigest.TDigest, sampleFactor float64) []byte {
	if td == nil {
		return AppendEmptyCentroids(buf)
	}
	centroids := td.Centroids()

	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(centroids.Len()))
	buf = append(buf, tmp[:n]...)
	for _, centroid := range centroids {
		binary.LittleEndian.PutUint32(tmp[:], math.Float32bits(float32(centroid.Mean)))
		buf = append(buf, tmp[:4]...)
		binary.LittleEndian.PutUint32(tmp[:], math.Float32bits(float32(centroid.Weight*sampleFactor)))
		buf = append(buf, tmp[:4]...)
	}
	return buf
}

func AppendEmptyCentroids(buf []byte) []byte {
	return append(buf, 0)
}

func AppendEmptyUnique(buf []byte) []byte {
	return append(buf, 0, 0) // SkipDegree, ItemCount
}

func AppendArgMinMaxStringEmpty(buf []byte) []byte { // same for any string:value type
	return append(buf, 0xFF, 0xFF, 0xFF, 0xFF, 0)
}

// Uses strange encoding. see struct SingleValueDataFixed and struct SingleValueDataString in ClickHouse code
func AppendArgMinMaxStringFloat64(buf []byte, arg string, v float64) []byte {
	var tmp1 [4]byte
	var tmp2 [8]byte
	encoding.PutUint32(tmp1[:], uint32(len(arg)+1)) // string size + 1, or -1 if aggregate is empty
	encoding.PutUint64(tmp2[:], math.Float64bits(v))
	buf = append(buf, tmp1[:]...)
	buf = append(buf, []byte(arg)...)
	buf = append(buf, 0, 1) // string terminator, bool
	return append(buf, tmp2[:]...)
}

func AppendArgMinMaxInt32Float32Empty(buf []byte) []byte { // same for many other types
	return append(buf, 0, 0)
}

func AppendArgMinMaxInt32Float32(buf []byte, arg int32, v float32) []byte { // same for many other types
	var tmp2 [10]byte
	tmp2[0] = 1
	tmp2[5] = 1
	encoding.PutUint32(tmp2[1:], uint32(arg))
	encoding.PutUint32(tmp2[6:], math.Float32bits(v))
	return append(buf, tmp2[:]...)
}

func appendLengthOfArrOrMap(buf []byte, len int) []byte {
	var tmp [binary.MaxVarintLen64]byte
	return append(buf, tmp[:binary.PutUvarint(tmp[:], uint64(len))]...)
}

func AppendArray[V any](buf []byte, in []V, onEachItem func(buf []byte, v V) []byte) []byte {
	buf = appendLengthOfArrOrMap(buf, len(in))
	for _, v := range in {
		buf = onEachItem(buf, v)
	}
	return buf
}

func AppendMap[K comparable, V any](buf []byte, in map[K]V, onEachKey func(buf []byte, k K) []byte, onEachValue func(buf []byte, v V) []byte) []byte {
	buf = appendLengthOfArrOrMap(buf, len(in))
	for k, v := range in {
		buf = onEachKey(buf, k)
		buf = onEachValue(buf, v)
	}
	return buf
}

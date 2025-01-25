package data_model

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/ClickHouse/ch-go/proto"
)

type ColUnique []ChUnique

func (col *ColUnique) Type() proto.ColumnType {
	return "AggregateFunction(uniq, Int64)"
}

func (col *ColUnique) Reset() {
	*col = nil // objects are owned by cache after reading, can not reuse
}

func (col *ColUnique) DecodeColumn(r *proto.Reader, rows int) error {
	var res ColUnique
	if cap(*col) < rows {
		res = make(ColUnique, rows)
	} else {
		res = (*col)[:rows]
	}
	for i := 0; i < len(res); i++ {
		res[i].read(r)
	}
	*col = res
	return nil
}

func (col *ColUnique) Rows() int {
	return len(*col)
}

func (u *ChUnique) read(r *proto.Reader) error {
	u.hasZeroItem = false
	sd, err := r.ReadByte()
	if err != nil {
		return err
	}
	u.skipDegree = uint32(sd)
	ic, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if ic > uniquesHashMaxSize {
		return fmt.Errorf("ChUnique has too many (%d) items", ic)
	}
	u.itemsCount = int32(ic)
	u.sizeDegree = uniquesHashSetInitialSizeDegree
	if ic > 1 {
		u.sizeDegree = uint32(math.Log2(float64(ic)) + 2)
		if u.sizeDegree < uniquesHashSetInitialSizeDegree {
			u.sizeDegree = uniquesHashSetInitialSizeDegree
		}
	}
	bufLen := 1 << u.sizeDegree

	if cap(u.buf) < bufLen {
		u.buf = make([]uint32, bufLen)
	} else {
		u.buf = u.buf[:bufLen]
		for i := range u.buf {
			u.buf[i] = 0
		}
	}

	var tmp [4]byte
	for i := 0; i < int(ic); i++ {
		if err = r.ReadFull(tmp[:]); err != nil {
			return err
		}
		x := binary.LittleEndian.Uint32(tmp[:])
		if x == 0 {
			u.hasZeroItem = true
			continue
		}
		u.reinsertImpl(x)
	}
	return nil
}

type ColTDigest []*TDigest

func (col *ColTDigest) Type() proto.ColumnType {
	return "AggregateFunction(quantilesTDigest(0.5), Float32)"
}

func (col *ColTDigest) Reset() {
	*col = nil // objects are owned by cache after reading, can not reuse
}

func (col *ColTDigest) Rows() int {
	return len(*col)
}

func (col *ColTDigest) DecodeColumn(r *proto.Reader, rows int) error {
	var res ColTDigest
	if cap(*col) < rows {
		res = make(ColTDigest, rows)
	} else {
		res = (*col)[:rows]
	}
	var bs [8]byte
	for i := 0; i < len(res); i++ {
		n, err := binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		if res[i] == nil {
			res[i] = NewWithCompression(256) // clickhouse has compression of 256 by default
		} else {
			res[i].Reset()
		}
		for j := uint64(0); j < n; j++ {
			if err = r.ReadFull(bs[:]); err != nil {
				return err
			}
			res[i].AddCentroid(Centroid{
				Mean:   float64(math.Float32frombits(binary.LittleEndian.Uint32(bs[:4]))),
				Weight: float64(math.Float32frombits(binary.LittleEndian.Uint32(bs[4:]))),
			})
		}
		res[i].MakeSafeForParallelReading()
	}
	(*col) = res
	return nil
}

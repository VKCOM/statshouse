package chutil

import (
	"encoding/binary"
	"math"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/vkcom/statshouse/internal/data_model"
)

type ColUnique []data_model.ChUnique

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
		if err := res[i].ReadFromProto(r); err != nil {
			return err
		}
	}
	*col = res
	return nil
}

func (col *ColUnique) Rows() int {
	return len(*col)
}

type ColTDigest []*data_model.TDigest

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
			res[i] = data_model.NewWithCompression(256) // clickhouse has compression of 256 by default
		} else {
			res[i].Reset()
		}
		for j := uint64(0); j < n; j++ {
			if err = r.ReadFull(bs[:]); err != nil {
				return err
			}
			res[i].AddCentroid(data_model.Centroid{
				Mean:   float64(math.Float32frombits(binary.LittleEndian.Uint32(bs[:4]))),
				Weight: float64(math.Float32frombits(binary.LittleEndian.Uint32(bs[4:]))),
			})
		}
		res[i].MakeSafeForParallelReading()
	}
	(*col) = res
	return nil
}

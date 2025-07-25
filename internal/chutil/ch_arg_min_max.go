package chutil

import (
	"encoding/binary"
	"math"

	"github.com/ClickHouse/ch-go/proto"
)

type ColArgMinInt32Float32 []ArgMinInt32Float32
type ColArgMaxInt32Float32 []ArgMaxInt32Float32

type ArgMinInt32Float32 struct {
	ArgMinMaxInt32Float32
}

type ArgMaxInt32Float32 struct {
	ArgMinMaxInt32Float32
}

type ArgMinMaxInt32Float32 struct {
	Arg int32
	val float32
}

func (col *ColArgMinInt32Float32) DecodeColumn(r *proto.Reader, rows int) error {
	var res ColArgMinInt32Float32
	if cap(*col) < rows {
		res = make(ColArgMinInt32Float32, rows)
	} else {
		res = (*col)[:rows]
	}
	buf := make([]byte, 4)
	for i := 0; i < len(res); i++ {
		res[i].unmarshal(r, buf)
	}
	*col = res
	return nil
}

func (col *ColArgMaxInt32Float32) DecodeColumn(r *proto.Reader, rows int) error {
	var res ColArgMaxInt32Float32
	if cap(*col) < rows {
		res = make(ColArgMaxInt32Float32, rows)
	} else {
		res = (*col)[:rows]
	}
	buf := make([]byte, 4)
	for i := 0; i < len(res); i++ {
		res[i].unmarshal(r, buf)
	}
	*col = res
	return nil
}

func (arg *ArgMinMaxInt32Float32) unmarshal(r *proto.Reader, s []byte) error {
	hasArg, err := r.ReadByte()
	if err != nil {
		return err
	}
	if hasArg != 0 {
		if err := r.ReadFull(s[:4]); err != nil {
			return err
		}
		arg.Arg = int32(binary.LittleEndian.Uint32(s))
	}
	hasVal, err := r.ReadByte()
	if err != nil {
		return err
	}
	if hasVal != 0 {
		if err := r.ReadFull(s[:4]); err != nil {
			return err
		}
		arg.val = math.Float32frombits(binary.LittleEndian.Uint32(s))
	}
	return nil
}

func (col *ColArgMinInt32Float32) Reset() {
	*col = (*col)[:0]
}

func (col *ColArgMaxInt32Float32) Reset() {
	*col = (*col)[:0]
}

func (col *ColArgMinInt32Float32) Rows() int {
	return len(*col)
}

func (col *ColArgMaxInt32Float32) Rows() int {
	return len(*col)
}

func (col *ColArgMinInt32Float32) Type() proto.ColumnType {
	return "AggregateFunction(argMin, Int32, Float32)"
}

func (col *ColArgMaxInt32Float32) Type() proto.ColumnType {
	return "AggregateFunction(argMax, Int32, Float32)"
}

func (arg *ArgMinInt32Float32) Merge(rhs ArgMinInt32Float32) {
	if rhs.val < arg.val {
		*arg = rhs
	}
}

func (arg *ArgMaxInt32Float32) Merge(rhs ArgMaxInt32Float32) {
	if arg.val < rhs.val {
		*arg = rhs
	}
}

func (arg *ArgMinInt32Float32) AsArgMinMaxStringFloat32() ArgMinMaxStringFloat32 {
	return ArgMinMaxStringFloat32{
		AsInt32: arg.Arg,
		val:     arg.val,
	}
}

func (arg *ArgMaxInt32Float32) AsArgMinMaxStringFloat32() ArgMinMaxStringFloat32 {
	return ArgMinMaxStringFloat32{
		AsInt32: arg.Arg,
		val:     arg.val,
	}
}

type ColArgMinStringFloat32 []ArgMinStringFloat32
type ColArgMaxStringFloat32 []ArgMaxStringFloat32

type ArgMinStringFloat32 struct {
	ArgMinMaxStringFloat32
}

type ArgMaxStringFloat32 struct {
	ArgMinMaxStringFloat32
}

type ArgMinMaxStringFloat32 struct {
	Arg     string
	AsInt32 int32
	val     float32
}

func (col *ColArgMinStringFloat32) DecodeColumn(r *proto.Reader, rows int) (err error) {
	var res ColArgMinStringFloat32
	if cap(*col) < rows {
		res = make(ColArgMinStringFloat32, rows)
	} else {
		res = (*col)[:rows]
	}
	buf := make([]byte, 6)
	for i := 0; i < len(res); i++ {
		if buf, err = res[i].unmarshal(r, buf); err != nil {
			return err
		}
	}
	*col = res
	return nil
}

func (col *ColArgMaxStringFloat32) DecodeColumn(r *proto.Reader, rows int) (err error) {
	var res ColArgMaxStringFloat32
	if cap(*col) < rows {
		res = make(ColArgMaxStringFloat32, rows)
	} else {
		res = (*col)[:rows]
	}
	buf := make([]byte, 6)
	for i := 0; i < len(res); i++ {
		if buf, err = res[i].unmarshal(r, buf); err != nil {
			return err
		}
	}
	*col = res
	return nil
}

func (res *ArgMinMaxStringFloat32) unmarshal(r *proto.Reader, tmp []byte) ([]byte, error) {
	// read string
	if err := r.ReadFull(tmp[:4]); err != nil {
		return tmp, err
	}
	if n := int32(binary.LittleEndian.Uint32(tmp)); n > 0 {
		if cap(tmp) < int(n) {
			tmp = make([]byte, n)
		} else {
			tmp = tmp[:n]
		}
		if err := r.ReadFull(tmp); err != nil {
			return tmp, err
		}
		if n == 6 && tmp[0] == 0 {
			res.AsInt32 = int32(binary.LittleEndian.Uint32(tmp[1:]))
		} else {
			res.Arg = string(tmp[1:])
		}
	}
	// read value
	hasValue, err := r.ReadByte()
	if err != nil {
		return tmp, err
	}
	if hasValue != 0 {
		if err := r.ReadFull(tmp[:4]); err != nil {
			return tmp, err
		}
		res.val = math.Float32frombits(binary.LittleEndian.Uint32(tmp))
	}
	return tmp, nil
}

func (col *ColArgMinStringFloat32) Reset() {
	*col = (*col)[:0]
}

func (col *ColArgMaxStringFloat32) Reset() {
	*col = (*col)[:0]
}

func (col *ColArgMinStringFloat32) Rows() int {
	return len(*col)
}

func (col *ColArgMaxStringFloat32) Rows() int {
	return len(*col)
}

func (col *ColArgMinStringFloat32) Type() proto.ColumnType {
	return "AggregateFunction(argMin, String, Float32)"
}

func (col *ColArgMaxStringFloat32) Type() proto.ColumnType {
	return "AggregateFunction(argMax, String, Float32)"
}

func (arg *ArgMinStringFloat32) Merge(rhs ArgMinStringFloat32) {
	if arg.Empty() {
		*arg = rhs
	} else if rhs.val < arg.val {
		*arg = rhs
	}
}

func (arg *ArgMaxStringFloat32) Merge(rhs ArgMaxStringFloat32) {
	if arg.Empty() {
		*arg = rhs
	} else if arg.val < rhs.val {
		*arg = rhs
	}
}

func (arg *ArgMinMaxStringFloat32) Merge(rhs ArgMinMaxStringFloat32, x int) {
	if arg.Empty() {
		*arg = rhs
	} else if x == 0 {
		// min
		if rhs.val < arg.val {
			*arg = rhs
		}
	} else {
		// max
		if arg.val < rhs.val {
			*arg = rhs
		}
	}
}

func (arg *ArgMinMaxStringFloat32) Empty() bool {
	return arg.AsInt32 == 0 && arg.Arg == ""
}

func AppendArgMinMaxStringFloat32(buf []byte, arg string, v float32) []byte {
	return AppendArgMinMaxBytesFloat32(buf, []byte(arg), v)
}

func AppendArgMinMaxBytesFloat32(buf []byte, arg []byte, v float32) []byte {
	var tmp1 [4]byte
	var tmp2 [4]byte
	binary.LittleEndian.PutUint32(tmp1[:], uint32(len(arg)+1)) // string size + 1, or -1 if aggregate is empty
	binary.LittleEndian.PutUint32(tmp2[:], math.Float32bits(v))
	buf = append(buf, tmp1[:]...)
	buf = append(buf, arg...)
	buf = append(buf, 0, 1) // string terminator, bool
	return append(buf, tmp2[:]...)
}

// MarshalBinary serializes ArgMinMaxInt32Float32 to ClickHouse rowbinary format
func (a *ArgMinMaxInt32Float32) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 0, 10)
	if a.Arg != 0 {
		buf = append(buf, 1)
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], uint32(a.Arg))
		buf = append(buf, tmp[:]...)
	} else {
		buf = append(buf, 0)
	}
	if a.val != 0 {
		buf = append(buf, 1)
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], math.Float32bits(a.val))
		buf = append(buf, tmp[:]...)
	} else {
		buf = append(buf, 0)
	}
	return buf, nil
}

// MarshalBinary serializes ArgMinMaxStringFloat32 to ClickHouse rowbinary format
func (a *ArgMinMaxStringFloat32) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 0, 32)
	arg := []byte(a.Arg)
	if a.Arg == "" && a.AsInt32 != 0 {
		arg = make([]byte, 5)
		arg[0] = 0
		binary.LittleEndian.PutUint32(arg[1:], uint32(a.AsInt32))
	}
	buf = AppendArgMinMaxBytesFloat32(buf, arg, a.val)
	return buf, nil
}

// ToStringFormat converts ArgMinMaxInt32Float32 to ArgMinMaxStringFloat32 (for V3)
func (a *ArgMinMaxInt32Float32) ToStringFormat() ArgMinMaxStringFloat32 {
	return ArgMinMaxStringFloat32{
		Arg:     string([]byte{0, byte(a.Arg), byte(a.Arg >> 8), byte(a.Arg >> 16), byte(a.Arg >> 24)}),
		AsInt32: a.Arg,
		val:     a.val,
	}
}

// MarshalBinary for ArgMinStringFloat32/ArgMaxStringFloat32
func (a *ArgMinStringFloat32) MarshalBinary() ([]byte, error) {
	return a.ArgMinMaxStringFloat32.MarshalBinary()
}
func (a *ArgMaxStringFloat32) MarshalBinary() ([]byte, error) {
	return a.ArgMinMaxStringFloat32.MarshalBinary()
}

// UnmarshalArgMinMaxInt32Float32 unmarshals from proto.Reader into ArgMinMaxInt32Float32
func UnmarshalArgMinMaxInt32Float32(r *proto.Reader, v *ArgMinMaxInt32Float32) error {
	return v.unmarshal(r, make([]byte, 4))
}

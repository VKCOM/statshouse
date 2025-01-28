package rowbinary

import "math"

func AppendArgMinMaxStringFloat32(buf []byte, arg string, v float32) []byte {
	return AppendArgMinMaxBytesFloat32(buf, []byte(arg), v)
}

func AppendArgMinMaxBytesFloat32(buf []byte, arg []byte, v float32) []byte {
	var tmp1 [4]byte
	var tmp2 [4]byte
	encoding.PutUint32(tmp1[:], uint32(len(arg)+1)) // string size + 1, or -1 if aggregate is empty
	encoding.PutUint32(tmp2[:], math.Float32bits(v))
	buf = append(buf, tmp1[:]...)
	buf = append(buf, arg...)
	buf = append(buf, 0, 1) // string terminator, bool
	return append(buf, tmp2[:]...)
}

package chutil

import (
	"bytes"
	"math"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

func TestArgMinMaxInt32ToStringConversionRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		arg  int32
		val  float32
	}{
		{"positive", 12345, 1.5},
		{"negative", -67890, 9.8},
		{"zero", 0, 0},
		{"max", math.MaxInt32, 100.0},
		{"min", math.MinInt32, -100.0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// V2 marshal
			v2 := ArgMinMaxInt32Float32{Arg: c.arg, val: c.val}
			v2bin, err := v2.MarshalBinary()
			if err != nil {
				t.Fatalf("marshal v2: %v", err)
			}
			// V2 unmarshal
			v2r := proto.NewReader(bytes.NewReader(v2bin))
			var v2b ArgMinMaxInt32Float32
			err = v2b.unmarshal(v2r, make([]byte, 4))
			if err != nil {
				t.Fatalf("unmarshal v2: %v", err)
			}
			if v2b.Arg != c.arg || v2b.val != c.val {
				t.Errorf("V2 roundtrip mismatch: got %d,%f want %d,%f", v2b.Arg, v2b.val, c.arg, c.val)
			}
			// Convert to V3
			v3 := v2b.ToStringFormat()
			// V3 marshal
			v3bin, err := v3.MarshalBinary()
			if err != nil {
				t.Fatalf("marshal v3: %v", err)
			}
			// V3 unmarshal
			v3r := proto.NewReader(bytes.NewReader(v3bin))
			var v3b ArgMinMaxStringFloat32
			_, err = v3b.unmarshal(v3r, make([]byte, 6))
			if err != nil {
				t.Fatalf("unmarshal v3: %v", err)
			}
			if v3b.val != c.val {
				t.Errorf("V3 value mismatch: got %f want %f", v3b.val, c.val)
			}
			if v3b.AsInt32 != c.arg {
				t.Errorf("V3 int32 mismatch: got %d want %d", v3b.AsInt32, c.arg)
			}
			if len(v3b.Arg) != 0 && v3b.Arg != string([]byte{0, byte(c.arg), byte(c.arg >> 8), byte(c.arg >> 16), byte(c.arg >> 24)}) {
				t.Errorf("V3 string mismatch: got %q", v3b.Arg)
			}
		})
	}
}

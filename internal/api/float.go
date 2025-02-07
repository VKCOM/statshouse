package api

import (
	"math"
	"unsafe"

	"github.com/mailru/easyjson/jlexer"
	"github.com/mailru/easyjson/jwriter"
)

// Float64 is a wrapper type that implements custom marshal and unmarshal for easyjson
// we need it to generate valid JSON without NaN values
type Float64 float64

func (f Float64) MarshalEasyJSON(w *jwriter.Writer) {
	if math.IsNaN(float64(f)) {
		w.RawString("null")
	} else {
		w.Float64(float64(f))
	}
}

func (f *Float64) UnmarshalEasyJSON(l *jlexer.Lexer) {
	if l.IsNull() {
		l.Skip()
		*f = Float64(math.NaN())
	} else {
		*f = Float64(l.Float64())
	}
}

func (f Float64) IsDefined() bool {
	return !math.IsNaN(float64(f))
}

func NaN() Float64 {
	return Float64(math.NaN())
}

// we are allowed to do it because underlying data is same
func FloatSlicePtrToNative(p *[]Float64) *[]float64 {
	return (*[]float64)(unsafe.Pointer(p))
}

// we are allowed to do it because underlying data is same
func FloatSlicePtrFromNative(p *[]float64) *[]Float64 {
	return (*[]Float64)(unsafe.Pointer(p))
}

package api

import (
	"math"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestFloat64_SameSize(t *testing.T) {
	n := float64(3.14)
	w := Float64(3.14)
	assert.Equal(t, unsafe.Sizeof(n), unsafe.Sizeof(w))
}

func TestNaN(t *testing.T) {
	n := NaN()
	assert.True(t, math.IsNaN(float64(n)))
}

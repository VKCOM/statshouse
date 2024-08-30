package data_model

import (
	"math"
	"testing"

	"pgregory.net/rand"
)

// 16 ns/op
func Benchmark_Log2(b *testing.B) {
	b.ReportAllocs()
	sum := 0
	for i := 0; i < b.N; i++ {
		sum += int(math.Log2(float64(1 + i)))
	}
}

// 6 ns/op
func Benchmark_Rng64(b *testing.B) {
	b.ReportAllocs()
	rng := rand.New()
	sum := 0
	for i := 0; i < b.N; i++ {
		sum += int(rng.Uint64n(uint64(i << 32))) // avoid 32-bit fastpath
	}
}

// 6 ns/op
func Benchmark_Rng64_Global(b *testing.B) {
	b.ReportAllocs()
	sum := 0
	for i := 0; i < b.N; i++ {
		sum += int(rand.Uint64n(uint64(i << 32))) // avoid 32-bit fastpath
	}
}

// 3 ns/op
func Benchmark_Rng32(b *testing.B) {
	b.ReportAllocs()
	rng := rand.New()
	sum := 0
	for i := 0; i < b.N; i++ {
		sum += int(rng.Uint64n(uint64(i))) // 32-bit fastpath
	}
}

// 3 ns/op
func Benchmark_Rng32_Global(b *testing.B) {
	b.ReportAllocs()
	sum := 0
	for i := 0; i < b.N; i++ {
		sum += int(rand.Uint64n(uint64(i))) // 32-bit fastpath
	}
}

// 7.5 ns/op
func Benchmark_AddCounterHost(b *testing.B) {
	b.ReportAllocs()
	rng := rand.New()
	item := ItemCounter{}
	for i := 0; i < b.N; i++ {
		item.AddCounterHost(rng, float64(i), int32(i))
	}
}

// 7.1 ns/op
func Benchmark_Merge(b *testing.B) {
	b.ReportAllocs()
	rng := rand.New()
	item := ItemCounter{}
	for i := 0; i < b.N; i++ {
		count := float64(i)
		item.Merge(rng, ItemCounter{count, int32(i)})
	}
}

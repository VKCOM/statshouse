package data_model

import (
	"fmt"
	"math"

	"pgregory.net/rand"
)

// max_host and max_counter_host are very useful mechanisms
// But they are both too deterministic.
// We'd often get single machine with max value for a long period.
// Instead, we decided we want a sample of machines with probability depending on counter
// TODO - for now implemented for max_counter_host, Implement for max_host also!

// We round count, because they are often intergers, but sampling shifts them around true values (0.99, 37.98, etc.)
func CounterHostDistribution(count float64) uint64 {
	// we also tried non-linear distribution but did not like how it works
	// return clampHostDistribution(uint64(1 + FrexpFast(count+0.5))) // max 1024
	return clampHostDistribution(uint64(math.Floor(count + 0.5)))
}

func clampHostDistribution(d uint64) uint64 {
	return max(1, min(d, math.MaxInt64)) // start from 1 so no 0 weights
}

func (s *ItemCounter) AddCounter(count float64) {
	s.counter += count
}

func (s *ItemCounter) AddCounterHost(rng *rand.Rand, count float64, hostTagId int32) {
	// optimization, can be implemented as
	// s.Merge(rng, ItemCounter{count, CounterHostDistribution(count), hostTagId})
	if count <= 0 {
		return
	}
	if s.counter <= 0 {
		s.MaxCounterHostTagId = hostTagId
		s.counter = count
		return
	}
	if s.MaxCounterHostTagId == hostTagId {
		// useful optimization to save rng call on agents where most host tags are 0
		// (and set by aggregator much later)
		s.counter += count
		return
	}
	weight := CounterHostDistribution(s.Count())  // clamped
	otherWeight := CounterHostDistribution(count) // clamped
	totalWeight := weight + otherWeight           // so always fits
	if rng.Uint64n(totalWeight) >= weight {
		s.MaxCounterHostTagId = hostTagId
	}
	s.counter += count
}

func (s *ItemCounter) Merge(rng *rand.Rand, other ItemCounter) {
	if other.counter <= 0 {
		return
	}
	if s.counter <= 0 {
		s.MaxCounterHostTagId = other.MaxCounterHostTagId
		s.counter = other.counter
		return
	}
	if s.MaxCounterHostTagId == other.MaxCounterHostTagId {
		// useful optimization to save rng call on agents where most host tags are 0
		// (and set by aggregator much later)
		s.counter += other.counter
		return
	}
	weight := CounterHostDistribution(s.Count())          // clamped
	otherWeight := CounterHostDistribution(other.counter) // clamped
	totalWeight := weight + otherWeight                   // so always fits
	if rng.Uint64n(totalWeight) >= weight {
		s.MaxCounterHostTagId = other.MaxCounterHostTagId
	}
	s.counter += other.counter
}

func argMaxClickhouse(s *ItemCounter, count float64, hostTagId int32) {
	if count > s.counter {
		s.counter = count
		s.MaxCounterHostTagId = hostTagId
	}
}

func aggregateLocalTest(rng *rand.Rand, perm []int, examples []ItemCounter) (ff ItemCounter, bb ItemCounter, pp ItemCounter) {
	// our distribution must not depend on order of events
	for i, e := range examples {
		ff.AddCounterHost(rng, e.Count(), e.MaxCounterHostTagId)
		b := examples[len(examples)-i-1]
		bb.AddCounterHost(rng, b.Count(), b.MaxCounterHostTagId)
		p := examples[perm[i]]
		pp.AddCounterHost(rng, p.Count(), p.MaxCounterHostTagId)
	}
	return
}

// Motivation - we carefully selected maxCounterHost by distribution by count, but clickhouse only
// has primitive argMax() func, so largest counter will destroy our distribution by replacing everything
// with host corresponding to max counter. Rand gives any row with small counter a chance to win over
// any row with large counter.
// But probability will be very wrong, so we try to transform it in the direction of small counters.
func SkewMaxCounterHost(rng *rand.Rand, count float64) float64 {
	if count <= 0 { // must be never, but check is cheap
		return 0
	}
	return rng.Float64() * math.Log2(1+count)
}

func clickhouseTest(rng *rand.Rand, perm []int, examples []ItemCounter) (ff ItemCounter, bb ItemCounter, pp ItemCounter) {
	// our distribution must not depend on order of events
	for i, e := range examples {
		argMaxClickhouse(&ff, SkewMaxCounterHost(rng, e.Count()), e.MaxCounterHostTagId)
		b := examples[len(examples)-i-1]
		argMaxClickhouse(&bb, SkewMaxCounterHost(rng, b.Count()), b.MaxCounterHostTagId)
		p := examples[perm[i]]
		argMaxClickhouse(&pp, SkewMaxCounterHost(rng, p.Count()), p.MaxCounterHostTagId)
	}
	return
}

func printHistogram(name string, count []int, examples []ItemCounter) {
	fmt.Printf("%s\n", name)
	fmt.Printf("---- real (ideal) ~error\n")
	totalCounter := 0.0
	for _, e := range examples {
		totalCounter += float64(CounterHostDistribution(e.Count()))
	}
	totalEvents := 0
	for _, v := range count {
		totalEvents += v
	}
	for k, v := range count {
		if k == 0 {
			continue
		}
		counter := float64(CounterHostDistribution(examples[k-1].Count()))
		ideal := counter * float64(totalEvents) / totalCounter
		diffPercent := 100 * math.Abs(float64(v)-ideal) / ideal
		fmt.Printf("%d: %6d (%6.1f) ~%2.2f%%\n", k, v, ideal, diffPercent)
	}
}

// Not actual test, it just prints histograms so we can look if they are ok.
func PrintLinearMaxHostProbabilities() {
	examples := []ItemCounter{
		{4, 1},
		{1, 2},
		{1, 3},
		{64, 4},
	}
	rng := rand.New()
	perm := rng.Perm(len(examples))
	countF := make([]int, len(examples)+1)
	countB := make([]int, len(examples)+1)
	countP := make([]int, len(examples)+1)
	for i := 0; i < 1000_000; i++ {
		f, b, p := aggregateLocalTest(rng, perm, examples)
		countF[f.MaxCounterHostTagId]++
		countB[b.MaxCounterHostTagId]++
		countP[p.MaxCounterHostTagId]++
	}
	printHistogram("forward", countF, examples)
	printHistogram("backward", countB, examples)
	printHistogram("perm", countP, examples)
	countF = make([]int, len(examples)+1)
	countB = make([]int, len(examples)+1)
	countP = make([]int, len(examples)+1)
	for i := 0; i < 1000_000; i++ {
		f, b, p := clickhouseTest(rng, perm, examples)
		countF[f.MaxCounterHostTagId]++
		countB[b.MaxCounterHostTagId]++
		countP[p.MaxCounterHostTagId]++
	}
	printHistogram("clickhouse forward", countF, examples)
	printHistogram("clickhouse backward", countB, examples)
	printHistogram("clickhouse perm", countP, examples)
}

// typical output below
//forward
//---- real (ideal) ~error
//1:  57376 (57142.9) ~0.41%
//2:  14198 (14285.7) ~0.61%
//3:  14257 (14285.7) ~0.20%
//4: 914169 (914285.7) ~0.01%
//backward
//---- real (ideal) ~error
//1:  57520 (57142.9) ~0.66%
//2:  14337 (14285.7) ~0.36%
//3:  14368 (14285.7) ~0.58%
//4: 913775 (914285.7) ~0.06%
//perm
//---- real (ideal) ~error
//1:  56750 (57142.9) ~0.69%
//2:  14364 (14285.7) ~0.55%
//3:  14200 (14285.7) ~0.60%
//4: 914686 (914285.7) ~0.04%
//clickhouse forward
//---- real (ideal) ~error
//1: 175301 (57142.9) ~206.78%
//2:  18072 (14285.7) ~26.50%
//3:  17908 (14285.7) ~25.36%
//4: 788719 (914285.7) ~13.73%
//clickhouse backward
//---- real (ideal) ~error
//1: 174723 (57142.9) ~205.77%
//2:  17921 (14285.7) ~25.45%
//3:  17938 (14285.7) ~25.57%
//4: 789418 (914285.7) ~13.66%
//clickhouse perm
//---- real (ideal) ~error
//1: 174911 (57142.9) ~206.09%
//2:  17659 (14285.7) ~23.61%
//3:  17987 (14285.7) ~25.91%
//4: 789443 (914285.7) ~13.65%

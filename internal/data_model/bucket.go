// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"encoding/binary"
	"math"
	"math/bits"
	"sort"
	"unsafe"

	"pgregory.net/rand"

	"github.com/dchest/siphash"
	"github.com/hrissan/tdigest"

	"github.com/vkcom/statshouse/internal/format"
)

const DefaultStringTopCapacity = 100 // if capacity is 0, this one will be used instead

type (
	// Time Series Key, will be optimized to single human-readable string
	Key struct {
		Timestamp uint32
		Metric    int32
		Keys      [format.MaxTags]int32 // Unused keys are set to special 0-value
	}

	ItemValue struct {
		Counter                      float64
		MaxCounterHostTag            int32   // Mapped hostname who provided most of the counter
		ValueMin, ValueMax, ValueSum float64 // Aggregates of Value
		ValueSumSquare               float64 // Aggregates of Value
		MinHostTag, MaxHostTag       int32   // Mapped hostname responsible for ValueMin and ValueMax
		ValueSet                     bool    // first value is assigned to Min&Max
	}

	MultiValue struct {
		Value        ItemValue
		ValueTDigest *tdigest.TDigest // We do not create it until we have at least 1 value to add
		HLL          ChUnique
	}

	// All our items are technically string tops, but most have empty Top map
	MultiItem struct {
		Top              map[string]*MultiValue
		Tail             MultiValue // elements not in top are collected here
		sampleFactorLog2 int
		Capacity         int     // algorithm supports changing on the fly, <2 means DefaultStringTopCapacity
		SF               float64 // set when Marshalling/Sampling
		MetricMeta       *format.MetricMetaValue
	}

	MetricsBucket struct {
		Time       uint32
		Resolution int // for tracking during testing. 0 is merge of all resolutions

		MultiItems map[Key]*MultiItem
	}
)

// Randomly selected, do not change. Which keys go to which shard depends on this.
const sipKeyA = 0x3605bf49d8e3adf2
const sipKeyB = 0xc302580679a8cef2

func (k Key) WithAgentEnvRouteArch(agentEnvTag int32, routeTag int32, buildArchTag int32) Key {
	// when aggregator receives metric from an agent inside another aggregator, those keys are already set,
	// so we simply keep them. AgentEnvTag or RouteTag are always non-zero in this case.
	if k.Keys[format.AgentEnvTag] == 0 {
		k.Keys[format.AgentEnvTag] = agentEnvTag
		k.Keys[format.RouteTag] = routeTag
		k.Keys[format.BuildArchTag] = buildArchTag
	}
	return k
}

func AggKey(t uint32, m int32, k [format.MaxTags]int32, hostTag int32, shardTag int32, replicaTag int32) Key {
	key := Key{Timestamp: t, Metric: m, Keys: k}
	key.Keys[format.AggHostTag] = hostTag
	key.Keys[format.AggShardTag] = shardTag
	key.Keys[format.AggReplicaTag] = replicaTag
	return key
}

func (k *Key) ClearedKeys() Key {
	return Key{Timestamp: k.Timestamp, Metric: k.Metric}
}

func (k *Key) Hash() uint64 {
	a := (*[unsafe.Sizeof(*k)]byte)(unsafe.Pointer(k))
	return siphash.Hash(sipKeyA, sipKeyB, a[4:]) // timestamp is not part of shard
}

func (k *Key) HashSafe() uint64 {
	var b [4 + 4*format.MaxTags]byte
	// timestamp is not part of shard
	binary.LittleEndian.PutUint32(b[:], uint32(k.Metric))
	binary.LittleEndian.PutUint32(b[4+0*4:], uint32(k.Keys[0]))
	binary.LittleEndian.PutUint32(b[4+1*4:], uint32(k.Keys[1]))
	binary.LittleEndian.PutUint32(b[4+2*4:], uint32(k.Keys[2]))
	binary.LittleEndian.PutUint32(b[4+3*4:], uint32(k.Keys[3]))
	binary.LittleEndian.PutUint32(b[4+4*4:], uint32(k.Keys[4]))
	binary.LittleEndian.PutUint32(b[4+5*4:], uint32(k.Keys[5]))
	binary.LittleEndian.PutUint32(b[4+6*4:], uint32(k.Keys[6]))
	binary.LittleEndian.PutUint32(b[4+7*4:], uint32(k.Keys[7]))
	binary.LittleEndian.PutUint32(b[4+8*4:], uint32(k.Keys[8]))
	binary.LittleEndian.PutUint32(b[4+9*4:], uint32(k.Keys[9]))
	binary.LittleEndian.PutUint32(b[4+10*4:], uint32(k.Keys[10]))
	binary.LittleEndian.PutUint32(b[4+11*4:], uint32(k.Keys[11]))
	binary.LittleEndian.PutUint32(b[4+12*4:], uint32(k.Keys[12]))
	binary.LittleEndian.PutUint32(b[4+13*4:], uint32(k.Keys[13]))
	binary.LittleEndian.PutUint32(b[4+14*4:], uint32(k.Keys[14]))
	binary.LittleEndian.PutUint32(b[4+15*4:], uint32(k.Keys[15]))
	const _ = uint(16 - format.MaxTags) // compile time assert to manually add new keys above
	return siphash.Hash(sipKeyA, sipKeyB, b[:])
}

func SimpleItemValue(value float64, count float64, hostTag int32) ItemValue {
	var item ItemValue
	item.AddValueCounterHost(value, count, hostTag)
	return item
}

func SimpleItemCounter(count float64, hostTag int32) ItemValue {
	var item ItemValue
	item.AddCounterHost(count, hostTag)
	return item
}

func (s *ItemValue) AddCounterHost(count float64, hostTag int32) {
	if count > s.Counter {
		s.MaxCounterHostTag = hostTag
	} else if count == s.Counter && bits.OnesCount64(math.Float64bits(count)+uint64(hostTag+s.MaxCounterHostTag))&1 == 0 {
		s.MaxCounterHostTag = hostTag // Motivation - max jitter for equal counts (like 1), so more hosts have chance to appear
	}
	s.Counter += count
}

func (s *ItemValue) AddValue(value float64) {
	s.AddValueCounter(value, 1)
}

func (s *ItemValue) AddValueCounter(value float64, count float64) {
	s.AddValueCounterHost(value, count, 0)
}

func (s *ItemValue) AddValueCounterHost(value float64, count float64, hostTag int32) {
	s.AddCounterHost(count, hostTag)
	s.ValueSum += value * count
	s.ValueSumSquare += value * value * count

	if !s.ValueSet || value < s.ValueMin {
		s.ValueMin = value
		s.MinHostTag = hostTag
	}
	if !s.ValueSet || value > s.ValueMax {
		s.ValueMax = value
		s.MaxHostTag = hostTag
	}
	s.ValueSet = true
}

func (s *ItemValue) AddValueArrayHost(values []float64, mult float64, hostTag int32) {
	for _, value := range values {
		s.AddValueCounterHost(value, mult, hostTag)
	}
}

func (s *ItemValue) TLSizeEstimate() int {
	if s.ValueMin == s.ValueMax { // only min will be saved
		if s.ValueMin == 0 {
			return 8 // counter:double
		}
		return 8 * 2 // counter:double value_min:double
	}
	return 8 * 5 // same as above + value_max:double vale_sum:double value_sum_square:double
}

func (s *ItemValue) Merge(s2 *ItemValue) {
	s.AddCounterHost(s2.Counter, s2.MaxCounterHostTag)
	if !s2.ValueSet {
		return
	}
	s.ValueSum += s2.ValueSum
	s.ValueSumSquare += s2.ValueSumSquare

	if !s.ValueSet || s2.ValueMin < s.ValueMin {
		s.ValueMin = s2.ValueMin
		s.MinHostTag = s2.MinHostTag
	}
	if !s.ValueSet || s2.ValueMax > s.ValueMax {
		s.ValueMax = s2.ValueMax
		s.MaxHostTag = s2.MaxHostTag
	}
	s.ValueSet = true
}

func (b *MetricsBucket) Empty() bool {
	return len(b.MultiItems) == 0
}

func MapKeyItemMultiItem(other *map[Key]*MultiItem, key Key, stringTopCapacity int, metricInfo *format.MetricMetaValue, created *bool) *MultiItem {
	if *other == nil {
		*other = map[Key]*MultiItem{}
	}
	item, ok := (*other)[key]
	if !ok {
		item = &MultiItem{Capacity: stringTopCapacity, SF: 1, MetricMeta: metricInfo}
		(*other)[key] = item
	}
	if created != nil {
		*created = !ok
	}
	return item
}

func (s *MultiItem) MapStringTop(str string, count float64) *MultiValue {
	if len(str) == 0 {
		return &s.Tail
	}
	if s.Top == nil {
		s.Top = map[string]*MultiValue{}
	}
	c, ok := s.Top[str]
	if ok {
		return c
	}
	sf := 1 << s.sampleFactorLog2
	if s.sampleFactorLog2 != 0 && rand.Float64()*float64(sf) >= count { // first cond is optimization
		return &s.Tail
	}
	capacity := s.Capacity
	if capacity < 1 {
		capacity = DefaultStringTopCapacity
	}
	for len(s.Top) >= capacity {
		s.resample()
	}
	c = &MultiValue{}
	s.Top[str] = c
	return c
}

func (s *MultiItem) MapStringTopBytes(str []byte, count float64) *MultiValue {
	if len(str) == 0 {
		return &s.Tail
	}
	if s.Top == nil {
		s.Top = map[string]*MultiValue{}
	}
	c, ok := s.Top[string(str)]
	if ok {
		return c
	}
	sf := 1 << s.sampleFactorLog2
	if s.sampleFactorLog2 != 0 && rand.Float64()*float64(sf) >= count { // first cond is optimization
		return &s.Tail
	}
	capacity := s.Capacity
	if capacity < 1 {
		capacity = DefaultStringTopCapacity
	}
	for len(s.Top) >= capacity {
		s.resample()
	}
	c = &MultiValue{}
	s.Top[string(str)] = c
	return c
}

func (s *MultiItem) resample() {
	for k, v := range s.Top {
		cc := 2 << s.sampleFactorLog2
		if v.Value.Counter >= float64(cc) { // first condition is optimization
			continue
		}
		rv := rand.Intn(cc)
		if v.Value.Counter > float64(rv) {
			continue
		}
		s.Tail.Merge(v)
		delete(s.Top, k)
	}
	s.sampleFactorLog2++
}

type multiItemPair struct {
	k string
	v *MultiValue
}

func (s *MultiItem) FinishStringTop(capacity int) float64 {
	whaleWeight := s.Tail.Value.Counter
	if len(s.Top) == 0 {
		return whaleWeight
	}
	if capacity < 0 {
		capacity = 0 // only prevent panic below
	}
	result := make([]multiItemPair, 0, len(s.Top))
	for k, v := range s.Top {
		result = append(result, multiItemPair{k: k, v: v})
		whaleWeight += v.Value.Counter
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].v.Value.Counter > result[j].v.Value.Counter // We do not need stability by key here
	})
	for i := capacity; i < len(result); i++ {
		s.Tail.Merge(result[i].v)
		delete(s.Top, result[i].k)
	}
	return whaleWeight
}

func (s *MultiItem) Merge(s2 *MultiItem) {
	// TODO - more efficient algorithm?
	for k, v := range s2.Top {
		mi := s.MapStringTop(k, v.Value.Counter)
		mi.Merge(v)
	}
	s.Tail.Merge(&s2.Tail)
}

func (s *MultiItem) RowBinarySizeEstimate() int {
	size := s.Tail.RowBinarySizeEstimate()
	for k, v := range s.Top {
		size += len(k) + v.RowBinarySizeEstimate()
	}
	return size
}

func (s *MultiValue) Empty() bool {
	return s.Value.Counter <= 0
}

func (s *MultiValue) AddCounterHost(count float64, hostTag int32) {
	s.Value.AddCounterHost(count, hostTag)
}

func (s *MultiValue) AddValueCounterHost(value float64, count float64, hostTag int32) {
	s.Value.AddValueCounterHost(value, count, hostTag)
}

func (s *MultiValue) AddValueCounterHostPercentile(value float64, count float64, hostTag int32, compression float64) {
	s.Value.AddValueCounterHost(value, count, hostTag)
	if s.ValueTDigest == nil {
		s.ValueTDigest = tdigest.NewWithCompression(compression)
	}
	s.ValueTDigest.Add(value, count)
}

func (s *MultiValue) AddValueArrayHostPercentile(values []float64, mult float64, hostTag int32, compression float64) {
	s.Value.AddValueArrayHost(values, mult, hostTag)
	if s.ValueTDigest == nil && len(values) != 0 {
		s.ValueTDigest = tdigest.NewWithCompression(compression)
	}
	for _, v := range values {
		s.ValueTDigest.Add(v, mult)
	}
}

func (s *MultiValue) ApplyValues(histogram [][2]float64, values []float64, count float64, totalCount float64, hostTag int32, compression float64, hasPercentiles bool) {
	if totalCount == 0 { // should be never, but as we divide by it, we keep check here
		return
	}
	if s.ValueTDigest == nil && hasPercentiles {
		s.ValueTDigest = tdigest.NewWithCompression(compression)
	}
	sumDiff := float64(0)
	sumSquareDiff := float64(0)
	mult := 1.0 // mult is for TDigest only, we must make multiplication when we Add()
	if count != totalCount {
		mult = count / totalCount
	}
	for _, fv := range values {
		if hasPercentiles {
			s.ValueTDigest.Add(fv, mult)
		}
		sumDiff += fv
		sumSquareDiff += fv * fv
		if !s.Value.ValueSet || fv < s.Value.ValueMin {
			s.Value.ValueMin = fv
			s.Value.MinHostTag = hostTag
		}
		if !s.Value.ValueSet || fv > s.Value.ValueMax {
			s.Value.ValueMax = fv
			s.Value.MaxHostTag = hostTag
		}
		s.Value.ValueSet = true
	}
	for _, kv := range histogram {
		fv := kv[0]
		cc := kv[1]
		if hasPercentiles {
			s.ValueTDigest.Add(fv, mult*cc)
		}
		fvc := fv * cc
		sumDiff += fvc
		sumSquareDiff += fvc * fvc
		if !s.Value.ValueSet || fv < s.Value.ValueMin {
			s.Value.ValueMin = fv
			s.Value.MinHostTag = hostTag
		}
		if !s.Value.ValueSet || fv > s.Value.ValueMax {
			s.Value.ValueMax = fv
			s.Value.MaxHostTag = hostTag
		}
		s.Value.ValueSet = true
	}
	if count == totalCount {
		s.Value.Counter += totalCount
		s.Value.ValueSum += sumDiff
		s.Value.ValueSumSquare += sumSquareDiff
		return
	}
	s.Value.Counter += count
	// values and counter are set, so if we get [1 2 2 100] with count 20, we do not know how many times each item was repeated,
	// so we simply guess they had equal probability.
	s.Value.ValueSum += sumDiff * count / totalCount
	s.Value.ValueSumSquare += sumSquareDiff * count / totalCount
}

func (s *MultiValue) AddUniqueHost(hashes []int64, count float64, hostTag int32) {
	s.AddCounterHost(count, hostTag)
	sumDiff := float64(0)
	sumSquareDiff := float64(0)
	for _, hash := range hashes {
		s.HLL.Insert(uint64(hash))
		fv := float64(hash)
		sumDiff += fv
		sumSquareDiff += fv * fv
		if !s.Value.ValueSet || fv < s.Value.ValueMin {
			s.Value.ValueMin = fv
		}
		if !s.Value.ValueSet || fv > s.Value.ValueMax {
			s.Value.ValueMax = fv
		}
		s.Value.ValueSet = true
	}
	if len(hashes) != 0 {
		// uniques are set, so if we get [1 2 2 100] with count 20, we do not know how many times each item was repeated,
		// so we simply guess they had equal probability
		s.Value.ValueSum += sumDiff * count / float64(len(hashes))
		s.Value.ValueSumSquare += sumSquareDiff * count / float64(len(hashes))
	}
}

func (s *MultiValue) ApplyUnique(hashes []int64, count float64, hostTag int32) {
	if len(hashes) == 0 { // should be never after all usages are from ApplyMetric
		return
	}
	sumDiff := float64(0)
	sumSquareDiff := float64(0)
	for _, hash := range hashes {
		s.HLL.Insert(uint64(hash))
		fv := float64(hash)
		sumDiff += fv
		sumSquareDiff += fv * fv
		if !s.Value.ValueSet || fv < s.Value.ValueMin {
			s.Value.ValueMin = fv
		}
		if !s.Value.ValueSet || fv > s.Value.ValueMax {
			s.Value.ValueMax = fv
		}
		s.Value.ValueSet = true
	}
	if count == 0 {
		s.AddCounterHost(float64(len(hashes)), hostTag)
		s.Value.ValueSum += sumDiff
		s.Value.ValueSumSquare += sumSquareDiff
		return
	}
	s.AddCounterHost(count, hostTag)
	// uniques and counter are set, so if we get [1 2 2 100] with count 20, we do not know how many times each item was repeated,
	// so we simply guess they had equal probability
	s.Value.ValueSum += sumDiff * count
	s.Value.ValueSumSquare += sumSquareDiff * count
	if len(hashes) != 1 {
		s.Value.ValueSum /= float64(len(hashes))
		s.Value.ValueSumSquare /= float64(len(hashes))
	}
}

func (s *MultiValue) Merge(s2 *MultiValue) {
	s.HLL.Merge(s2.HLL)
	if s2.ValueTDigest != nil {
		if s.ValueTDigest == nil {
			s.ValueTDigest = s2.ValueTDigest
		} else {
			s.ValueTDigest.Merge(s2.ValueTDigest)
		}
	}
	s.Value.Merge(&s2.Value)
}

func (s *MultiValue) RowBinarySizeEstimate() int {
	if s.Empty() {
		return 0
	}
	size := 4 + 4 + format.MaxTags*4 + // time, metric, keys
		5*8 + // Aggregates
		1 + 1 + // centroids count byte, unique, string size byte
		10 // max_host
	size += s.HLL.MarshallAppendEstimatedSize()
	if s.ValueTDigest != nil {
		size += 8 * len(s.ValueTDigest.Centroids()) // center, radious
	}
	return size
}

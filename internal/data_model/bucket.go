// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"encoding/binary"
	"sort"

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
		Tags      [format.MaxTags]int32 // Unused tags are set to special 0-value
		sTags     *sTagsHolder          // If no stags are used then nil
	}

	sTagsHolder struct {
		values [format.MaxTags]string
	}

	ItemCounter struct {
		counter             float64
		MaxCounterHostTagId int32 // Mapped example hostname randomized according to distribution
	}

	ItemValue struct {
		ItemCounter
		ValueMin, ValueMax, ValueSum float64 // Aggregates of Value
		ValueSumSquare               float64 // Aggregates of Value
		MinHostTagId, MaxHostTagId   int32   // Mapped hostname responsible for ValueMin and ValueMax
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

func (s *ItemCounter) Count() float64 { return s.counter }

func (k *Key) SetSTag(i int, s string) {
	if k.sTags == nil {
		k.sTags = new(sTagsHolder)
	}
	k.sTags.values[i] = s
}

func (k *Key) GetSTag(i int) string {
	if k.sTags == nil {
		return ""
	}
	return k.sTags.values[i]
}

func (k *Key) STagSlice() []string {
	if k.sTags == nil {
		return []string{}
	}
	result := append([]string{}, k.sTags.values[:]...)
	i := format.MaxTags
	for ; i != 0; i-- {
		if len(result[i-1]) != 0 {
			break
		}
	}
	return result[:i]
}

func (k Key) WithAgentEnvRouteArch(agentEnvTag int32, routeTag int32, buildArchTag int32) Key {
	// when aggregator receives metric from an agent inside another aggregator, those keys are already set,
	// so we simply keep them. AgentEnvTag or RouteTag are always non-zero in this case.
	if k.Tags[format.AgentEnvTag] == 0 {
		k.Tags[format.AgentEnvTag] = agentEnvTag
		k.Tags[format.RouteTag] = routeTag
		k.Tags[format.BuildArchTag] = buildArchTag
	}
	return k
}

func AggKey(t uint32, m int32, k [format.MaxTags]int32, hostTagId int32, shardTag int32, replicaTag int32) Key {
	key := Key{Timestamp: t, Metric: m, Tags: k}
	key.Tags[format.AggHostTag] = hostTagId
	key.Tags[format.AggShardTag] = shardTag
	key.Tags[format.AggReplicaTag] = replicaTag
	return key
}

func (k *Key) Hash() uint64 {
	var b [4 + 4*format.MaxTags]byte
	// timestamp is not part of shard
	binary.LittleEndian.PutUint32(b[:], uint32(k.Metric))
	binary.LittleEndian.PutUint32(b[4+0*4:], uint32(k.Tags[0]))
	binary.LittleEndian.PutUint32(b[4+1*4:], uint32(k.Tags[1]))
	binary.LittleEndian.PutUint32(b[4+2*4:], uint32(k.Tags[2]))
	binary.LittleEndian.PutUint32(b[4+3*4:], uint32(k.Tags[3]))
	binary.LittleEndian.PutUint32(b[4+4*4:], uint32(k.Tags[4]))
	binary.LittleEndian.PutUint32(b[4+5*4:], uint32(k.Tags[5]))
	binary.LittleEndian.PutUint32(b[4+6*4:], uint32(k.Tags[6]))
	binary.LittleEndian.PutUint32(b[4+7*4:], uint32(k.Tags[7]))
	binary.LittleEndian.PutUint32(b[4+8*4:], uint32(k.Tags[8]))
	binary.LittleEndian.PutUint32(b[4+9*4:], uint32(k.Tags[9]))
	binary.LittleEndian.PutUint32(b[4+10*4:], uint32(k.Tags[10]))
	binary.LittleEndian.PutUint32(b[4+11*4:], uint32(k.Tags[11]))
	binary.LittleEndian.PutUint32(b[4+12*4:], uint32(k.Tags[12]))
	binary.LittleEndian.PutUint32(b[4+13*4:], uint32(k.Tags[13]))
	binary.LittleEndian.PutUint32(b[4+14*4:], uint32(k.Tags[14]))
	binary.LittleEndian.PutUint32(b[4+15*4:], uint32(k.Tags[15]))
	const _ = uint(16 - format.MaxTags) // compile time assert to manually add new keys above
	return siphash.Hash(sipKeyA, sipKeyB, b[:])
}

func SimpleItemValue(value float64, count float64, hostTagId int32) ItemValue {
	item := SimpleItemCounter(count, hostTagId)
	item.addOnlyValue(value, count, hostTagId)
	return item
}

func SimpleItemCounter(count float64, hostTagId int32) ItemValue {
	return ItemValue{ItemCounter: ItemCounter{counter: count, MaxCounterHostTagId: hostTagId}}
}

func (s *ItemValue) AddValue(value float64) {
	s.AddValueCounter(value, 1)
}

func (s *ItemValue) addOnlyValue(value float64, count float64, hostTagId int32) {
	s.ValueSum += value * count
	s.ValueSumSquare += value * value * count

	if !s.ValueSet || value < s.ValueMin {
		s.ValueMin = value
		s.MinHostTagId = hostTagId
	}
	if !s.ValueSet || value > s.ValueMax {
		s.ValueMax = value
		s.MaxHostTagId = hostTagId
	}
	s.ValueSet = true
}

func (s *ItemValue) AddValueCounter(value float64, count float64) {
	s.AddCounter(count)
	s.addOnlyValue(value, count, 0)
}

func (s *ItemValue) AddValueCounterHost(rng *rand.Rand, value float64, count float64, hostTagId int32) {
	s.AddCounterHost(rng, count, hostTagId)
	s.addOnlyValue(value, count, hostTagId)
}

func (s *ItemValue) AddValueArrayHost(rng *rand.Rand, values []float64, mult float64, hostTagId int32) {
	s.AddCounterHost(rng, mult*float64(len(values)), hostTagId)
	for _, value := range values {
		s.addOnlyValue(value, mult, hostTagId)
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

func (s *ItemValue) Merge(rng *rand.Rand, s2 *ItemValue) {
	s.ItemCounter.Merge(rng, s2.ItemCounter)
	if !s2.ValueSet {
		return
	}
	s.ValueSum += s2.ValueSum
	s.ValueSumSquare += s2.ValueSumSquare

	if !s.ValueSet || s2.ValueMin < s.ValueMin {
		s.ValueMin = s2.ValueMin
		s.MinHostTagId = s2.MinHostTagId
	}
	if !s.ValueSet || s2.ValueMax > s.ValueMax {
		s.ValueMax = s2.ValueMax
		s.MaxHostTagId = s2.MaxHostTagId
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

func (s *MultiItem) MapStringTop(rng *rand.Rand, str string, count float64) *MultiValue {
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
	if s.sampleFactorLog2 != 0 && rng.Float64()*float64(sf) >= count { // first cond is optimization
		return &s.Tail
	}
	capacity := s.Capacity
	if capacity < 1 {
		capacity = DefaultStringTopCapacity
	}
	for len(s.Top) >= capacity {
		s.resample(rng)
	}
	c = &MultiValue{}
	s.Top[str] = c
	return c
}

func (s *MultiItem) MapStringTopBytes(rng *rand.Rand, str []byte, count float64) *MultiValue {
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
	if s.sampleFactorLog2 != 0 && rng.Float64()*float64(sf) >= count { // first cond is optimization
		return &s.Tail
	}
	capacity := s.Capacity
	if capacity < 1 {
		capacity = DefaultStringTopCapacity
	}
	for len(s.Top) >= capacity {
		s.resample(rng)
	}
	c = &MultiValue{}
	s.Top[string(str)] = c
	return c
}

func (s *MultiItem) resample(rng *rand.Rand) {
	for k, v := range s.Top {
		cc := 2 << s.sampleFactorLog2
		if v.Value.Count() >= float64(cc) { // first condition is optimization
			continue
		}
		rv := rng.Intn(cc)
		if v.Value.Count() > float64(rv) {
			continue
		}
		s.Tail.Merge(rng, v)
		delete(s.Top, k)
	}
	s.sampleFactorLog2++
}

type multiItemPair struct {
	k string
	v *MultiValue
}

func (s *MultiItem) FinishStringTop(rng *rand.Rand, capacity int) float64 {
	whaleWeight := s.Tail.Value.Count()
	if len(s.Top) == 0 {
		return whaleWeight
	}
	if capacity < 0 {
		capacity = 0 // only prevent panic below
	}
	result := make([]multiItemPair, 0, len(s.Top))
	for k, v := range s.Top {
		result = append(result, multiItemPair{k: k, v: v})
		whaleWeight += v.Value.Count()
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].v.Value.Count() > result[j].v.Value.Count() // We do not need stability by key here
	})
	for i := capacity; i < len(result); i++ {
		s.Tail.Merge(rng, result[i].v)
		delete(s.Top, result[i].k)
	}
	return whaleWeight
}

func (s *MultiItem) Merge(rng *rand.Rand, s2 *MultiItem) {
	// TODO - more efficient algorithm?
	for k, v := range s2.Top {
		mi := s.MapStringTop(rng, k, v.Value.Count())
		mi.Merge(rng, v)
	}
	s.Tail.Merge(rng, &s2.Tail)
}

func (s *MultiItem) RowBinarySizeEstimate() int {
	size := s.Tail.RowBinarySizeEstimate()
	for k, v := range s.Top {
		size += len(k) + v.RowBinarySizeEstimate()
	}
	return size
}

func (s *MultiValue) Empty() bool {
	return s.Value.Count() <= 0
}

func (s *MultiValue) AddCounterHost(rng *rand.Rand, count float64, hostTagId int32) {
	s.Value.AddCounterHost(rng, count, hostTagId)
}

func (s *MultiValue) AddValueCounterHost(rng *rand.Rand, value float64, count float64, hostTagId int32) {
	s.Value.AddValueCounterHost(rng, value, count, hostTagId)
}

func (s *MultiValue) AddValueCounterHostPercentile(rng *rand.Rand, value float64, count float64, hostTagId int32, compression float64) {
	s.Value.AddValueCounterHost(rng, value, count, hostTagId)
	if s.ValueTDigest == nil {
		s.ValueTDigest = tdigest.NewWithCompression(compression)
	}
	s.ValueTDigest.Add(value, count)
}

func (s *MultiValue) AddValueArrayHostPercentile(rng *rand.Rand, values []float64, mult float64, hostTagId int32, compression float64) {
	s.Value.AddValueArrayHost(rng, values, mult, hostTagId)
	if s.ValueTDigest == nil && len(values) != 0 {
		s.ValueTDigest = tdigest.NewWithCompression(compression)
	}
	for _, v := range values {
		s.ValueTDigest.Add(v, mult)
	}
}

func (s *MultiValue) ApplyValues(rng *rand.Rand, histogram [][2]float64, values []float64, count float64, totalCount float64, hostTagId int32, compression float64, hasPercentiles bool) {
	if totalCount <= 0 { // should be never, but as we divide by it, we keep check here
		return
	}
	if s.ValueTDigest == nil && hasPercentiles {
		s.ValueTDigest = tdigest.NewWithCompression(compression)
	}
	mult := 1.0
	// mult is for TDigest only, we must make multiplication when we Add()
	// mult can be 0.3333333333, so we divide our sums by totalCount, not multiply by mult
	if count != totalCount {
		mult = count / totalCount
	}
	tmp := SimpleItemCounter(count, hostTagId)
	for _, fv := range values {
		if hasPercentiles {
			s.ValueTDigest.Add(fv, mult)
		}
		tmp.addOnlyValue(fv, 1, hostTagId)
	}
	for _, kv := range histogram {
		fv := kv[0]
		cc := kv[1]
		if hasPercentiles {
			s.ValueTDigest.Add(fv, mult*cc)
		}
		tmp.addOnlyValue(fv, cc, hostTagId)
	}
	if count != totalCount {
		tmp.ValueSum *= count
		tmp.ValueSumSquare *= count
		if totalCount != 1 { // optimization
			tmp.ValueSum /= totalCount // clean division by, for example 3
			tmp.ValueSumSquare /= totalCount
		}
	}
	s.Value.Merge(rng, &tmp)
}

func (s *MultiValue) ApplyUnique(rng *rand.Rand, hashes []int64, count float64, hostTagId int32) {
	totalCount := float64(len(hashes))
	if totalCount <= 0 { // should be never, but as we divide by it, we keep check here
		return
	}
	tmp := SimpleItemCounter(count, hostTagId)
	for _, hash := range hashes {
		s.HLL.Insert(uint64(hash))
		fv := float64(hash) // hashes are also values
		tmp.addOnlyValue(fv, 1, hostTagId)
	}
	// if both uniques and counter are set, we might get unique [1 2 2 100] with count 20,
	// We do not know how many times each item was repeated,
	// So we simply guess they had equal probability
	if count != totalCount {
		tmp.ValueSum *= count
		tmp.ValueSumSquare *= count
		if totalCount != 1 { // optimization
			tmp.ValueSum /= totalCount // clean division by, for example 3
			tmp.ValueSumSquare /= totalCount
		}
	}
	s.Value.Merge(rng, &tmp)
}

func (s *MultiValue) Merge(rng *rand.Rand, s2 *MultiValue) {
	s.HLL.Merge(s2.HLL)
	if s2.ValueTDigest != nil {
		if s.ValueTDigest == nil {
			s.ValueTDigest = s2.ValueTDigest
		} else {
			s.ValueTDigest.Merge(s2.ValueTDigest)
		}
	}
	s.Value.Merge(rng, &s2.Value)
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

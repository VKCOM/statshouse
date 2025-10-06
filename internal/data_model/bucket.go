// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"bytes"
	"encoding/binary"
	"sort"
	"unsafe"

	"pgregory.net/rand"

	"github.com/hrissan/tdigest"
	"github.com/zeebo/xxh3"

	"github.com/VKCOM/statshouse/internal/format"
)

const DefaultStringTopCapacity = 100 // if capacity is 0, this one will be used instead
const oldTagNumber = 16

type (
	TagUnion struct {
		S string
		I int32
	}
	TagUnionBytes struct {
		S []byte
		I int32
	}

	// Time Series Key, will be optimized to single human-readable string
	Key struct {
		Timestamp uint32
		Metric    int32
		Tags      [format.MaxTags]int32  // Unused tags are set to special 0-value
		STags     [format.MaxTags]string // Unused stags are set to empty string
	}

	ItemCounter struct {
		counter           float64
		MaxCounterHostTag TagUnionBytes // Example hostname randomized according to distribution
	}

	ItemValue struct {
		ItemCounter
		ValueMin, ValueMax, ValueSum float64 // Aggregates of Value
		ValueSumSquare               float64 // Aggregates of Value
		MinHostTag                   TagUnionBytes
		MaxHostTag                   TagUnionBytes
		ValueSet                     bool // first value is assigned to Min&Max
	}

	MultiValue struct {
		Value        ItemValue
		ValueTDigest *tdigest.TDigest // We do not create it on agent until we have at least 2 different values
		HLL          ChUnique
	}

	// All our items are technically string tops, but most have empty Top map
	MultiItem struct {
		Key              Key
		Top              map[TagUnion]*MultiValue
		Tail             MultiValue // elements not in top are collected here
		sampleFactorLog2 int
		SF               float64 // set when Marshalling/Sampling
		MetricMeta       *format.MetricMetaValue
		WeightMultiplier int // Temporary weight boost if all metric rows is written to single shard. Can be 1 or NumShards.
	}

	MetricsBucket struct {
		Time uint32

		MultiItemMap
	}

	MultiItemMap struct {
		MultiItems map[string]*MultiItem // string is unsafe and points to part of the keysBuffer
		keysBuffer []byte
	}
)

func (t TagUnionBytes) Equal(rhs TagUnionBytes) bool {
	if t.I != 0 || rhs.I != 0 {
		return t.I == rhs.I
	}
	return bytes.Equal(t.S, rhs.S)
}

func (t TagUnionBytes) Empty() bool {
	return t.I == 0 && len(t.S) == 0
}

func (s *ItemCounter) Count() float64 { return s.counter }

func (k *Key) SetSTag(i int, s string) {
	k.STags[i] = s
}

func (k *Key) GetSTag(i int) string {
	return k.STags[i]
}

func (k *Key) MarshalAppend(buffer []byte) (updatedBuffer []byte, newKey []byte) {
	// compile time assert to ensure that 1 byte is enough for tags count
	const _ = uint(255 - len(k.Tags))
	const _ = uint(255 - len(k.STags))
	tagsCount := len(k.Tags) // ignore empty tags
	for ; tagsCount > 0 && k.Tags[tagsCount-1] == 0; tagsCount-- {
	}
	stagsCount := len(k.STags) // ignore empty stags
	for ; stagsCount > 0 && len(k.STags[stagsCount-1]) == 0; stagsCount-- {
	}
	stagsSize := 0
	for i := 0; i < stagsCount; i++ {
		stagsSize += len(k.STags[i]) + 1 // zero terminated
	}
	// ts 4b + metric 4b + #tags 1b + tags 4b each + #stags 1b + stags zero term strings from 1 to 129 bytes each
	updatedBuffer = append(buffer, make([]byte, 4+4+1+tagsCount*4+1+stagsSize)...)
	newKey = updatedBuffer[len(buffer):]
	binary.LittleEndian.PutUint32(newKey[0:], k.Timestamp)
	binary.LittleEndian.PutUint32(newKey[4:], uint32(k.Metric))
	newKey[8] = byte(tagsCount)
	const tagsPos = 9
	for i := 0; i < tagsCount; i++ {
		binary.LittleEndian.PutUint32(newKey[tagsPos+i*4:], uint32(k.Tags[i]))
	}
	newKey[tagsPos+tagsCount*4] = byte(stagsCount)
	stagsPos := tagsPos + tagsCount*4
	for i := 0; i < stagsCount; i++ {
		copy(newKey[stagsPos:], k.STags[i])
		stagsPos += len(k.STags[i])
		newKey[stagsPos] = 0
		stagsPos += 1
	}
	return
}

func AggKey(t uint32, m int32, k [format.MaxTags]int32, hostTagId int32, shardTag int32, replicaTag int32) *Key {
	key := Key{Timestamp: t, Metric: m, Tags: k}
	key.Tags[format.AggHostTag] = hostTagId
	key.Tags[format.AggShardTag] = shardTag
	key.Tags[format.AggReplicaTag] = replicaTag
	return &key
}

// returns possibly reallocated scratch
func (k *Key) XXHash(scratch []byte) ([]byte, uint64) {
	scratch, _ = k.MarshalAppend(scratch[:0])
	return scratch, xxh3.Hash(scratch[4:]) // skip timestamp in first 4 bytes
}

func SimpleItemValue(value float64, count float64, hostTag TagUnionBytes) ItemValue {
	item := SimpleItemCounter(count, hostTag)
	item.addOnlyValue(value, count, hostTag)
	return item
}

func SimpleItemCounter(count float64, hostTag TagUnionBytes) ItemValue {
	return ItemValue{ItemCounter: ItemCounter{counter: count, MaxCounterHostTag: hostTag}}
}

func (s *ItemValue) AddValue(value float64) {
	s.AddValueCounter(value, 1)
}

func (s *ItemValue) addOnlyValue(value float64, count float64, hostTag TagUnionBytes) {
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

func (s *ItemValue) AddValueCounter(value float64, count float64) {
	s.AddCounter(count)
	s.addOnlyValue(value, count, TagUnionBytes{})
}

func (s *ItemValue) AddValueCounterHost(rng *rand.Rand, value float64, count float64, hostTag TagUnionBytes) {
	s.AddCounterHost(rng, count, hostTag)
	s.addOnlyValue(value, count, hostTag)
}

func (s *ItemValue) AddValueArrayHost(rng *rand.Rand, values []float64, mult float64, hostTag TagUnionBytes) {
	s.AddCounterHost(rng, mult*float64(len(values)), hostTag)
	for _, value := range values {
		s.addOnlyValue(value, mult, hostTag)
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

func (b *MultiItemMap) GetOrCreateMultiItem(key *Key, metricInfo *format.MetricMetaValue, weightMul int, keyBytes []byte) (item *MultiItem, created bool) {
	//if key.Timestamp == 0 { // TODO - remove check before merge to master
	//	fmt.Printf("key: %v\n", *key)
	//	panic("timestamp must be always set at this point of conveyor")
	//}
	if b.MultiItems == nil {
		b.MultiItems = make(map[string]*MultiItem)
	}
	wasLen := len(b.keysBuffer)
	if len(keyBytes) > 0 { // no need to marshall since we already have result
		b.keysBuffer = append(b.keysBuffer, keyBytes...)
		keyBytes = b.keysBuffer[wasLen:] // we want unsafe pointer into b.keysBuffer
	} else {
		b.keysBuffer, keyBytes = key.MarshalAppend(b.keysBuffer)
	}
	keyString := unsafe.String(unsafe.SliceData(keyBytes), len(keyBytes))
	// Strings in map are unsafe references to bytes of b.keysBuffer.
	// We promise to never change those bytes, but also we must prevent those strings spreading.
	// We must periodically check that no iteration over MultiItems retains keys (!).
	// After b.keysBuffer is reallocated, all strings in map point to previous keysBuffer, and
	// no strings will ever point to the first part of new keysBuffer.
	// This is 50% efficient, but becomes 100% efficient as soon as MultiItemMap starts to be reused.
	item, ok := b.MultiItems[keyString]
	created = !ok
	if ok {
		b.keysBuffer = b.keysBuffer[:wasLen]
		return
	}
	item = &MultiItem{Key: *key, SF: 1, MetricMeta: metricInfo, WeightMultiplier: weightMul}
	b.MultiItems[keyString] = item
	return
}

func (b *MultiItemMap) DeleteMultiItem(key *Key) {
	if b.MultiItems == nil {
		return
	}
	var keyBytes []byte
	b.keysBuffer, keyBytes = key.MarshalAppend(b.keysBuffer)
	keyString := unsafe.String(unsafe.SliceData(keyBytes), len(keyBytes))
	delete(b.MultiItems, keyString)
	b.keysBuffer = b.keysBuffer[:len(b.keysBuffer)-len(keyBytes)]
	// we do not clean keysBuffer, it has same lifetime as b and should be reused
}

func (s *MultiItem) MapStringTop(rng *rand.Rand, capacity int, tag TagUnion, count float64) *MultiValue {
	if len(tag.S) == 0 && tag.I == 0 {
		return &s.Tail
	}
	if s.Top == nil {
		s.Top = map[TagUnion]*MultiValue{}
	}
	c, ok := s.Top[tag]
	if ok {
		return c
	}
	sf := 1 << s.sampleFactorLog2
	if s.sampleFactorLog2 != 0 && rng.Float64()*float64(sf) >= count { // first cond is optimization
		return &s.Tail
	}
	if capacity < 1 {
		capacity = DefaultStringTopCapacity
	}
	for len(s.Top) >= capacity {
		s.resample(rng)
	}
	c = &MultiValue{}
	s.Top[tag] = c
	return c
}

func (s *MultiItem) MapStringTopBytes(rng *rand.Rand, capacity int, tag TagUnionBytes, count float64) *MultiValue {
	if len(tag.S) == 0 && tag.I == 0 {
		return &s.Tail
	}
	if s.Top == nil {
		s.Top = map[TagUnion]*MultiValue{}
	}
	c, ok := s.Top[TagUnion{S: string(tag.S), I: tag.I}]
	if ok {
		return c
	}
	sf := 1 << s.sampleFactorLog2
	if s.sampleFactorLog2 != 0 && rng.Float64()*float64(sf) >= count { // first cond is optimization
		return &s.Tail
	}
	if capacity < 1 {
		capacity = DefaultStringTopCapacity
	}
	for len(s.Top) >= capacity {
		s.resample(rng)
	}
	c = &MultiValue{}
	s.Top[TagUnion{S: string(tag.S), I: tag.I}] = c
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
	k TagUnion
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

//func (s *MultiItem) Merge(rng *rand.Rand, s2 *MultiItem) {
//	// TODO - more efficient algorithm?
//	for k, v := range s2.Top {
//		mi := s.MapStringTop(rng, k, v.Value.Count())
//		mi.Merge(rng, v)
//	}
//	s.Tail.Merge(rng, &s2.Tail)
//}

func (s *MultiItem) RowBinarySizeEstimate() int {
	// we don't want sampling to jump, so we keep assuming that key is still 16 tags
	keySize := 4 + 4 + oldTagNumber*4 // time, metric, tags
	for _, st := range s.Key.STags {  // stags
		keySize += len(st)
	}
	size := keySize + s.Tail.RowBinarySizeEstimate()
	for k, v := range s.Top {
		size += keySize + 4 + len(k.S) + v.RowBinarySizeEstimate()
	}
	return size
}

func (s *MultiItem) isSingleValueCounter() bool {
	switch len(s.Top) {
	case 0:
		return s.Tail.isSingleValueCounter()
	case 1:
		if s.Tail.Empty() {
			for _, v := range s.Top {
				if v.isSingleValueCounter() {
					return true
				}
			}
		}
	}
	return false
}

func (s *MultiValue) isSingleValueCounter() bool {
	return s.ValueTDigest == nil && s.HLL.ItemsCount() == 0
}

func (s *MultiValue) Empty() bool {
	return s.Value.Count() <= 0
}

func (s *MultiValue) AddCounter(rng *rand.Rand, count float64) {
	s.Value.AddCounterHost(rng, count, TagUnionBytes{})
}

func (s *MultiValue) AddCounterHost(rng *rand.Rand, count float64, hostTag TagUnionBytes) {
	s.Value.AddCounterHost(rng, count, hostTag)
}

func (s *MultiValue) AddValueCounter(rng *rand.Rand, value float64, count float64) {
	s.Value.AddValueCounterHost(rng, value, count, TagUnionBytes{})
}

func (s *MultiValue) AddValueCounterHost(rng *rand.Rand, value float64, count float64, hostTag TagUnionBytes) {
	s.Value.AddValueCounterHost(rng, value, count, hostTag)
}

func (s *MultiValue) AddValueCounterHostPercentile(rng *rand.Rand, value float64, count float64, hostTag TagUnionBytes, compression float64) {
	wasValue, wasCount, wasSet := s.Value.ValueMax, s.Value.counter, s.Value.ValueSet
	s.Value.AddValueCounterHost(rng, value, count, hostTag)
	if s.Value.ValueMin == s.Value.ValueMax {
		return // all values still identical, no TDigest needed
	}
	if s.ValueTDigest == nil {
		s.ValueTDigest = tdigest.NewWithCompression(compression)
		if wasSet { // must be always, unless float assignment breaks equality
			s.ValueTDigest.Add(wasValue, wasCount)
		}
	}
	s.ValueTDigest.Add(value, count)
}

// for tests between existing and new percentiles transfer
func (s *MultiValue) AddValueCounterHostPercentileLegacy(rng *rand.Rand, value float64, count float64, hostTag TagUnionBytes, compression float64) {
	s.Value.AddValueCounterHost(rng, value, count, hostTag)
	if s.ValueTDigest == nil {
		s.ValueTDigest = tdigest.NewWithCompression(compression)
	}
	s.ValueTDigest.Add(value, count)
}

func (s *MultiValue) ApplyValues(rng *rand.Rand, histogram [][2]float64, values []float64, count float64, totalCount float64, hostTag TagUnionBytes, compression float64, hasPercentiles bool) {
	if totalCount <= 0 { // should be never, but as we divide by it, we keep check here
		return
	}
	tmp := SimpleItemCounter(count, hostTag)
	// we aggregate into tmp first, then merge because we want single expensive rand() call, and less noise in by host distribution
	for _, fv := range values {
		tmp.addOnlyValue(fv, 1, hostTag)
	}
	for _, kv := range histogram {
		fv := kv[0]
		cc := kv[1]
		tmp.addOnlyValue(fv, cc, hostTag)
	}
	if count != totalCount {
		tmp.ValueSum *= count
		tmp.ValueSumSquare *= count
		if totalCount != 1 { // optimization
			tmp.ValueSum /= totalCount // clean division by, for example 3
			tmp.ValueSumSquare /= totalCount
		}
	}
	wasValue, wasCount, wasSet := s.Value.ValueMax, s.Value.counter, s.Value.ValueSet
	s.Value.Merge(rng, &tmp)
	if !hasPercentiles {
		return
	}
	if s.Value.ValueMin == s.Value.ValueMax {
		return // all values still identical, no TDigest needed
	}
	if s.ValueTDigest == nil {
		s.ValueTDigest = tdigest.NewWithCompression(compression)
		if wasSet { // must be always, unless float assignment breaks equality
			s.ValueTDigest.Add(wasValue, wasCount)
		}
	}
	mult := 1.0
	// mult is for TDigest only, we must make multiplication when we Add()
	// mult can be 0.3333333333, so we divide our sums above by totalCount, not multiply by mult
	if count != totalCount {
		mult = count / totalCount
	}
	for _, fv := range values {
		s.ValueTDigest.Add(fv, mult)
	}
	for _, kv := range histogram {
		fv := kv[0]
		cc := kv[1]
		s.ValueTDigest.Add(fv, mult*cc)
	}
}

// for tests between existing and new percentiles transfer
func (s *MultiValue) ApplyValuesLegacy(rng *rand.Rand, histogram [][2]float64, values []float64, count float64, totalCount float64, hostTag TagUnionBytes, compression float64, hasPercentiles bool) {
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
	tmp := SimpleItemCounter(count, hostTag)
	for _, fv := range values {
		if hasPercentiles {
			s.ValueTDigest.Add(fv, mult)
		}
		tmp.addOnlyValue(fv, 1, hostTag)
	}
	for _, kv := range histogram {
		fv := kv[0]
		cc := kv[1]
		if hasPercentiles {
			s.ValueTDigest.Add(fv, mult*cc)
		}
		tmp.addOnlyValue(fv, cc, hostTag)
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

func (s *MultiValue) ApplyUnique(rng *rand.Rand, hashes []int64, count float64, hostTag TagUnionBytes) {
	totalCount := float64(len(hashes))
	if totalCount <= 0 { // should be never, but as we divide by it, we keep check here
		return
	}
	tmp := SimpleItemCounter(count, hostTag)
	for _, hash := range hashes {
		s.HLL.Insert(uint64(hash))
		fv := float64(hash) // hashes are also values
		tmp.addOnlyValue(fv, 1, hostTag)
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
	size := 5*8 + // Aggregates
		1 + 1 + // centroids count byte, unique, string size byte
		10 // max_host
	size += s.HLL.MarshallAppendEstimatedSize()
	if s.ValueTDigest != nil {
		size += 8 * len(s.ValueTDigest.Centroids()) // center, radious
	}
	return size
}

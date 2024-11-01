// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"bytes"
	"math"

	"github.com/hrissan/tdigest"
	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
)

func (k *Key) TagSlice() []int32 {
	result := append([]int32{}, k.Tags[:]...)
	i := format.MaxTags
	for ; i != 0; i-- {
		if result[i-1] != 0 {
			break
		}
	}
	return result[:i]
}

func KeyFromStatshouseMultiItem(item *tlstatshouse.MultiItemBytes, bucketTimestamp uint32, newestTime uint32) (key Key, shardID int) {
	// We use high byte of fieldsmask to pass shardID to aggregator, otherwise it is too much work for CPU
	sID := item.FieldsMask >> 24
	key.Timestamp = bucketTimestamp
	if item.IsSetT() {
		key.Timestamp = item.T
		// sometimes if agent conveyor is stuck or if it is on machine with wrong clock but receives events from
		// client with correct clock, item timestamp will be > agent bucketTimestamp, we should not clamp by it.
		// instead we use aggregator clock which we demand to always be set correctly.
		if key.Timestamp > newestTime {
			key.Timestamp = newestTime
		} else if bucketTimestamp > BelieveTimestampWindow && key.Timestamp < bucketTimestamp-BelieveTimestampWindow {
			key.Timestamp = bucketTimestamp - BelieveTimestampWindow
		}
		// above checks can be moved below }, but they will always be NOP as bucketTimestamp is both <= newestTime and in beleive window
	}
	key.Metric = item.Metric
	copy(key.Tags[:], item.Keys)
	if item.IsSetSkeys() {
		for i := range item.Skeys {
			key.SetSTag(i, string(item.Skeys[i]))
		}
	}
	return key, int(sID)
}

func (k *Key) TLSizeEstimate(defaultTimestamp uint32) int {
	sz := 4 // field mask
	i := format.MaxTags
	for ; i != 0; i-- {
		if k.Tags[i-1] != 0 {
			break
		}
	}
	sz += 4 + 4 + 4*i // metric, # of tags, tags
	i = format.MaxTags
	for ; i != 0; i-- {
		if len(k.GetSTag(i-1)) != 0 {
			break
		}
	}
	// we send stags only if we have at least one
	if i > 0 {
		sz += 4 // # of stags
		for ; i != 0; i-- {
			l := 1 + len(k.GetSTag(i-1)) // stag len + data
			l += (4 - l%4) % 4           // align to 4 bytes
			sz += l
		}
	}
	if k.Timestamp != 0 && k.Timestamp != defaultTimestamp {
		sz += 4 // timestamp
	}
	return sz
}

func (k *Key) TLMultiItemFromKey(defaultTimestamp uint32) tlstatshouse.MultiItem {
	item := tlstatshouse.MultiItem{
		Metric: k.Metric,
		Keys:   k.TagSlice(),
	}
	stags := k.STagSlice()
	if len(stags) > 0 {
		item.SetSkeys(stags)
	}
	// TODO - check that timestamp is never 0 here
	if k.Timestamp != 0 && k.Timestamp != defaultTimestamp {
		item.SetT(k.Timestamp)
	}
	item.FieldsMask |= uint32(k.Hash()%AggregationShardsPerSecond) << 24
	return item
}

func (s *MultiValue) TLSizeEstimate() int {
	sz := 8 // counter without considering 0 and 1 optimizations
	if s.Value.MaxHostTagId != 0 {
		sz += 4
	}
	if s.Value.MinHostTagId != s.Value.MaxHostTagId {
		sz += 4
	}
	if s.Value.MaxCounterHostTagId != s.Value.MaxHostTagId {
		sz += 4
	}
	if s.HLL.ItemsCount() != 0 {
		sz += s.HLL.MarshallAppendEstimatedSize()
	}
	if s.ValueTDigest != nil { // without considering 0 centroids case
		sz += 4 + 8*len(s.ValueTDigest.Centroids())
	}
	if !s.Value.ValueSet {
		return sz
	}
	if s.Value.ValueMin != 0 {
		sz += 8
	}
	if s.Value.ValueMin != s.Value.ValueMax {
		sz += 3 * 8
	}
	return sz
}

func (s *MultiValue) MultiValueToTL(item *tlstatshouse.MultiValue, sampleFactor float64, fieldsMask *uint32, marshalBuf *[]byte) {
	cou := s.Value.Count() * sampleFactor
	if cou <= 0 {
		return
	}
	// host tags are passed from "_h" tag (if set) in ApplyValue, ApplyUnique, ApplyCount functions
	if s.Value.MaxHostTagId != 0 {
		item.SetMaxHostTag(s.Value.MaxHostTagId, fieldsMask)
	}
	if s.Value.MinHostTagId != s.Value.MaxHostTagId {
		item.SetMinHostTag(s.Value.MinHostTagId, fieldsMask)
	}
	if s.Value.MaxCounterHostTagId != s.Value.MaxHostTagId {
		item.SetMaxCounterHostTag(s.Value.MaxCounterHostTagId, fieldsMask)
	}
	if s.HLL.ItemsCount() != 0 {
		*marshalBuf = s.HLL.MarshallAppend((*marshalBuf)[:0])
		item.SetUniques(string(*marshalBuf), fieldsMask)
	}
	if s.ValueTDigest != nil {
		var cc []tlstatshouse.CentroidFloat
		for _, c := range s.ValueTDigest.Centroids() {
			cc = append(cc, tlstatshouse.CentroidFloat{Value: float32(c.Mean), Count: float32(c.Weight * sampleFactor)})
		}
		if len(cc) != 0 { // empty centroids is ordinary value
			item.SetCentroids(cc, fieldsMask) // TODO - do not set percentiles if v.ValueMin == v.ValueMax, restore on other side
		}
	}
	if cou == 1 {
		item.SetCounterEq1(true, fieldsMask)
	} else {
		item.SetCounter(cou, fieldsMask)
	}
	if !s.Value.ValueSet {
		return
	}
	item.SetValueSet(true, fieldsMask)
	if s.Value.ValueMin != 0 {
		item.SetValueMin(s.Value.ValueMin, fieldsMask)
	}
	if s.Value.ValueMin != s.Value.ValueMax {
		item.SetValueMax(s.Value.ValueMax, fieldsMask)
		item.SetValueSum(s.Value.ValueSum*sampleFactor, fieldsMask)
		item.SetValueSumSquare(s.Value.ValueSumSquare*sampleFactor, fieldsMask)
	}
}

func (s *ItemValue) MergeWithTLItem2(rng *rand.Rand, s2 *tlstatshouse.MultiValueBytes, fields_mask uint32) {
	counter := float64(0)
	if s2.IsSetCounterEq1(fields_mask) {
		counter = 1
	}
	if s2.IsSetCounter(fields_mask) {
		counter = s2.Counter
	}
	if counter <= 0 || math.IsNaN(counter) { // sanity check/check for empty String Top tail
		return
	}
	if counter > math.MaxFloat32 { // agents do similar check, but this is so cheap, we repeat on aggregators.
		counter = math.MaxFloat32
	}
	s.AddCounterHost(rng, counter, s2.MaxCounterHostTag)
	if !s2.IsSetValueSet(fields_mask) {
		return
	}
	// We do not care checking values for NaN/Inf here
	if !s2.IsSetValueMax(fields_mask) {
		s2.ValueSum = s2.ValueMin * counter
		s2.ValueSumSquare = s2.ValueSum * s2.ValueMin
		s2.ValueMax = s2.ValueMin
	}
	s.ValueSum += s2.ValueSum
	s.ValueSumSquare += s2.ValueSumSquare

	if !s.ValueSet || s2.ValueMin < s.ValueMin {
		s.ValueMin = s2.ValueMin
		s.MinHostTagId = s2.MinHostTag
	}
	if !s.ValueSet || s2.ValueMax > s.ValueMax {
		s.ValueMax = s2.ValueMax
		s.MaxHostTagId = s2.MaxHostTag
	}
	s.ValueSet = true
}

func (s *MultiItem) MergeWithTLMultiItem(rng *rand.Rand, s2 *tlstatshouse.MultiItemBytes, hostTagId int32) {
	for _, v := range s2.Top {
		mi := s.MapStringTopBytes(rng, v.Key, v.Value.Counter)
		v.Key, _ = format.AppendValidStringValue(v.Key[:0], v.Key) // TODO - report this error via builtin metrics
		// we want to validate all incoming strings. In case of encoding error, v.Key will be truncated to 0
		mi.MergeWithTL2(rng, &v.Value, v.FieldsMask, hostTagId, AggregatorPercentileCompression)
	}
	s.Tail.MergeWithTL2(rng, &s2.Tail, s2.FieldsMask, hostTagId, AggregatorPercentileCompression)
}

func (s *MultiItem) TLSizeEstimate() int {
	size := s.Tail.TLSizeEstimate()
	for k, v := range s.Top {
		size += 4 + len(k) + 3 + v.TLSizeEstimate()
	}
	return size
}

func (s *MultiValue) MergeWithTL2(rng *rand.Rand, s2 *tlstatshouse.MultiValueBytes, fields_mask uint32, hostTagId int32, compression float64) {
	if s2.IsSetUniques(fields_mask) {
		_ = s.HLL.MergeRead(bytes.NewBuffer(s2.Uniques)) // return error, write meta metric
	}
	if s2.IsSetCentroids(fields_mask) {
		if s.ValueTDigest == nil && len(s2.Centroids) != 0 {
			s.ValueTDigest = tdigest.NewWithCompression(compression)
		}
		for _, c := range s2.Centroids {
			s.ValueTDigest.Add(float64(c.Value), float64(c.Count))
		}
	}
	if !s2.IsSetMaxHostTag(fields_mask) {
		s2.MaxHostTag = hostTagId
	}
	if !s2.IsSetMinHostTag(fields_mask) {
		s2.MinHostTag = s2.MaxHostTag // either original or set above
	}
	if !s2.IsSetMaxCounterHostTag(fields_mask) {
		s2.MaxCounterHostTag = s2.MaxHostTag // either original or set above
	}
	s.Value.MergeWithTLItem2(rng, s2, fields_mask)
}

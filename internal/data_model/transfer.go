// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"bytes"

	"github.com/hrissan/tdigest"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
)

func (k *Key) ToSlice() []int32 {
	result := append([]int32{}, k.Keys[:]...)
	i := format.MaxTags
	for ; i != 0; i-- {
		if result[i-1] != 0 {
			break
		}
	}
	return result[:i]
}

func KeyFromStatshouseMultiItem(item *tlstatshouse.MultiItemBytes, bucketTimestamp uint32) (key Key, shardID int) {
	// We use high byte of fieldsmask to pass shardID to aggregator, otherwise it is too much work for CPU
	sID := item.FieldsMask >> 24
	key.Timestamp = bucketTimestamp
	if item.IsSetT() {
		key.Timestamp = item.T
		if key.Timestamp >= bucketTimestamp {
			key.Timestamp = bucketTimestamp
		} else if bucketTimestamp > BelieveTimestampWindow && key.Timestamp < bucketTimestamp-BelieveTimestampWindow {
			key.Timestamp = bucketTimestamp - BelieveTimestampWindow
		}
	}
	key.Metric = item.Metric
	copy(key.Keys[:], item.Keys)
	return key, int(sID)
}

func (k *Key) TLSizeEstimate(defaultTimestamp uint32) int {
	i := format.MaxTags
	for ; i != 0; i-- {
		if k.Keys[i-1] != 0 {
			break
		}
	}
	sz := 4 + 4 + 4*i // metric, # of keys, keys
	if k.Timestamp != 0 && k.Timestamp != defaultTimestamp {
		sz += 4 // timestamp
	}
	return sz
}

func (k *Key) TLMultiItemFromKey(defaultTimestamp uint32) tlstatshouse.MultiItem {
	item := tlstatshouse.MultiItem{
		Metric: k.Metric,
		Keys:   k.ToSlice(),
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
	if s.Value.MaxHostTag != 0 {
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
	if s.Value.MaxHostTag != 0 {
		item.SetHostTag(s.Value.MaxHostTag, fieldsMask)
	}
	if s.HLL.ItemsCount() != 0 {
		*marshalBuf = s.HLL.MarshallAppend((*marshalBuf)[:0])
		item.SetUniques(string(*marshalBuf), fieldsMask)
	}
	if s.ValueTDigest != nil {
		var cc []tlstatshouse.Centroid
		for _, c := range s.ValueTDigest.Centroids() {
			cc = append(cc, tlstatshouse.Centroid{Value: float32(c.Mean), Weight: float32(c.Weight * sampleFactor)})
		}
		if len(cc) != 0 { // empty centroids is ordinary value
			item.SetCentroids(cc, fieldsMask) // TODO - do not set percentiles if v.ValueMin == v.ValueMax, restore on other side
		}
	}
	cou := s.Value.Counter * sampleFactor
	if cou != 0 {
		if cou == 1 {
			item.SetCounterEq1(true, fieldsMask)
		} else {
			item.SetCounter(cou, fieldsMask)
		}
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

func (s *ItemValue) MergeWithTLItem2(s2 *tlstatshouse.MultiValueBytes, fields_mask uint32, hostTag int32) {
	counter := float64(0)
	if s2.IsSetCounterEq1(fields_mask) {
		counter = 1
	}
	if s2.IsSetCounter(fields_mask) {
		counter = s2.Counter
	}
	if !s2.IsSetValueSet(fields_mask) {
		s.AddCounterHost(counter, hostTag)
		return
	}
	s.Counter += counter
	if !s2.IsSetValueMax(fields_mask) {
		s2.ValueSum = s2.ValueMin * counter
		s2.ValueSumSquare = s2.ValueSum * s2.ValueMin
		s2.ValueMax = s2.ValueMin
	}
	s.ValueSum += s2.ValueSum
	s.ValueSumSquare += s2.ValueSumSquare

	if !s.ValueSet || s2.ValueMin < s.ValueMin {
		s.ValueMin = s2.ValueMin
		s.MinHostTag = hostTag
	}
	if !s.ValueSet || s2.ValueMax > s.ValueMax {
		s.ValueMax = s2.ValueMax
		s.MaxHostTag = hostTag
	}
	s.ValueSet = true
}

func (s *MultiItem) MergeWithTLMultiItem(s2 *tlstatshouse.MultiItemBytes, hostTag int32) {
	for _, v := range s2.Top {
		mi := s.MapStringTopBytes(v.Key, v.Value.Counter)
		v.Key, _ = format.AppendValidStringValue(v.Key[:0], v.Key) // TODO - report this error via builtin metrics
		// we want to validate all incoming strings. In case of encoding error, v.Key will be truncated to 0
		mi.MergeWithTL2(&v.Value, v.FieldsMask, hostTag, AggregatorPercentileCompression)
	}
	s.Tail.MergeWithTL2(&s2.Tail, s2.FieldsMask, hostTag, AggregatorPercentileCompression)
}

func (s *MultiItem) TLSizeEstimate() int {
	size := s.Tail.TLSizeEstimate()
	for k, v := range s.Top {
		size += 4 + len(k) + 3 + v.TLSizeEstimate()
	}
	return size
}

func (s *MultiValue) MergeWithTL2(s2 *tlstatshouse.MultiValueBytes, fields_mask uint32, hostTag int32, compression float64) {
	if s2.IsSetUniques(fields_mask) {
		_ = s.HLL.MergeRead(bytes.NewBuffer(s2.Uniques)) // return error, write meta metric
	}
	if s2.IsSetCentroids(fields_mask) {
		if s.ValueTDigest == nil && len(s2.Centroids) != 0 {
			s.ValueTDigest = tdigest.NewWithCompression(compression)
		}
		for _, c := range s2.Centroids {
			s.ValueTDigest.Add(float64(c.Value), float64(c.Weight))
		}
	}
	if s2.IsSetHostTag(fields_mask) {
		hostTag = s2.HostTag
	}
	s.Value.MergeWithTLItem2(s2, fields_mask, hostTag)
}

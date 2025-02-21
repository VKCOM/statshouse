// Copyright 2025 V Kontakte LLC
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
	i := format.MaxTags
	for ; i != 0; i-- {
		if k.Tags[i-1] != 0 {
			break
		}
	}
	return k.Tags[:i]
}

func (k *Key) STagSlice() []string {
	i := format.MaxTags
	for ; i != 0; i-- {
		if len(k.STags[i-1]) != 0 {
			break
		}
	}
	return k.STags[:i]
}

// does not copy strings, we need max efficiency so want to look up in local map before converting []byte to string
func KeyFromStatshouseMultiItem(item *tlstatshouse.MultiItemBytes, bucketTimestamp uint32, newestTime uint32) (key Key, clampedTimestampTag int32) {
	key.Timestamp = bucketTimestamp
	if item.IsSetT() {
		key.Timestamp = item.T
		// sometimes if agent conveyor is stuck or if it is on machine with wrong clock but receives events from
		// client with correct clock, item timestamp will be > agent bucketTimestamp, we should not clamp by it.
		// instead we use aggregator clock which we demand to always be set correctly.
		if key.Timestamp > newestTime {
			key.Timestamp = newestTime
			clampedTimestampTag = format.TagValueIDSrcIngestionStatusWarnTimestampClampedFuture
		} else if bucketTimestamp > BelieveTimestampWindow && key.Timestamp < bucketTimestamp-BelieveTimestampWindow {
			key.Timestamp = bucketTimestamp - BelieveTimestampWindow
			clampedTimestampTag = format.TagValueIDSrcIngestionStatusWarnTimestampClampedPast
		}
		// above checks can be moved below }, but they will always be NOP as bucketTimestamp is both <= newestTime and in believe window
	}
	key.Metric = item.Metric
	copy(key.Tags[:], item.Keys)
	return
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
	return item
}

func (s *MultiValue) TLSizeEstimate() int {
	sz := 8 // counter without considering 0 and 1 optimizations
	if s.Value.MaxHostTag.I != 0 {
		sz += 4
	} else {
		sz += len(s.Value.MaxHostTag.S)
	}
	if !s.Value.MinHostTag.Equal(s.Value.MaxHostTag) {
		if s.Value.MinHostTag.I != 0 {
			sz += 4
		} else {
			sz += len(s.Value.MinHostTag.S)
		}
	}
	if !s.Value.MaxCounterHostTag.Equal(s.Value.MaxHostTag) {
		if s.Value.MaxCounterHostTag.I != 0 {
			sz += 4
		} else {
			sz += len(s.Value.MaxCounterHostTag.S)
		}
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

func (s *MultiValue) MultiValueToTL(metricInfo *format.MetricMetaValue, item *tlstatshouse.MultiValue, sampleFactor float64, fieldsMask *uint32, scratch []byte) []byte {
	cou := s.Value.Count() * sampleFactor
	if cou <= 0 {
		return scratch
	}
	// host tags are passed from "_h" tag (if set) in ApplyValue, ApplyUnique, ApplyCount functions
	if s.Value.MaxHostTag.I != 0 {
		item.SetMaxHostTag(s.Value.MaxHostTag.I, fieldsMask)
	} else if len(s.Value.MaxHostTag.S) > 0 {
		item.SetMaxHostStag(string(s.Value.MaxHostTag.S), fieldsMask)
	}
	if !s.Value.MinHostTag.Equal(s.Value.MaxHostTag) {
		if s.Value.MinHostTag.I != 0 {
			item.SetMinHostTag(s.Value.MinHostTag.I, fieldsMask)
		} else if len(s.Value.MinHostTag.S) > 0 {
			item.SetMinHostStag(string(s.Value.MinHostTag.S), fieldsMask)
		}
	}
	if !s.Value.MaxCounterHostTag.Equal(s.Value.MaxHostTag) {
		if s.Value.MaxCounterHostTag.I != 0 {
			item.SetMaxHostTag(s.Value.MaxCounterHostTag.I, fieldsMask)
		} else if len(s.Value.MaxCounterHostTag.S) > 0 {
			item.SetMaxHostStag(string(s.Value.MaxCounterHostTag.S), fieldsMask)
		}
	}
	if s.HLL.ItemsCount() != 0 {
		scratch = s.HLL.MarshallAppend(scratch[:0])
		item.SetUniques(string(scratch), fieldsMask) // allocates here
	}
	if cou == 1 {
		item.SetCounterEq1(true, fieldsMask)
	} else {
		item.SetCounter(cou, fieldsMask)
	}
	if !s.Value.ValueSet {
		return scratch
	}
	if metricInfo.HasPercentiles { // we want to set implicit centroids so aggregators aggregate without knowing metricInfo
		if s.ValueTDigest != nil {
			var cc []tlstatshouse.CentroidFloat
			for _, c := range s.ValueTDigest.Centroids() {
				cc = append(cc, tlstatshouse.CentroidFloat{Value: float32(c.Mean), Count: float32(c.Weight * sampleFactor)})
			}
			if len(cc) != 0 { // empty centroids is ordinary value
				item.SetCentroids(cc, fieldsMask)
			}
		} else {
			item.SetImplicitCentroid(true, fieldsMask)
		}
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
	return scratch
}

func (s *ItemValue) MergeWithTLItem2(s2 *tlstatshouse.MultiValueBytes, fields_mask uint32) {
	// We do not care checking values for NaN/Inf here
	if !s2.IsSetValueMax(fields_mask) {
		s2.ValueSum = s2.ValueMin * s2.Counter
		s2.ValueSumSquare = s2.ValueSum * s2.ValueMin
		s2.ValueMax = s2.ValueMin
	}
	s.ValueSum += s2.ValueSum
	s.ValueSumSquare += s2.ValueSumSquare

	if !s.ValueSet || s2.ValueMin < s.ValueMin {
		s.ValueMin = s2.ValueMin
		s.MinHostTag = TagUnionBytes{I: s2.MinHostTag, S: s2.MinHostStag}
	}
	if !s.ValueSet || s2.ValueMax > s.ValueMax {
		s.ValueMax = s2.ValueMax
		s.MaxHostTag = TagUnionBytes{I: s2.MaxHostTag, S: s2.MaxHostStag}
	}
	s.ValueSet = true
}

func (s *MultiItem) MergeWithTLMultiItem(rng *rand.Rand, capacity int, s2 *tlstatshouse.MultiItemBytes, hostTag TagUnionBytes) {
	for _, v := range s2.Top {
		mi := s.MapStringTopBytes(rng, capacity, TagUnionBytes{S: v.Stag, I: v.Tag}, v.Value.Counter)
		v.Stag, _ = format.AppendValidStringValue(v.Stag[:0], v.Stag) // TODO - report this error via builtin metrics
		// we want to validate all incoming strings. In case of encoding error, v.Key will be truncated to 0
		mi.MergeWithTL2(rng, &v.Value, v.FieldsMask, hostTag, AggregatorPercentileCompression)
	}
	s.Tail.MergeWithTL2(rng, &s2.Tail, s2.FieldsMask, hostTag, AggregatorPercentileCompression)
}

func (s *MultiItem) TLSizeEstimate() int {
	size := s.Tail.TLSizeEstimate()
	for k, v := range s.Top {
		size += 4 + len(k.S) + 3 + v.TLSizeEstimate()
	}
	return size
}

func (s *MultiValue) MergeWithTL2(rng *rand.Rand, s2 *tlstatshouse.MultiValueBytes, fields_mask uint32, hostTag TagUnionBytes, compression float64) {
	// 1. restore and check cuunter
	if s2.IsSetCounterEq1(fields_mask) {
		s2.Counter = 1
	}
	if s2.Counter <= 0 || math.IsNaN(s2.Counter) { // sanity check/check for empty String Top tail
		return
	} // TODO - write metric
	if s2.Counter > math.MaxFloat32 { // agents do similar check, but this is so cheap, we repeat on aggregators.
		s2.Counter = math.MaxFloat32
	}
	// 2. restore hosts
	if !s2.IsSetMaxHostTag(fields_mask) && !s2.IsSetMaxHostStag(fields_mask) {
		if hostTag.I != 0 {
			s2.MaxHostTag = hostTag.I
		} else if len(hostTag.S) > 0 {
			s2.MaxHostStag = hostTag.S
		}
	}
	if !s2.IsSetMinHostTag(fields_mask) && !s2.IsSetMinHostStag(fields_mask) {
		// either original or set above
		s2.MinHostTag = s2.MaxHostTag
		s2.MinHostStag = s2.MaxHostStag
	}
	if !s2.IsSetMaxCounterHostTag(fields_mask) && !s2.IsSetMaxCounterHostStag(fields_mask) {
		// either original or set above
		s2.MaxCounterHostTag = s2.MaxHostTag
		s2.MaxCounterHostStag = s2.MaxHostStag
	}
	// 3. aggregate counter
	s.AddCounterHost(rng, s2.Counter, TagUnionBytes{I: s2.MaxCounterHostTag, S: s2.MaxCounterHostStag})
	if len(s2.Uniques) != 0 {
		_ = s.HLL.MergeRead(bytes.NewBuffer(s2.Uniques)) // return error, write meta metric
	}
	if !s2.IsSetValueSet(fields_mask) {
		return
	}
	// 4. aggregate value
	s.Value.MergeWithTLItem2(s2, fields_mask)
	if len(s2.Centroids) != 0 {
		if s.ValueTDigest == nil {
			s.ValueTDigest = tdigest.NewWithCompression(compression)
		}
		for _, c := range s2.Centroids {
			s.ValueTDigest.Add(float64(c.Value), float64(c.Count))
		}
	}
	if s2.IsSetImplicitCentroid(fields_mask) {
		if s.ValueTDigest == nil {
			s.ValueTDigest = tdigest.NewWithCompression(compression)
		}
		s.ValueTDigest.Add(float64(s2.ValueMin), float64(s2.Counter))
	}
}

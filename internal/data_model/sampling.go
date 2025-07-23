// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"math"
	"sort"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
	"pgregory.net/rand"
)

const maxFairKeyLen = 3

type (
	SamplingMultiItemPair struct {
		Item        *MultiItem
		WhaleWeight float64 // whale selection criteria, for now sum Counters
		Size        int
		MetricID    int32
		BucketTs    uint32
		metric      *format.MetricMetaValue
		fairKey     [maxFairKeyLen]int32
		fairKeyLen  int
	}

	samplerGroup struct { // either metric group, metric or fair key
		NamespaceID    int32
		GroupID        int32
		MetricID       int32
		SumSizeKeep    ItemValue
		SumSizeDiscard ItemValue
		depth          int
		budget         int64
		budgetDenom    int64
		roundFactors   bool
		noSampleAgent  bool
		weight         int64 // actually, effective weight
		sumSize        int64
		items          []SamplingMultiItemPair
	}

	SamplerConfig struct {
		// Options
		ModeAgent            bool
		SampleKeepSingle     bool
		DisableNoSampleAgent bool // disables "noSampleAgent" option above
		SampleNamespaces     bool
		SampleGroups         bool
		SampleKeys           bool

		// External services
		Meta format.MetaStorageInterface
		Rand *rand.Rand

		// Called when sampling algorithm calculates metric sample factor
		SampleFactorF func(int32, float64)

		// Called when sampling algorithm decides to either keep or discard the item
		KeepF    func(*MultiItem, uint32)
		DiscardF func(*MultiItem, uint32)

		// Unit tests support
		RoundF  func(float64, *rand.Rand) float64 // rounds sample factor to an integer
		SelectF func([]SamplingMultiItemPair, float64, *rand.Rand) int

		SamplerBuffers
	}

	partitionFunc func(*sampler, samplerGroup) ([]samplerGroup, int64)

	sampler struct {
		SamplerConfig
		sumSizeKeepBuiltin   ItemValue
		sumSizeDiscard       ItemValue
		currentGroup         samplerGroup
		currentMetricID      int32
		currentMetricSFSum   float64
		currentMetricSFCount float64
		MetricCount          int

		timeStart      time.Time
		timePartition  time.Time
		timeMetricMeta time.Duration
		timeBudgeting  time.Time
		timeSampling   time.Duration
		timeEnd        time.Time
	}

	SamplerBuffers struct {
		items         []SamplingMultiItemPair
		partF         []partitionFunc
		MetricGroups  []samplerGroup
		SampleFactors []tlstatshouse.SampleFactor
	}
)

var missingMetricMeta = format.MetricMetaValue{
	GroupID:     format.BuiltinGroupIDMissing,
	NamespaceID: format.BuiltinNamespaceIDMissing,
}

func NewSampler(c SamplerConfig) sampler {
	// buffer reuse
	c.items = c.items[:0]
	c.partF = c.partF[:0]
	c.MetricGroups = c.MetricGroups[:0]
	c.SampleFactors = c.SampleFactors[:0]
	// partition functions
	if c.SampleNamespaces {
		c.partF = append(c.partF, partitionByNamespace)
	}
	if c.SampleGroups {
		c.partF = append(c.partF, partitionByGroup)
	}
	c.partF = append(c.partF, partitionByMetric)
	// unit test support
	if c.RoundF == nil {
		c.RoundF = roundSampleFactor
	}
	if c.SelectF == nil {
		c.SelectF = selectRandom
	}
	return sampler{
		timeStart:     time.Now(),
		SamplerConfig: c,
	}
}

func (h *sampler) Add(p SamplingMultiItemPair) {
	if p.Size < 1 {
		p.Item.SF = math.MaxFloat32
		if h.DiscardF != nil {
			h.DiscardF(p.Item, p.BucketTs)
		}
		h.sumSizeDiscard.AddValue(0)
		return
	}
	h.items = append(h.items, p)
}

func (h *sampler) Run(budget int64) {
	if len(h.items) == 0 {
		return
	}
	h.timePartition = time.Now()
	// query metric meta, initialize fair key
	// TODO: do not query metric meta on agent
	sort.Slice(h.items, func(i, j int) bool {
		return h.items[i].MetricID < h.items[j].MetricID
	})
	for i := 0; i < len(h.items); i++ {
		if i > 0 && h.items[i].MetricID == h.items[i-1].MetricID {
			h.items[i].metric = h.items[i-1].metric
		} else {
			if h.items[i].Item.MetricMeta != nil && h.items[i].MetricID == h.items[i].Item.MetricMeta.MetricID {
				h.items[i].metric = h.items[i].Item.MetricMeta
			} else {
				h.items[i].metric = h.getMetricMeta(h.items[i].MetricID)
			}
		}
		if h.SampleKeys && len(h.items[i].metric.FairKey) != 0 {
			n := len(h.items[i].metric.FairKey)
			if n > maxFairKeyLen {
				n = maxFairKeyLen
			}
			for j := 0; j < n; j++ {
				if x := h.items[i].metric.FairKey[j]; 0 <= x && x < len(h.items[i].Item.Key.Tags) {
					h.items[i].fairKey[j] = h.items[i].Item.Key.Tags[x]
				}
			}
			h.items[i].fairKeyLen = n
		}
	}
	// partition by namespace/group/metric/key
	sort.Slice(h.items, func(i, j int) bool {
		var lhs, rhs *SamplingMultiItemPair = &h.items[i], &h.items[j]
		if lhs.metric.NamespaceID != rhs.metric.NamespaceID {
			return lhs.metric.NamespaceID < rhs.metric.NamespaceID
		}
		if lhs.metric.GroupID != rhs.metric.GroupID {
			return lhs.metric.GroupID < rhs.metric.GroupID
		}
		if lhs.MetricID != rhs.MetricID {
			return lhs.MetricID < rhs.MetricID
		}
		for i := 0; i < lhs.fairKeyLen; i++ {
			if lhs.fairKey[i] != rhs.fairKey[i] {
				return lhs.fairKey[i] < rhs.fairKey[i]
			}
		}
		return false
	})
	// count metrics & groups
	metricCount := 1
	groupCount := 1
	for i := 1; i < len(h.items); i++ {
		if h.items[i-1].metric.GroupID != h.items[i].metric.GroupID {
			groupCount++
		}
		if h.items[i-1].MetricID != h.items[i].MetricID {
			metricCount++
		}
	}
	h.MetricCount = metricCount
	h.MetricGroups = make([]samplerGroup, 0, groupCount+2)
	// initialize current metric and group
	h.currentGroup = samplerGroup{
		NamespaceID: format.BuiltinNamespaceIDDefault,
		GroupID:     format.BuiltinGroupIDDefault,
		items:       h.items,
		budget:      budget,
	}
	h.currentMetricID = h.items[0].MetricID
	// run sampling
	h.timeBudgeting = time.Now()
	h.run(h.currentGroup)
	// finalize "MetricGroups"
	h.MetricGroups = append(h.MetricGroups, h.currentGroup)
	if h.sumSizeKeepBuiltin.Count() > 0 {
		h.MetricGroups = append(h.MetricGroups, samplerGroup{
			NamespaceID: format.BuiltinNamespaceIDDefault,
			GroupID:     format.BuiltinGroupIDBuiltin,
			SumSizeKeep: h.sumSizeKeepBuiltin,
		})
	}
	if h.sumSizeDiscard.Count() > 0 {
		h.MetricGroups = append(h.MetricGroups, samplerGroup{
			SumSizeDiscard: h.sumSizeDiscard,
		})
	}
	// record last metric sample factor
	h.setCurrentMetric(0)
	// record running time
	h.timeEnd = time.Now()
}

func (h *sampler) KeepBuiltin(p SamplingMultiItemPair) {
	h.sumSizeKeepBuiltin.AddValue(float64(p.Size))
}

func (h *sampler) ItemCount() int {
	return len(h.items)
}

func (h *sampler) TimeAppend() float64 {
	return h.timePartition.Sub(h.timeStart).Seconds()
}

func (h *sampler) TimePartition() float64 {
	return (h.timeBudgeting.Sub(h.timePartition) - h.timeMetricMeta).Seconds()
}

func (h *sampler) TimeMetricMeta() float64 {
	return h.timeMetricMeta.Seconds()
}

func (h *sampler) TimeBudgeting() float64 {
	return (h.timeEnd.Sub(h.timeBudgeting) - h.timeSampling).Seconds()
}

func (h *sampler) TimeSampling() float64 {
	return h.timeSampling.Seconds()
}

func (h *sampler) run(g samplerGroup) {
	// partition, then sort by sumSize/weight ratio
	var s []samplerGroup
	var sumWeight int64
	if g.depth < len(h.partF) {
		s, sumWeight = h.partF[g.depth](h, g)
	} else {
		s, sumWeight = partitionByKey(h, g)
	}
	sort.Slice(s, func(i, j int) bool {
		return s[i].sumSize*s[j].weight < s[j].sumSize*s[i].weight // comparing rational numbers
	})
	// groups smaller than the budget aren't sampled
	i := 0
	for ; i < len(s); i++ {
		s[i].budget = g.budget * s[i].weight
		s[i].budgetDenom = sumWeight
		if s[i].budget < sumWeight*s[i].sumSize {
			break // SF > 1
		}
		if s[i].MetricID == 0 {
			h.setCurrentGroup(s[i])
		}
		s[i].keep(h)
		g.budget -= s[i].sumSize
		sumWeight -= s[i].weight
	}
	// sample groups larger than budget
	for ; i < len(s); i++ {
		s[i].budget = g.budget * s[i].weight
		s[i].budgetDenom = sumWeight
		if s[i].depth < len(h.partF) {
			if s[i].GroupID != 0 && s[i].MetricID == 0 {
				h.setCurrentGroup(s[i])
			}
		} else if s[i].depth == len(h.partF) && s[i].MetricID != h.currentMetricID {
			h.setCurrentMetric(s[i].MetricID)
		}
		if s[i].noSampleAgent && h.ModeAgent && !h.DisableNoSampleAgent {
			s[i].keep(h)
		} else if s[i].depth < len(h.partF)+s[i].items[0].fairKeyLen {
			s[i].budget = int64(h.RoundF(float64(s[i].budget)/float64(s[i].budgetDenom), h.Rand))
			s[i].budgetDenom = 1
			h.run(s[i])
		} else {
			h.sample(s[i])
		}
	}
}

func (g samplerGroup) keep(h *sampler) {
	for i := range g.items {
		g.items[i].keep(1, h)
	}
}

func (p *SamplingMultiItemPair) keep(sf float64, h *sampler) {
	p.Item.SF = sf // communicate selected factor to next step of processing
	if h.KeepF != nil {
		h.KeepF(p.Item, p.BucketTs)
	}
	h.currentGroup.SumSizeKeep.AddValue(float64(p.Size))
}

func (p *SamplingMultiItemPair) discard(sf float64, h *sampler) {
	p.Item.SF = sf // communicate selected factor to next step of processing
	if h.DiscardF != nil {
		h.DiscardF(p.Item, p.BucketTs)
	}
	h.currentGroup.SumSizeDiscard.AddValue(float64(p.Size))
}

func (h *sampler) sample(g samplerGroup) {
	if len(g.items) == 0 {
		return
	}
	timeStart := time.Now()
	defer func() { h.timeSampling += time.Since(timeStart) }()
	if h.SampleKeepSingle && len(g.items) == 1 && g.items[0].Item != nil && g.items[0].Item.isSingleValueCounter() {
		g.items[0].keep(1, h)
		return
	}
	sfNum := g.budgetDenom * g.sumSize
	sfDenom := g.budget
	if sfNum < 1 {
		sfNum = 1
	}
	if sfDenom < 1 {
		sfDenom = 1
	}
	sf := float64(sfNum) / float64(sfDenom)
	if g.roundFactors {
		sf = h.RoundF(sf, h.Rand)
		if sf <= 1 { // many sample factors are between 1 and 2, so this is worthy optimization
			g.keep(h)
			return
		}
		sfNum = int64(sf)
		sfDenom = 1
	}
	h.currentMetricSFCount++
	h.currentMetricSFSum += sf
	// keep whales
	items := g.items
	if !items[0].metric.WhalesOff {
		// Often we have a few rows with dominating counts (whales). If we randomly discard those rows, we get wild fluctuation
		// of sums. On the other hand if we systematically discard rows with small counts, rare events, like errors cannot get through.
		// So we allow half of sampling budget for whales, and the other half is spread fairly between other events.
		pos := int(int64(len(items)) * sfDenom / sfNum / 2) // len(items) / sf / 2
		if pos > 0 {
			if pos > len(items) { // should always hold but checking is cheap
				pos = len(items)
			}
			sort.Slice(items, func(i, j int) bool {
				return items[i].WhaleWeight > items[j].WhaleWeight
			})
			for i := 0; i < pos; i++ {
				items[i].keep(1, h)
			}
			items = items[pos:]
			sf *= 2 // space has been taken by whales
		}
	}
	// sample remaining
	pos := h.SelectF(items, sf, h.Rand)
	for i := 0; i < pos; i++ {
		items[i].keep(sf, h)
	}
	for i := pos; i < len(items); i++ {
		items[i].discard(sf, h)
	}
}

func (h *sampler) setCurrentGroup(g samplerGroup) {
	if h.currentGroup.depth != 0 {
		h.MetricGroups = append(h.MetricGroups, h.currentGroup)
	}
	if g.GroupID == 0 {
		g.GroupID = format.BuiltinGroupIDDefault
	}
	h.currentGroup = g
}

func (h *sampler) setCurrentMetric(metricID int32) {
	if h.currentMetricSFCount > 0 {
		sf := h.currentMetricSFSum / h.currentMetricSFCount // AVG
		if h.SampleFactorF != nil {
			h.SampleFactorF(h.currentMetricID, sf)
		} else {
			h.SampleFactors = append(h.SampleFactors, tlstatshouse.SampleFactor{
				Metric: h.currentMetricID,
				Value:  float32(sf),
			})
		}
	}
	h.currentMetricID = metricID
	h.currentMetricSFSum = 0
	h.currentMetricSFCount = 0
}

func partitionByNamespace(h *sampler, g samplerGroup) ([]samplerGroup, int64) {
	s := g.items
	if len(s) == 0 {
		return nil, 0
	}
	newSamplerGroup := func(items []SamplingMultiItemPair, sumSize int64) samplerGroup {
		return samplerGroup{
			depth:       g.depth + 1,
			weight:      items[0].getNamespaceWeight(h),
			items:       items,
			sumSize:     sumSize,
			NamespaceID: items[0].metric.NamespaceID,
		}
	}
	var res []samplerGroup
	var sumWeight int64
	var i, j int
	sumSize := int64(s[0].Size)
	for j = 1; j < len(s); j++ {
		if s[i].metric.NamespaceID != s[j].metric.NamespaceID {
			v := newSamplerGroup(s[i:j], sumSize)
			res = append(res, v)
			sumWeight += v.weight
			i = j
			sumSize = 0
		}
		sumSize += int64(s[j].Size)
	}
	v := newSamplerGroup(s[i:j], sumSize)
	res = append(res, v)
	sumWeight += v.weight
	return res, sumWeight
}

func partitionByGroup(h *sampler, g samplerGroup) ([]samplerGroup, int64) {
	s := g.items
	if len(s) == 0 {
		return nil, 0
	}
	newSamplerGroup := func(items []SamplingMultiItemPair, sumSize int64) samplerGroup {
		return samplerGroup{
			depth:       g.depth + 1,
			weight:      items[0].getGroupWeight(h),
			items:       items,
			sumSize:     sumSize,
			NamespaceID: items[0].metric.NamespaceID,
			GroupID:     items[0].metric.GroupID,
		}
	}
	var res []samplerGroup
	var sumWeight int64
	var i, j int
	sumSize := int64(s[0].Size)
	for j = 1; j < len(s); j++ {
		if s[i].metric.GroupID != s[j].metric.GroupID {
			v := newSamplerGroup(s[i:j], sumSize)
			res = append(res, v)
			sumWeight += v.weight
			i = j
			sumSize = 0
		}
		sumSize += int64(s[j].Size)
	}
	v := newSamplerGroup(s[i:j], sumSize)
	res = append(res, v)
	sumWeight += v.weight
	return res, sumWeight
}

func partitionByMetric(h *sampler, g samplerGroup) ([]samplerGroup, int64) {
	s := g.items
	if len(s) == 0 {
		return nil, 0
	}
	newSamplerGroup := func(items []SamplingMultiItemPair, sumSize int64) samplerGroup {
		return samplerGroup{
			depth:         g.depth + 1,
			weight:        items[0].getMetricWeight(),
			items:         items,
			sumSize:       sumSize,
			roundFactors:  items[0].metric.RoundSampleFactors,
			noSampleAgent: items[0].metric.NoSampleAgent,
			NamespaceID:   items[0].metric.NamespaceID,
			GroupID:       items[0].metric.GroupID,
			MetricID:      items[0].MetricID,
		}
	}
	var res []samplerGroup
	var sumWeight int64
	var i, j int
	sumSize := int64(s[0].Size)
	for j = 1; j < len(s); j++ {
		if s[i].MetricID != s[j].MetricID {
			v := newSamplerGroup(s[i:j], sumSize)
			res = append(res, v)
			sumWeight += v.weight
			i = j
			sumSize = 0
		}
		sumSize += int64(s[j].Size)
	}
	v := newSamplerGroup(s[i:j], sumSize)
	res = append(res, v)
	sumWeight += v.weight
	return res, sumWeight
}

func partitionByKey(h *sampler, g samplerGroup) ([]samplerGroup, int64) {
	s := g.items
	if len(s) == 0 {
		return nil, 0
	}
	depth := g.depth - len(h.partF)
	newSamplerGroup := func(items []SamplingMultiItemPair, sumSize int64) samplerGroup {
		return samplerGroup{
			depth:         g.depth + 1,
			weight:        1,
			items:         items,
			sumSize:       sumSize,
			roundFactors:  items[0].metric.RoundSampleFactors,
			noSampleAgent: items[0].metric.NoSampleAgent,
			NamespaceID:   items[0].metric.NamespaceID,
			GroupID:       items[0].metric.GroupID,
			MetricID:      items[0].MetricID,
		}
	}
	var res []samplerGroup
	var sumWeight int64
	var i, j int
	sumSize := int64(s[0].Size)
	for j = 1; j < len(s); j++ {
		if s[i].fairKey[depth] != s[j].fairKey[depth] {
			v := newSamplerGroup(s[i:j], sumSize)
			res = append(res, v)
			sumWeight += v.weight
			i = j
			sumSize = 0
		}
		sumSize += int64(s[j].Size)
	}
	v := newSamplerGroup(s[i:j], sumSize)
	res = append(res, v)
	sumWeight += v.weight
	return res, sumWeight
}

func (h *sampler) getMetricMeta(metricID int32) *format.MetricMetaValue {
	if h.Meta == nil {
		return &missingMetricMeta
	}
	timeStart := time.Now()
	if res := h.Meta.GetMetaMetric(metricID); res != nil {
		h.timeMetricMeta += time.Since(timeStart)
		return res
	}
	if res := format.BuiltinMetrics[metricID]; res != nil {
		return res
	}
	return &missingMetricMeta
}

func (p *SamplingMultiItemPair) getMetricWeight() int64 {
	res := p.metric.EffectiveWeight * int64(p.Item.WeightMultiplier)
	if res < 1 {
		res = 1
	}
	return res
}

func (p *SamplingMultiItemPair) getGroupWeight(h *sampler) int64 {
	var res int64
	if h.Meta != nil && p.metric.GroupID != 0 {
		if meta := h.Meta.GetGroup(p.metric.GroupID); meta != nil {
			res = meta.EffectiveWeight
		}
	}
	if res < 1 {
		res = 1
	}
	return res
}

func (p *SamplingMultiItemPair) getNamespaceWeight(h *sampler) int64 {
	var res int64
	if h.Meta != nil && p.metric.NamespaceID != 0 {
		if meta := h.Meta.GetNamespace(p.metric.NamespaceID); meta != nil {
			res = meta.EffectiveWeight
		}
	}
	if res < 1 {
		res = 1
	}
	return res
}

func (s samplerGroup) Budget() float64 {
	res := float64(s.budget)
	if s.budgetDenom > 1 {
		res /= float64(s.budgetDenom)
	}
	return res
}

func selectRandom(s []SamplingMultiItemPair, sf float64, r *rand.Rand) int {
	if sf <= 1 {
		return len(s)
	}
	n := 0
	for i := 0; i < len(s); i++ {
		if r.Float64()*sf < 1 {
			if n < i {
				s[n], s[i] = s[i], s[n]
			}
			n++
		}
	}
	return n
}

// This function will be used in second sampling pass to fit all saved data in predefined budget
func SampleFactor(rnd *rand.Rand, sampleFactors map[int32]float64, metric int32) (float64, bool) {
	sf, ok := sampleFactors[metric]
	if !ok {
		return 1, true
	}
	if rnd.Float64()*sf < 1 {
		return sf, true
	}
	return 0, false
}

func roundSampleFactor(sf float64, rnd *rand.Rand) float64 {
	floor := math.Floor(sf)
	delta := sf - floor
	if rnd.Float64() < delta {
		return floor + 1
	}
	return floor
}

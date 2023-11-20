// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"math"
	"sort"

	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
)

type (
	SamplingMultiItemPair struct {
		Key         Key
		Item        *MultiItem
		WhaleWeight float64 // whale selection criteria, for now sum Counters
		Size        int
		MetricID    int32
		metric      *format.MetricMetaValue
		group       *format.MetricsGroup
		fairKey     int32
	}

	SamplerGroup struct { // either metric group, metric or fair key
		MetricID int32
		SF       float64

		roundFactors  bool
		noSampleAgent bool
		weight        int64 // actually, effective weight
		sumSize       int64
		items         []SamplingMultiItemPair
	}

	SamplerConfig struct {
		// Options
		ModeAgent    bool // to support "NoSampleAgent" option above
		SampleGroups bool
		SampleKeys   bool

		// External services
		Meta format.MetaStorageInterface
		Rand *rand.Rand

		// Called when sampling algorithm decides to either keep or discard the item
		KeepF    func(Key, *MultiItem)
		DiscardF func(Key, *MultiItem)

		// Unit tests support
		RoundF  func(float64, *rand.Rand) float64 // rounds sample factor to an integer
		SelectF func([]SamplingMultiItemPair, float64, *rand.Rand) int
	}

	Sampler struct {
		items     []SamplingMultiItemPair
		config    SamplerConfig
		partF     []func([]SamplingMultiItemPair) ([]SamplerGroup, int64)
		nilMetric format.MetricMetaValue
		nilGroup  format.MetricsGroup
	}

	SamplerStep struct {
		Groups      []SamplerGroup // sorted by SumSize/Weight ratio ascending
		BudgetNum   int64          // groups starting from StartPos were sampled to fit the budget=BudgetNum/BudgetDenom
		BudgetDenom int64          // budget denominator
		StartPos    int            // groups left of StartPos were not sampled
		SumWeight   int64          // summary weight of groups starting from StartPos
	}

	SamplerStatistics struct {
		Count   int                                 // number of metrics with SF > 1
		Steps   []SamplerStep                       // steps contributed to "Count"
		Items   map[[3]int32]*SamplerStatisticsItem // grouped by [namespace, group, metric_kind]
		Metrics map[int32]int                       // series count grouped by metric
	}

	SamplerStatisticsItem struct {
		SumSizeKeep    ItemValue
		SumSizeDiscard ItemValue
	}
)

func NewSampler(capacity int, config SamplerConfig) Sampler {
	if config.RoundF == nil {
		config.RoundF = roundSampleFactor
	}
	if config.SelectF == nil {
		config.SelectF = selectRandom
	}
	h := Sampler{
		items:  make([]SamplingMultiItemPair, 0, capacity),
		config: config,
		nilMetric: format.MetricMetaValue{
			EffectiveWeight: format.EffectiveWeightOne,
		},
		nilGroup: format.MetricsGroup{
			EffectiveWeight: format.EffectiveWeightOne,
		},
	}
	if config.SampleGroups {
		h.partF = append(h.partF, partitionByGroup)
	}
	h.partF = append(h.partF, partitionByMetric)
	if config.SampleKeys {
		h.partF = append(h.partF, partitionByKey)
	}
	return h
}

func (h *Sampler) Add(p SamplingMultiItemPair) {
	if p.Size <= 0 { // size can't be zero or less, sanity check
		return
	}
	if p.Item.MetricMeta != nil && p.MetricID == p.Item.MetricMeta.MetricID {
		p.metric = p.Item.MetricMeta
	} else {
		p.metric = h.getMetricMeta(p.MetricID)
	}
	if h.config.SampleGroups {
		p.group = h.getGroupMeta(p.metric.GroupID)
	} else {
		p.group = &h.nilGroup
	}
	if h.config.SampleKeys {
		x := p.metric.FairKeyIndex
		if 0 <= x || x < format.MaxTags {
			p.fairKey = p.Key.Keys[x]
		}
	}
	h.items = append(h.items, p)
}

func (h *Sampler) Run(budgetNum, budgetDenom int64) SamplerStatistics {
	// Partition by group/metric/key and run
	sort.Slice(h.items, func(i, j int) bool {
		var lhs, rhs *SamplingMultiItemPair = &h.items[i], &h.items[j]
		if lhs.group.ID != rhs.group.ID {
			return lhs.group.ID < rhs.group.ID
		}
		if lhs.MetricID != rhs.MetricID {
			return lhs.MetricID < rhs.MetricID
		}
		return lhs.fairKey < rhs.fairKey
	})
	stat := SamplerStatistics{
		Items:   map[[3]int32]*SamplerStatisticsItem{},
		Metrics: map[int32]int{},
	}
	h.run(h.items, 0, budgetNum, budgetDenom, &stat)
	return stat
}

func (h *Sampler) run(s []SamplingMultiItemPair, depth int, budgetNum, budgetDenom int64, stat *SamplerStatistics) {
	// The following two cases should not happen, but if they do, it will cause panic
	if budgetNum < 1 {
		budgetNum = 1
	}
	if budgetDenom < 1 {
		budgetDenom = 1
	}
	// Partition, then sort groups by sumSize/weight ratio
	groups, sumWeight := h.partF[depth](s)
	sort.Slice(groups, func(i, j int) bool {
		var lhs, rhs *SamplerGroup = &groups[i], &groups[j]
		return lhs.sumSize*rhs.weight < rhs.sumSize*lhs.weight // comparing rational numbers
	})
	// Groups smaller than the budget aren't sampled
	pos := 0
	for i := range groups {
		g := &groups[i]
		if g.sumSize*budgetDenom*sumWeight > budgetNum*g.weight {
			break // SF > 1
		}
		budgetNum -= g.sumSize * budgetDenom
		sumWeight -= g.weight
		h.keep(g, stat)
		pos++
	}
	// Sample remaining groups who didn't fit into the budget
	n := 0 // number of "sample" calls with g.SF > 1
	for i := pos; i < len(groups); i++ {
		g := &groups[i]
		if g.noSampleAgent && h.config.ModeAgent {
			h.keep(g, stat)
		} else if depth < len(h.partF)-1 {
			d := budgetDenom * (int64(len(groups) - pos)) // group budget denominator
			h.run(g.items, depth+1, budgetNum, d, stat)
		} else {
			h.sample(g, budgetNum, budgetDenom, sumWeight, stat)
			if g.SF > 1 {
				n++
			}
		}
	}
	// Update statistics
	if n != 0 {
		stat.Count += n
		stat.Steps = append(stat.Steps, SamplerStep{
			Groups:      groups,
			BudgetNum:   budgetNum,
			BudgetDenom: budgetDenom,
			StartPos:    pos,
			SumWeight:   sumWeight,
		})
	}
}

func (h *Sampler) keep(g *SamplerGroup, stat *SamplerStatistics) {
	for i := range g.items {
		p := &g.items[i]
		p.Item.SF = 1 // communicate selected factor to next step of processing
		if h.config.KeepF != nil {
			h.config.KeepF(p.Key, p.Item)
		}
		stat.add(p, true)
	}
}

func (h *Sampler) sample(g *SamplerGroup, budgetNum, budgetDenom, sumWeight int64, stat *SamplerStatistics) {
	var (
		sfNum   = g.sumSize * budgetDenom * sumWeight
		sfDenom = budgetNum * g.weight
	)
	if sfNum < 1 {
		sfNum = 1
	}
	if sfDenom < 1 {
		sfDenom = 1
	}
	sf := float64(sfNum) / float64(sfDenom)
	if g.roundFactors {
		sf = h.config.RoundF(sf, h.config.Rand)
		if sf <= 1 { // many sample factors are between 1 and 2, so this is worthy optimization
			h.keep(g, stat)
			return
		}
		sfNum = int64(sf)
		sfDenom = 1
	}
	g.SF = sf
	// Keep whales
	//
	// Often we have a few rows with dominating counts (whales). If we randomly discard those rows, we get wild fluctuation
	// of sums. On the other hand if we systematically discard rows with small counts, rare events, like errors cannot get through.
	// So we allow half of sampling budget for whales, and the other half is spread fairly between other events.
	var (
		items = g.items
		pos   = int(int64(len(items)) * sfDenom / sfNum / 2) // len(items) / sf / 2
	)
	if pos > 0 {
		if pos > len(items) { // should always hold but checking is cheap
			pos = len(items)
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].WhaleWeight > items[j].WhaleWeight
		})
		for i := 0; i < pos; i++ {
			p := &items[i]
			p.Item.SF = 1 // communicate selected factor to next step of processing
			if h.config.KeepF != nil {
				h.config.KeepF(p.Key, p.Item)
			}
			stat.add(p, true)
		}
		items = items[pos:]
	}
	// Space has been taken by whales
	// NB! Should be moved at the end of the above "if" ASAP
	// NB! because unconditional multiplication here is a BUG
	// NB! leading to significant counters (sums as well) increase.
	// NB! Left here to not deploy multiple changes together.
	sf *= 2
	// Sample tail
	pos = h.config.SelectF(items, sf, h.config.Rand)
	for i := 0; i < pos; i++ {
		p := &items[i]
		p.Item.SF = sf // communicate selected factor to next step of processing
		if h.config.KeepF != nil {
			h.config.KeepF(p.Key, p.Item)
		}
		stat.add(p, true)
	}
	for i := pos; i < len(items); i++ {
		p := &items[i]
		p.Item.SF = sf // communicate selected factor to next step of processing
		if h.config.DiscardF != nil {
			h.config.DiscardF(p.Key, p.Item)
		}
		stat.add(p, false)
	}
}

func (stat *SamplerStatistics) add(p *SamplingMultiItemPair, keep bool) {
	var metricKind int32
	switch {
	case p.Item.Tail.ValueTDigest != nil:
		metricKind = format.TagValueIDSizePercentiles
	case p.Item.Tail.HLL.ItemsCount() != 0:
		metricKind = format.TagValueIDSizeUnique
	case p.Item.Tail.Value.ValueSet:
		metricKind = format.TagValueIDSizeValue
	default:
		metricKind = format.TagValueIDSizeCounter
	}
	k := [3]int32{p.metric.NamespaceID, p.metric.GroupID, metricKind}
	v := stat.Items[k]
	if v == nil {
		v = &SamplerStatisticsItem{}
		stat.Items[k] = v
	}
	if keep {
		v.SumSizeKeep.AddValue(float64(p.Size))
	} else {
		v.SumSizeDiscard.AddValue(float64(p.Size))
	}
	stat.Metrics[p.MetricID]++
}

func partitionByGroup(s []SamplingMultiItemPair) ([]SamplerGroup, int64) {
	if len(s) == 0 {
		return nil, 0
	}
	newSamplerGroup := func(items []SamplingMultiItemPair, sumSize int64) SamplerGroup {
		weight := items[0].group.EffectiveWeight
		if weight < 1 { // weight can't be zero or less, sanity check
			weight = 1
		}
		return SamplerGroup{
			weight:  weight,
			items:   items,
			sumSize: sumSize,
		}
	}
	var res []SamplerGroup
	var sumWeight int64
	var i, j int
	sumSize := int64(s[0].Size)
	for j = 1; j < len(s); j++ {
		if s[i].group.ID != s[j].group.ID {
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

func partitionByMetric(s []SamplingMultiItemPair) ([]SamplerGroup, int64) {
	if len(s) == 0 {
		return nil, 0
	}
	newSamplerGroup := func(items []SamplingMultiItemPair, sumSize int64) SamplerGroup {
		weight := items[0].metric.EffectiveWeight
		if weight < 1 { // weight can't be zero or less, sanity check
			weight = 1
		}
		return SamplerGroup{
			MetricID:      items[0].MetricID,
			weight:        weight,
			items:         items,
			sumSize:       sumSize,
			roundFactors:  items[0].metric.RoundSampleFactors,
			noSampleAgent: items[0].metric.NoSampleAgent,
		}
	}
	var res []SamplerGroup
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

func partitionByKey(s []SamplingMultiItemPair) ([]SamplerGroup, int64) {
	if len(s) == 0 {
		return nil, 0
	}
	x := s[0].metric.FairKeyIndex
	if x < 0 || format.MaxTags <= x {
		weight := int64(format.EffectiveWeightOne)
		return []SamplerGroup{{weight: weight, items: s}}, weight
	}
	newSamplerGroup := func(items []SamplingMultiItemPair, sumSize int64) SamplerGroup {
		return SamplerGroup{
			MetricID:      items[0].MetricID,
			weight:        1,
			items:         items,
			sumSize:       sumSize,
			roundFactors:  items[0].metric.RoundSampleFactors,
			noSampleAgent: items[0].metric.NoSampleAgent,
		}
	}
	var res []SamplerGroup
	var sumWeight int64
	var i, j int
	sumSize := int64(s[0].Size)
	for j = 1; j < len(s); j++ {
		if s[i].fairKey != s[j].fairKey {
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

func (h *Sampler) getMetricMeta(metricID int32) *format.MetricMetaValue {
	if h.config.Meta == nil {
		return &h.nilMetric
	}
	if meta := h.config.Meta.GetMetaMetric(metricID); meta != nil {
		return meta
	}
	if meta := format.BuiltinMetrics[metricID]; meta != nil {
		return meta
	}
	return &h.nilMetric
}

func (h *Sampler) getGroupMeta(groupID int32) *format.MetricsGroup {
	if h.config.Meta == nil || groupID == 0 {
		return &h.nilGroup
	}
	if meta := h.config.Meta.GetGroup(groupID); meta != nil {
		return meta
	}
	return &h.nilGroup
}

func (s SamplerStatistics) GetSampleFactors(sf []tlstatshouse.SampleFactor) []tlstatshouse.SampleFactor {
	if s.Count == 0 {
		return sf
	}
	if sf == nil {
		sf = make([]tlstatshouse.SampleFactor, 0, s.Count)
	}
	for _, step := range s.Steps {
		sf = step.GetSampleFactors(sf)
	}
	return sf
}

func (s SamplerStep) GetSampleFactors(sf []tlstatshouse.SampleFactor) []tlstatshouse.SampleFactor {
	for _, b := range s.Groups {
		if b.SF > 1 {
			sf = append(sf, tlstatshouse.SampleFactor{
				Metric: b.MetricID,
				Value:  float32(b.SF),
			})
		}
	}
	return sf
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

// This function assumes structure of hour table with time = toStartOfHour(time)
// This turned out bad idea, so we do not use it anywhere now
func SampleFactorDeterministic(sampleFactors map[int32]float64, key Key, time uint32) (float64, bool) {
	sf, ok := sampleFactors[key.Metric]
	if !ok {
		return 1, true
	}
	// Deterministic sampling - we select random set of allowed keys per hour
	key.Metric = int32(time/3600) * 3600 // a bit of hack, we do not need metric here, so we replace it with toStartOfHour(time)
	ha := key.Hash()
	if float64(ha>>32)*sf < (1 << 32) { // 0.XXXX * sf < 1 condition shifted left by 32
		return sf, true
	}
	return 0, false
}

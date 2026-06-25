// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"encoding/binary"
	"math"
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
		I int32 // should always have priority over S
	}
	TagUnionBytes struct { // TODO - deprecate and remove
		S []byte
		I int32 // should always have priority over S
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
		MaxCounterHostTag TagUnion // Example hostname randomized according to distribution
	}

	ItemValue struct {
		ItemCounter
		ValueMin, ValueMax, ValueSum float64 // Aggregates of Value
		ValueSumSquare               float64 // Aggregates of Value
		MinHostTag                   TagUnion
		MaxHostTag                   TagUnion
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
		SF               float64  // set when Marshalling/Sampling
		GlobalSF         *float64 // BucketPartition SF
		Count            float64
		Size             uint32
		DupCnt           uint32 // duplicates count
		MetricMeta       *format.MetricMetaValue
	}

	MetricsBucket struct {
		Time uint32

		MultiItemMap
		CurSizes map[int32]map[string]*BucketSizeItem // once per key, no merge logic
		CurStats map[int32]*BucketStat                // -1 => others
	}

	BucketStat struct {
		Traffic    uint32
		KeepSize   uint32
		Partition  *BucketPartition                  // for fixed memory metric optimisation
		Partitions map[PartitionKey]*BucketPartition // mostly len=1. len>1 for fairKey and others
	}

	BucketSizeItem struct {
		key  string // reuse old keystring optimisation
		Size uint32
	}

	PartitionKey struct {
		ID   int32
		fair [maxFairKeyLen]int32
	}

	BucketPartition struct {
		Traffic     uint32
		Budget      uint32
		KeptTraffic uint32
		SF          *float64 // ptr to every MultiItem.GlobalSF

		TopSize   uint32
		TopSfLog2 int64
		Top       map[string]*MultiItem // unsafe string

		TailSize uint32
		Tail     map[string]*MultiItem // unsafe string
	}

	MultiItemMap struct {
		MultiItems map[string]*MultiItem // string is unsafe and points to part of the keysBuffer
		keysBuffer []byte
	}

	AgentEnvRouteArch struct {
		AgentEnv  int32
		Route     int32
		BuildArch int32
	}

	// TODO - better place?
	CreateMappingExtra struct {
		Create    bool
		Metric    string // set by old conveyor, TODO - remove?
		MetricID  int32  // set by new conveyor
		TagIDKey  int32
		ClientEnv int32
		Aera      AgentEnvRouteArch
		HostTag   TagUnion
	}
)

func (t *TagUnion) Normalize() {
	if t.I != 0 {
		t.S = ""
	}
}

func (t *TagUnionBytes) Normalize() {
	if t.I != 0 {
		t.S = nil
	}
}

func (t TagUnion) Empty() bool {
	return t.I == 0 && len(t.S) == 0
}

func (t TagUnionBytes) Empty() bool {
	return t.I == 0 && len(t.S) == 0
}

func (s *ItemCounter) Count() float64 { return s.counter }

func (k *Key) SetTagUnion(i int, tag TagUnion) {
	if tag.I != 0 {
		k.Tags[i] = tag.I
	} else {
		k.STags[i] = tag.S
	}
}

func (k *Key) RemoveStringTopTag() TagUnion {
	result := TagUnion{S: k.STags[format.StringTopTagIndexV3], I: k.Tags[format.StringTopTagIndexV3]}
	k.STags[format.StringTopTagIndexV3] = ""
	k.Tags[format.StringTopTagIndexV3] = 0
	return result
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

func (k *Key) AccountMetric() int32 {
	if k.Metric == format.BuiltinMetricIDIngestionStatus && k.Tags[1] != 0 {
		return k.Tags[1]
	}
	return k.Metric
}

func (k *Key) OutsideBudget() bool {
	return k.Metric == format.BuiltinMetricIDIngestionStatus && k.Tags[2] == format.TagValueIDSrcIngestionStatusOKCached
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

func SimpleItemValue(value float64, count float64, hostTag TagUnion) ItemValue {
	item := SimpleItemCounter(count, hostTag)
	item.addOnlyValue(value, count, hostTag)
	return item
}

func SimpleItemCounter(count float64, hostTag TagUnion) ItemValue {
	return ItemValue{ItemCounter: ItemCounter{counter: count, MaxCounterHostTag: hostTag}}
}

func (s *ItemValue) AddValue(value float64) {
	s.AddValueCounter(value, 1)
}

func (s *ItemValue) addOnlyValue(value float64, count float64, hostTag TagUnion) {
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
	s.addOnlyValue(value, count, TagUnion{})
}

func (s *ItemValue) AddValueCounterHost(rng *rand.Rand, value float64, count float64, hostTag TagUnion) {
	s.AddCounterHost(rng, count, hostTag)
	s.addOnlyValue(value, count, hostTag)
}

func (s *ItemValue) AddValueArrayHost(rng *rand.Rand, values []float64, mult float64, hostTag TagUnion) {
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

func (b *MetricsBucket) SampleOrCreateMultiItem(rng *rand.Rand, key *Key, metricInfo *format.MetricMetaValue, budgetID int32, budget uint32, count float64, keyBytes []byte) (item *MultiItem, created bool) {
	if budget == 0 || key.OutsideBudget() || metricInfo != nil && metricInfo.NoSampleAgent {
		return b.MultiItemMap.GetOrCreateMultiItem(key, metricInfo, keyBytes)
	}

	metricID := key.AccountMetric()
	wasLen := len(b.keysBuffer)
	if len(keyBytes) > 0 { // no need to marshall since we already have result
		b.keysBuffer = append(b.keysBuffer, keyBytes...)
		keyBytes = b.keysBuffer[wasLen:] // we want unsafe pointer into b.keysBuffer
	} else {
		b.keysBuffer, keyBytes = key.MarshalAppend(b.keysBuffer)
	}
	keyString := unsafe.String(unsafe.SliceData(keyBytes), len(keyBytes))

	root := b.CurStats[budgetID]
	if root == nil {
		root = &BucketStat{Partitions: map[PartitionKey]*BucketPartition{}}
		b.CurStats[budgetID] = root
	}
	decisionKey := samplingDecisionKey(key, metricInfo, metricID, budgetID)
	var part *BucketPartition
	if decisionKey.ID == 0 { // no-map optimisation
		if root.Partition == nil {
			sf := float64(1)
			root.Partition = &BucketPartition{SF: &sf, Tail: map[string]*MultiItem{}, Top: map[string]*MultiItem{}}
		}
		part = root.Partition
	} else {
		part = root.Partitions[decisionKey]
		if part == nil {
			sf := float64(1)
			part = &BucketPartition{SF: &sf, Tail: map[string]*MultiItem{}, Top: map[string]*MultiItem{}}
			root.Partitions[decisionKey] = part
		}
	}

	itemSize := uint32(key.TLSizeEstimate(key.Timestamp))
	part.Traffic += itemSize
	root.Traffic += itemSize

	if item = part.Top[keyString]; item != nil {
		item.Count += count
		item.DupCnt++
		part.KeptTraffic += item.Size
		b.keysBuffer = b.keysBuffer[:wasLen]
		return item, false
	}
	if item = part.Tail[keyString]; item != nil {
		item.Count += count
		itemSize = item.Size
		if v := b.sampleTop(rng, part, part.Budget/2, &item.Key, item.MetricMeta, keyString, item, item.Count, item.Size); v != nil { // try move to top
			b.removeTail(part, keyString, false)
		} else if item.Size != 0 {
			item.DupCnt++
			part.KeptTraffic += item.Size
		} else { // we could full lose it after sample
			if _, ok := part.Top[keyString]; ok { // item deleted in tail, might stay in top
				if itemSize >= part.TopSize {
					part.TopSize = 0
				} else {
					part.TopSize -= itemSize
				}
				delete(part.Top, keyString)
			}
			return nil, false
		}
		b.keysBuffer = b.keysBuffer[:wasLen]
		return item, false
	}

	sizes := b.CurSizes[metricID]
	if sizes == nil {
		sizes = map[string]*BucketSizeItem{}
		b.CurSizes[metricID] = sizes
	}
	created = true
	if size, ok := sizes[keyString]; ok {
		keyString = size.key // use old keystring
		b.keysBuffer = b.keysBuffer[:wasLen]
		created = false
	} else {
		sizes[keyString] = &BucketSizeItem{key: keyString, Size: itemSize}
	}

	part.Budget = budget
	halfBudget := budget // get full root budget, until fit budget
	if root.KeepSize >= budget {
		if len(root.Partitions) > 0 {
			part.Budget = uint32(math.Round(float64(budget) / float64(len(root.Partitions))))
		}
		halfBudget = uint32(math.Round(float64(part.Budget) / 2))
	}

	root.KeepSize -= part.TopSize + part.TailSize
	if item = b.sampleTop(rng, part, halfBudget, key, metricInfo, keyString, nil, count, itemSize); item != nil { // not create item if sample optimisation
		root.KeepSize += part.TopSize + part.TailSize
		root.recalc(rng, b, budget, part.Budget)
		return item, created
	}
	root.KeepSize += part.TopSize
	if item = b.sampleTail(rng, part, halfBudget, key, metricInfo, keyString, nil, count, itemSize); item != nil { // not create item if sample optimisation
		root.KeepSize += part.TailSize
		root.recalc(rng, b, budget, part.Budget)
		return item, created
	}
	root.KeepSize += part.TailSize
	return nil, created
}

func (p *BucketPartition) SetSampleFactor() {
	if p.KeptTraffic == 0 || p.Traffic <= p.KeptTraffic {
		*p.SF = 1
		return
	}
	*p.SF = float64(p.Traffic) / float64(p.KeptTraffic)
}

func (s *BucketStat) recalc(rng *rand.Rand, b *MetricsBucket, totalBudget, partBudget uint32) {
	if totalBudget >= s.KeepSize {
		return // do not delete everything
	}
	f := func(p *BucketPartition) {
		if p.TopSize+p.TailSize < partBudget*2 {
			return // no need recalc
		}
		p.Budget = partBudget
		halfBudget := uint32(math.Round(float64(partBudget) / 2))

		s.KeepSize -= p.TopSize
		s.KeepSize -= p.TailSize
		for p.TopSize > halfBudget && len(p.Top) != 0 {
			p.resampleTop(rng, b, halfBudget)
		}
		for p.TailSize > halfBudget && len(p.Tail) > 1 {
			b.removeRandomTail(p)
		}
		s.KeepSize += p.TopSize
		s.KeepSize += p.TailSize
	}
	if s.Partition != nil {
		f(s.Partition)
		return
	}
	for _, p := range s.Partitions {
		f(p)
		if totalBudget >= s.KeepSize {
			break // do not delete everything
		}
	}
}

func samplingDecisionKey(key *Key, metricInfo *format.MetricMetaValue, metricID, budgetID int32) PartitionKey {
	if budgetID == -1 {
		return PartitionKey{ID: metricID}
	} // no fair logic for common metrics
	if metricInfo == nil || len(metricInfo.FairKeyIndex) == 0 {
		return PartitionKey{}
	}

	var pk = PartitionKey{ID: metricID}
	n := min(len(metricInfo.FairKeyIndex), maxFairKeyLen)
	for i := 0; i < n; i++ {
		if x := metricInfo.FairKeyIndex[i]; 0 <= x && x < len(key.Tags) {
			pk.fair[i] = key.Tags[x]
		}
	}
	return pk
}

func (b *MetricsBucket) sampleTail(rng *rand.Rand, part *BucketPartition, budget uint32, key *Key, meta *format.MetricMetaValue, keyString string, item *MultiItem, count float64, size uint32) *MultiItem {
	if part.Traffic > budget && rng.Float64()*float64(part.Traffic) >= float64(budget) {
		b.removeTail(part, keyString, true)
		return nil
	}
	if item == nil {
		item = &MultiItem{Key: *key, Size: size, SF: 1, GlobalSF: part.SF, Count: count, MetricMeta: meta}
	}
	part.TailSize += item.Size
	item.DupCnt++
	part.KeptTraffic += item.Size
	part.Tail[keyString] = item
	b.MultiItems[keyString] = item
	for part.TailSize > budget && len(part.Tail) != 0 {
		b.removeRandomTail(part)
	}
	if item.Size == 0 { // removeRandomTail could remove item
		return nil
	}
	return item
}

func (b *MetricsBucket) removeRandomTail(part *BucketPartition) {
	if len(part.Tail) == 0 {
		return
	}
	i := len(part.Tail) / 2
	for k := range part.Tail { // quasirandom remove, quite ok for O(1)
		b.removeTail(part, k, true)
		if i--; i <= 0 {
			break
		}
	}
}

func (b *MetricsBucket) removeTail(part *BucketPartition, key string, fullDelete bool) {
	if item, ok := part.Tail[key]; ok {
		if item.Size >= part.TailSize {
			part.TailSize = 0
		} else {
			part.TailSize -= item.Size
		}
		delete(part.Tail, key)
	}
	if item, ok := b.MultiItems[key]; ok && fullDelete {
		if traffic := item.Size * item.DupCnt; traffic >= part.KeptTraffic {
			part.KeptTraffic = 0
		} else {
			part.KeptTraffic -= traffic
		}
		item.Size = 0 // optimisation to avoid map lookup
		delete(b.MultiItems, key)
	}
}

func (b *MetricsBucket) sampleTop(rng *rand.Rand, part *BucketPartition, budget uint32, key *Key, meta *format.MetricMetaValue, keyString string, item *MultiItem, count float64, size uint32) *MultiItem {
	sf := 1 << part.TopSfLog2
	if part.TopSfLog2 != 0 && count < float64(sf) {
		if rng.Float64()*float64(sf) >= count {
			return nil
		}
	}
	if item == nil {
		item = &MultiItem{Key: *key, Size: size, SF: 1, GlobalSF: part.SF, Count: count, MetricMeta: meta}
	}
	part.TopSize += item.Size
	item.DupCnt++
	part.KeptTraffic += item.Size
	part.Top[keyString] = item
	b.MultiItems[keyString] = item
	for part.TopSize > budget && len(part.Top) != 0 {
		part.resampleTop(rng, b, budget)
	}
	if item.Size == 0 { // resampleTop could drop item to tail
		return nil
	}
	return item
}

const maxTopSfLog2 = 61

func (p *BucketPartition) resampleTop(rng *rand.Rand, b *MetricsBucket, tailBudget uint32) {
	i := 0
	was := len(p.Top)
	for k, v := range p.Top {
		if p.TopSfLog2 < maxTopSfLog2 { // if overflow just drop half random
			cc := 2 << p.TopSfLog2
			if v.Count >= float64(cc) {
				continue
			}
			rv := rng.Intn(cc)
			if v.Count > float64(rv) {
				continue
			}
		}
		if v.Size >= p.TopSize {
			p.TopSize = 0
		} else {
			p.TopSize -= v.Size
		}
		v.DupCnt--
		if v.Size >= p.KeptTraffic {
			p.KeptTraffic = 0
		} else {
			p.KeptTraffic -= v.Size
		}
		delete(p.Top, k)
		b.sampleTail(rng, p, tailBudget, &v.Key, v.MetricMeta, k, v, v.Count, v.Size) // move to tail
		if i++; i >= was/2 {
			return // for remain low items
		}
	}
	if p.TopSfLog2 < maxTopSfLog2 {
		p.TopSfLog2++
	}
}

func (b *MetricsBucket) Clear() {
	clear(b.CurStats)
	clear(b.CurSizes)
	b.keysBuffer = []byte{}
	b.MultiItems = map[string]*MultiItem{}
}

func (b *MultiItemMap) GetOrCreateMultiItem(key *Key, metricInfo *format.MetricMetaValue, keyBytes []byte) (item *MultiItem, created bool) {
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
	item = &MultiItem{Key: *key, SF: 1, MetricMeta: metricInfo}
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
	if tag.Empty() {
		return &s.Tail
	}
	tag.Normalize() // important here
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
	if tag.Empty() {
		return &s.Tail
	}
	tag.Normalize() // important here
	if s.Top == nil {
		s.Top = map[TagUnion]*MultiValue{}
	}
	unsafeTagS := unsafe.String(unsafe.SliceData(tag.S), len(tag.S)) // avoid allocation for existing tag
	c, ok := s.Top[TagUnion{S: unsafeTagS, I: tag.I}]
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

func (s *MultiItem) TLSize() uint32 {
	return uint32(s.Key.TLSizeEstimate(s.Key.Timestamp) + s.TLSizeEstimate())
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
	s.Value.AddCounterHost(rng, count, TagUnion{})
}

func (s *MultiValue) AddCounterHost(rng *rand.Rand, count float64, hostTag TagUnion) {
	s.Value.AddCounterHost(rng, count, hostTag)
}

func (s *MultiValue) AddValueCounter(rng *rand.Rand, value float64, count float64) {
	s.Value.AddValueCounterHost(rng, value, count, TagUnion{})
}

func (s *MultiValue) AddValueCounterHost(rng *rand.Rand, value float64, count float64, hostTag TagUnion) {
	s.Value.AddValueCounterHost(rng, value, count, hostTag)
}

func (s *MultiValue) AddValueCounterHostPercentile(rng *rand.Rand, value float64, count float64, hostTag TagUnion, compression float64) {
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
func (s *MultiValue) AddValueCounterHostPercentileLegacy(rng *rand.Rand, value float64, count float64, hostTag TagUnion, compression float64) {
	s.Value.AddValueCounterHost(rng, value, count, hostTag)
	if s.ValueTDigest == nil {
		s.ValueTDigest = tdigest.NewWithCompression(compression)
	}
	s.ValueTDigest.Add(value, count)
}

func (s *MultiValue) ApplyValues(rng *rand.Rand, histogram [][2]float64, values []float64, count float64, totalCount float64, hostTag TagUnion, compression float64, hasPercentiles bool) {
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
func (s *MultiValue) ApplyValuesLegacy(rng *rand.Rand, histogram [][2]float64, values []float64, count float64, totalCount float64, hostTag TagUnion, compression float64, hasPercentiles bool) {
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

func (s *MultiValue) ApplyUnique(rng *rand.Rand, hashes []int64, count float64, hostTag TagUnion) {
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

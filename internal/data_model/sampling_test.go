package data_model

import (
	"fmt"
	"math"
	"os"
	"sort"
	"testing"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/stretchr/testify/require"
	"pgregory.net/rand"
	"pgregory.net/rapid"
)

func TestSampling(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		type metricInfo struct {
			id    int32
			sf    float32
			maxSF float32
			minSF float32
			size  int64
		}
		b := newSamplingTestBucket()
		b.generateSeriesSize(t, samplingTestSpec{
			maxMetricCount: rapid.IntRange(1, 1024).Draw(t, "max metric count"),
			minMetricSize:  rapid.IntRange(28, 256).Draw(t, "min metric size"),
			maxMetricSize:  rapid.IntRange(512, 1024).Draw(t, "max metric size"),
		})
		var keepN, discardN int
		var keepSumSize int64
		m := make(map[int32]*metricInfo)
		s := NewSampler(SamplerConfig{
			KeepF: func(item *MultiItem, _ uint32) {
				keepN++
				keepSumSize += int64(samplingTestSizeOf(item))
				stat := m[item.Key.Metric]
				require.LessOrEqual(t, 1., item.SF)
				require.LessOrEqual(t, stat.maxSF, float32(item.SF))
				if item.SF > 1 {
					if stat.minSF == 0 || stat.minSF > float32(item.SF) {
						stat.minSF = float32(item.SF)
					}
					if stat.maxSF < float32(item.SF) {
						stat.maxSF = float32(item.SF)
					}
				}
			},
			DiscardF: func(item *MultiItem, _ uint32) {
				discardN++
				b.DeleteMultiItem(&item.Key)
				stat := m[item.Key.Metric]
				require.LessOrEqual(t, 1., item.SF)
				require.LessOrEqual(t, stat.maxSF, float32(item.SF))
				if item.SF > 1 {
					if stat.minSF == 0 || stat.minSF > float32(item.SF) {
						stat.minSF = float32(item.SF)
					}
					if stat.maxSF < float32(item.SF) {
						stat.maxSF = float32(item.SF)
					}
				}
			},
			SelectF: func(s []SamplingMultiItemPair, sf float64, _ *rand.Rand) int {
				return int(float64(len(s)) / sf)
			},
		})
		for _, item := range b.MultiItems {
			var v *metricInfo
			metric := item.Key.Metric
			if v = m[metric]; v == nil {
				v = &metricInfo{id: metric}
				m[metric] = v
			}
			v.size += int64(samplingTestSizeOf(item))
		}
		budget := rapid.Int64Range(20, 20+b.sumSize*2).Draw(t, "budget")
		metricCount := len(b.MultiItems)
		b.run(&s, budget)
		require.LessOrEqual(t, keepSumSize, budget)
		require.Equal(t, metricCount, keepN+discardN, "some series were neither keeped nor discarded")
		if b.sumSize <= budget {
			require.Zero(t, discardN)
			require.Equal(t, metricCount, keepN)
		} else {
			require.NotZero(t, discardN)
		}
		for _, v := range s.SampleFactors {
			stat := m[v.Metric]
			require.Less(t, float32(1), v.Value, "SF less or equal one should not be reported")
			require.LessOrEqualf(t, v.Value, stat.minSF, "Reported SF shouldn't take whales into account")
			stat.sf = v.Value
		}
		// the more size the more sample factor
		x := make([]*metricInfo, 0, len(m))
		for _, v := range m {
			maxSF := float32(float64(v.size*int64(len(m))) / float64(budget))
			require.LessOrEqualf(t, v.sf, maxSF, "Reported SF is out of bound")
			x = append(x, v)
		}
		sort.Slice(x, func(i, j int) bool {
			if x[i].size != x[j].size {
				return x[i].size < x[j].size
			}
			return x[i].sf < x[j].sf
		})
		for i := 1; i < len(x); i++ {
			require.LessOrEqual(t, x[i-1].sf, x[i].sf)
		}
	})
}

func TestSamplingWithNilKeepF(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		b := newSamplingTestBucket()
		b.generateSeriesSize(t, samplingTestSpec{
			maxMetricCount: rapid.IntRange(1, 1024).Draw(t, "max metric count"),
			minMetricSize:  rapid.IntRange(28, 256).Draw(t, "min metric size"),
			maxMetricSize:  rapid.IntRange(512, 1024).Draw(t, "max metric size"),
		})
		s := NewSampler(SamplerConfig{
			KeepF: nil, // agent doesn't set it
			DiscardF: func(item *MultiItem, _ uint32) {
				b.DeleteMultiItem(&item.Key)
			},
			SelectF: func(s []SamplingMultiItemPair, sf float64, _ *rand.Rand) int {
				return int(float64(len(s)) / sf)
			},
			RoundF: func(sf float64, _ *rand.Rand) float64 {
				floor := math.Floor(sf)
				delta := sf - floor
				if rapid.Float64Range(0, 1).Draw(t, "RoundF") < delta {
					return floor + 1
				}
				return floor
			},
		})
		budget := rapid.Int64Range(20, 20+b.sumSize*2).Draw(t, "budget")
		b.run(&s, budget)
		m := map[int32]float64{}
		for _, item := range b.MultiItems {
			require.Less(t, 0., item.SF)
			if item.SF < m[item.Key.Metric] {
				m[item.Key.Metric] = item.SF
			}
		}
		for _, v := range s.SampleFactors {
			if sf, ok := m[v.Metric]; ok {
				require.Truef(t, v.Value == float32(sf), "Item SF %v, metric SF %v", sf, v.Value)
				delete(m, v.Metric)
			}
			require.Less(t, float32(1), v.Value, "SF less or equal one should not be reported")
		}
		for _, item := range b.MultiItems {
			require.LessOrEqual(t, 1., item.SF)
		}
	})
}

func TestNoSamplingWhenFitBudget(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		b := newSamplingTestBucket()
		b.generateSeriesCount(t, samplingTestSpec{maxSeriesCount: 256, maxMetricCount: 256})
		var (
			s = NewSampler(SamplerConfig{
				KeepF: func(item *MultiItem, _ uint32) {
					b.DeleteMultiItem(&item.Key)
				},
				DiscardF: func(mi *MultiItem, _ uint32) {
					t.Fatal("budget is enough but series were discarded")
				},
			})
		)
		b.run(&s, b.sumSize)
		require.Empty(t, b.MultiItems, "missing keep")
		require.Empty(t, s.SampleFactors, "sample factors aren't empty")
	})
}

func TestNormalDistributionPreserved(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var (
			b     = newSamplingTestBucket()
			r     = rand.New()
			statM = make(map[Key]*samplingTestStat, len(b.MultiItems))
			keepF = func(item *MultiItem, _ uint32) {
				var s *samplingTestStat
				if s = statM[item.Key]; s == nil {
					s = &samplingTestStat{}
					statM[item.Key] = s
				}
				s.aggregate(item)
			}
		)
		b.generateSeriesCount(t, samplingTestSpec{maxMetricCount: 10, maxSeriesCount: 10})
		for i := 0; i < 1024; i++ {
			b.generateNormValues(r)
			s := NewSampler(SamplerConfig{KeepF: keepF, Rand: r})
			b.run(&s, b.sumSize/2) // budget is half size
		}
		for _, v := range statM {
			// NormFloat64 generates standard normal distribution with mean = 0, stddev = 1
			s := v.stdDev()
			require.Greater(t, s, 0.)
			require.Less(t, s, 2.)
		}
	})
}

func TestFairKeySampling(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// generate series
		b := newSamplingTestBucket()
		key := Key{Metric: 1}
		n := rapid.IntRange(1, 16).Draw(t, "fair key value count")
		for i, v := 1, 1; i <= n; i, v = i+1, v*2 {
			key.Tags[0] = int32(i)
			for j := 1; j <= v; j++ {
				key.Tags[1] = int32(j)
				v := &MultiItem{}
				mi, _ := b.GetOrCreateMultiItem(&key, nil, 1, nil)
				mi.Tail.Value.AddValueCounter(0, 1)
				b.sumSize += int64(key.TLSizeEstimate(key.Timestamp) + v.TLSizeEstimate())
			}
		}
		// run sampling with budget enouph to store single value for each fair key
		keepCount := make(map[Key]int)
		sampler := NewSampler(SamplerConfig{
			SampleKeys: true,
			Meta: metaStorageMock{
				getMetaMetric: func(metricID int32) *format.MetricMetaValue {
					return &format.MetricMetaValue{
						FairKey: []int{0},
					}
				},
			},
			RoundF: func(sf float64, _ *rand.Rand) float64 {
				require.Equal(t, sf, math.Floor(sf))
				return sf
			},
			SelectF: func(s []SamplingMultiItemPair, sf float64, _ *rand.Rand) int {
				require.Equal(t, sf, math.Floor(sf))
				return int(float64(len(s)) / sf)
			},
			KeepF: func(item *MultiItem, u uint32) {
				keepCount[item.Key]++
			},
		})
		budget := b.sumSize * int64(n) / int64(len(b.MultiItems))
		b.run(&sampler, budget)
		// sampler should keep single value for each fair key
		require.Equal(t, n, len(keepCount))
		for _, v := range keepCount {
			require.Equal(t, 1, v)
		}
	})
}

type metaStorageMock struct {
	version             func() int64
	stateHash           func() string
	getMetaMetric       func(metricID int32) *format.MetricMetaValue
	getMetaMetricByName func(metricName string) *format.MetricMetaValue
	getGroup            func(id int32) *format.MetricsGroup
	getNamespace        func(id int32) *format.NamespaceMeta
	getNamespaceByName  func(name string) *format.NamespaceMeta
	getGroupByName      func(name string) *format.MetricsGroup
}

func (m metaStorageMock) Version() int64 {
	return m.version()
}

func (m metaStorageMock) StateHash() string {
	return m.stateHash()
}

func (m metaStorageMock) GetMetaMetric(metricID int32) *format.MetricMetaValue {
	return m.getMetaMetric(metricID)
}

func (m metaStorageMock) GetMetaMetricByName(metricName string) *format.MetricMetaValue {
	return m.getMetaMetricByName(metricName)
}

func (m metaStorageMock) GetGroup(id int32) *format.MetricsGroup {
	return m.getGroup(id)
}

func (m metaStorageMock) GetNamespace(id int32) *format.NamespaceMeta {
	return m.getNamespace(id)
}

func (m metaStorageMock) GetNamespaceByName(name string) *format.NamespaceMeta {
	return m.getNamespaceByName(name)
}

func (m metaStorageMock) GetGroupByName(name string) *format.MetricsGroup {
	return m.getGroupByName(name)
}

func TestCompareSampleFactors(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		b := newSamplingTestBucket()
		b.generateSeriesSize(t, samplingTestSpec{
			maxMetricCount: rapid.IntRange(1, 1024).Draw(t, "max metric count"),
			minMetricSize:  rapid.IntRange(28, 256).Draw(t, "min metric size"),
			maxMetricSize:  rapid.IntRange(512, 1024).Draw(t, "max metric size"),
		})
		var sumSize int
		for _, item := range b.MultiItems {
			sumSize += samplingTestSizeOf(item)
		}
		bucket := MetricsBucket{MultiItemMap: b.MultiItemMap}
		config := samplerConfigEx{
			SamplerConfig: SamplerConfig{
				SelectF: func(s []SamplingMultiItemPair, sf float64, r *rand.Rand) int {
					return len(s) / int(sf)
				},
			},
			stringTopCountSend: 20,
			numShards:          1,
			sampleBudget:       rapid.IntRange(20, 20+sumSize*2).Draw(t, "max metric count"),
		}
		sizeSum := map[int]int{}
		config.KeepF = func(mi *MultiItem, _ uint32) {
			sizeSum[int(mi.Key.Metric)] += samplingTestSizeOf(mi)
		}
		sf := sampleBucket(&bucket, config)
		sizeSumLegacy := map[int]int{}
		config.KeepF = func(mi *MultiItem, _ uint32) {
			sizeSumLegacy[int(mi.Key.Metric)] += samplingTestSizeOf(mi)
		}
		sfLegacy := sampleBucketLegacy(&bucket, config)
		normSF := func(s []tlstatshouse.SampleFactor) []tlstatshouse.SampleFactor {
			if s == nil {
				return make([]tlstatshouse.SampleFactor, 0)
			}
			sort.Slice(s, func(i, j int) bool {
				return s[i].Metric < s[j].Metric
			})
			return s
		}
		require.Equalf(t, normSF(sfLegacy), normSF(sf), "Sample factors mistmatch!")
		require.Equalf(t, sizeSumLegacy, sizeSum, "Size sum mistmatch!")
	})
}

func TestSelectRandom(t *testing.T) {
	if os.Getenv("STATSHOUSE_TEST_SELECT_RANDOM") == "1" {
		testSelectRandom(t, selectRandom)
	}
}

func TestSelectRandom2(t *testing.T) {
	if os.Getenv("STATSHOUSE_TEST_SELECT_RANDOM") == "1" {
		testSelectRandom(t, selectRandom)
	}
}

func testSelectRandom(t *testing.T, fn func([]SamplingMultiItemPair, float64, *rand.Rand) int) {
	rapid.Check(t, func(t *rapid.T) {
		var (
			n = rapid.IntRange(1, 1024).Draw(t, "number of items")
			f = rapid.Float64().Draw(t, "sample factor")
			k = fn(make([]SamplingMultiItemPair, n), f, rand.New())
		)
		if 1 < f {
			require.LessOrEqual(t, 0, k)
			require.LessOrEqual(t, k, n)
			require.LessOrEqual(t, math.Floor(float64(n)/f), float64(k))
			require.LessOrEqual(t, float64(k), math.Ceil(float64(n)/f))
		} else {
			require.Equal(t, n, k)
		}
	})
}

type samplingTestSpec struct {
	minMetricCount int
	maxMetricCount int
	minSeriesCount int
	maxSeriesCount int
	minMetricSize  int
	maxMetricSize  int
}

type samplingTestBucket struct {
	MultiItemMap
	sumSize int64
}

func newSamplingTestBucket() samplingTestBucket {
	return samplingTestBucket{
		// series: make(map[string]*MultiItem),
	}
}

func (b *samplingTestBucket) generateSeriesCount(t *rapid.T, s samplingTestSpec) {
	var (
		metricCount  = rapid.IntRange(s.minMetricCount, s.maxMetricCount).Draw(t, "number of metrics")
		seriesCountG = rapid.IntRange(s.minSeriesCount, s.maxSeriesCount)
		miMap        = MultiItemMap{}
		sumSize      int64
	)
	for i := 0; i < metricCount; i++ {
		var (
			metricID    = int32(i + 1)
			seriesCount = seriesCountG.Draw(t, fmt.Sprintf("#%d number of series", metricID))
		)
		for i := 0; i < seriesCount; i++ {
			var (
				k = Key{Metric: metricID, Tags: [format.MaxTags]int32{int32(i + 1)}}
			)
			mi, _ := miMap.GetOrCreateMultiItem(&k, nil, 1, nil)
			mi.Tail.Value.AddValueCounter(0, 1)
			sumSize += int64(k.TLSizeEstimate(k.Timestamp) + mi.TLSizeEstimate())
		}
	}
	b.MultiItemMap = miMap
	b.sumSize = sumSize
}

func (b *samplingTestBucket) generateSeriesSize(t *rapid.T, s samplingTestSpec) {
	var (
		metricCount = rapid.IntRange(s.minMetricCount, s.maxMetricCount).Draw(t, "number of metrics")
		metricSizeG = rapid.IntRange(s.minMetricSize, s.maxMetricSize)
		miMap       = MultiItemMap{}
		sumSize     int64
	)
	for i := 0; i < metricCount; i++ {
		var (
			metricID = int32(i + 1)
			sizeT    = metricSizeG.Draw(t, fmt.Sprintf("#%d series size", metricID)) // target
			size     int                                                             // current
		)
		for i := int32(1); size < sizeT; i++ {
			var k = Key{Metric: metricID, Tags: [format.MaxTags]int32{i}}
			item, _ := miMap.GetOrCreateMultiItem(&k, nil, 1, nil)
			item.Tail.Value.AddValueCounter(0, 1)
			size += samplingTestSizeOf(item)
		}
		sumSize += int64(size)
	}
	b.MultiItemMap = miMap
	b.sumSize = sumSize
}

func (b *samplingTestBucket) generateNormValues(r *rand.Rand) {
	for _, v := range b.MultiItems {
		v.Tail.Value = SimpleItemCounter(r.NormFloat64(), TagUnionBytes{})
	}
}

func (b *samplingTestBucket) run(s *sampler, budget int64) {
	for _, item := range b.MultiItems {
		s.Add(SamplingMultiItemPair{
			Item:        item,
			WhaleWeight: item.FinishStringTop(rand.New(), 20),
			Size:        samplingTestSizeOf(item),
			MetricID:    item.Key.Metric,
		})
	}
	s.Run(budget)
}

func samplingTestSizeOf(mi *MultiItem) int {
	return mi.Key.TLSizeEstimate(mi.Key.Timestamp) + mi.TLSizeEstimate()
}

type samplingTestStat struct {
	sum   float64
	sumSq float64
	n     int
}

func (s *samplingTestStat) aggregate(item *MultiItem) {
	v := item.Tail.Value.Count()
	s.sum += v
	s.sumSq += v * v
	s.n++
}

func (s *samplingTestStat) stdDev() float64 {
	return math.Sqrt(s.sumSq / float64(s.n)) // assumed mean = 0
}

type samplerConfigEx struct {
	SamplerConfig
	stringTopCountSend int
	numShards          int
	sampleBudget       int
}

type samplingMetric struct {
	metricID      int32
	metricWeight  int64 // actually, effective weight
	roundFactors  bool
	noSampleAgent bool
	sumSize       int64
	items         []SamplingMultiItemPair
}

func BenchmarkSampleBucket(b *testing.B) {
	if os.Getenv("STATSHOUSE_BENCHMARK_LEGACY_SAMPLE_BUCKET") == "1" {
		benchmarkSampleBucket(b, sampleBucketLegacy)
	} else {
		benchmarkSampleBucket(b, sampleBucket)
	}
}

func benchmarkSampleBucket(b *testing.B, f func(*MetricsBucket, samplerConfigEx) []tlstatshouse.SampleFactor) {
	var (
		metricCount = 2000
		seriesCount = 2000
		bucket      = MetricsBucket{}
		config      = samplerConfigEx{
			SamplerConfig: SamplerConfig{
				SampleFactorF: func(int32, float64) {},
				Rand:          rand.New(),
			},
			stringTopCountSend: 20,
			numShards:          1,
			sampleBudget:       10,
		}
	)
	for i := 0; i < metricCount; i++ {
		metricID := int32(i + 1)
		for i := 0; i < seriesCount; i++ {
			var (
				k = Key{Metric: metricID, Tags: [format.MaxTags]int32{int32(i + 1)}}
			)
			mi, _ := bucket.GetOrCreateMultiItem(&k, nil, 1, nil)
			mi.Tail.Value.AddValueCounter(0, 1)
		}
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		f(&bucket, config)
	}
}

func sampleBucket(bucket *MetricsBucket, config samplerConfigEx) []tlstatshouse.SampleFactor {
	sampler := NewSampler(config.SamplerConfig)
	for _, item := range bucket.MultiItems {
		whaleWeight := item.FinishStringTop(rand.New(), config.stringTopCountSend) // all excess items are baked into Tail
		accountMetric := item.Key.Metric
		sz := item.Key.TLSizeEstimate(bucket.Time) + item.TLSizeEstimate()
		if item.Key.Metric == format.BuiltinMetricIDIngestionStatus {
			if item.Key.Tags[1] != 0 {
				// Ingestion status and other unlimited per-metric built-ins should use its metric budget
				// So metrics are better isolated
				accountMetric = item.Key.Tags[1]
				whaleWeight = 0 // ingestion statuses do not compete for whale status
			}
			if item.Key.Tags[2] == format.TagValueIDSrcIngestionStatusOKCached {
				// These are so common, we have transfer optimization for them
				sz = 3 * 4 // see statshouse.ingestion_status2
			}
		}
		sampler.Add(SamplingMultiItemPair{
			Item:        item,
			WhaleWeight: whaleWeight,
			Size:        sz,
			MetricID:    accountMetric,
		})
	}
	numShards := config.numShards
	remainingBudget := int64((config.sampleBudget + numShards - 1) / numShards)
	sampler.Run(remainingBudget)
	return sampler.SampleFactors
}

func sampleBucketLegacy(bucket *MetricsBucket, config samplerConfigEx) []tlstatshouse.SampleFactor {
	// Same algorithm as in aggregator, but instead of inserting selected, we remove items which were not selected by sampling algorithm
	metricsMap := map[int32]*samplingMetric{}
	var metricsList []*samplingMetric
	totalItemsSize := 0
	var remainingWeight int64

	for _, item := range bucket.MultiItems {
		whaleWeight := item.FinishStringTop(rand.New(), config.stringTopCountSend) // all excess items are baked into Tail, config.StringTopCountSend
		accountMetric := item.Key.Metric
		sz := item.Key.TLSizeEstimate(bucket.Time) + item.TLSizeEstimate()
		if item.Key.Metric == format.BuiltinMetricIDIngestionStatus {
			if item.Key.Tags[1] != 0 {
				// Ingestion status and other unlimited per-metric built-ins should use its metric budget
				// So metrics are better isolated
				accountMetric = item.Key.Tags[1]
				whaleWeight = 0 // ingestion statuses do not compete for whale status
			}
			if item.Key.Tags[2] == format.TagValueIDSrcIngestionStatusOKCached {
				// These are so common, we have transfer optimization for them
				sz = 3 * 4 // see statshouse.ingestion_status2
			}
		}

		metric, ok := metricsMap[accountMetric]
		if !ok {
			var metricInfo *format.MetricMetaValue
			if config.Meta != nil {
				metricInfo = config.Meta.GetMetaMetric(accountMetric)
			}
			metric = &samplingMetric{
				metricID:     accountMetric,
				metricWeight: format.EffectiveWeightOne,
				roundFactors: false, // default is no rounding
			}
			if metricInfo != nil {
				metric.metricWeight = metricInfo.EffectiveWeight
				metric.roundFactors = metricInfo.RoundSampleFactors
				metric.noSampleAgent = metricInfo.NoSampleAgent
			}
			metricsMap[accountMetric] = metric
			metricsList = append(metricsList, metric)
			remainingWeight += metric.metricWeight
		}
		metric.sumSize += int64(sz)
		metric.items = append(metric.items, SamplingMultiItemPair{Item: item, WhaleWeight: whaleWeight})
		totalItemsSize += sz
	}

	sort.Slice(metricsList, func(i, j int) bool {
		// comparing rational numbers
		return metricsList[i].sumSize*metricsList[j].metricWeight < metricsList[j].sumSize*metricsList[i].metricWeight
	})
	numShards := config.numShards
	remainingBudget := int64((config.sampleBudget + numShards - 1) / numShards)
	if remainingBudget <= 0 { // if happens, will lead to divide by zero below, so we add cheap protection
		remainingBudget = 1
	}
	if remainingBudget > MaxUncompressedBucketSize/2 { // Algorithm is not exact
		remainingBudget = MaxUncompressedBucketSize / 2
	}
	var sampleFactors []tlstatshouse.SampleFactor
	pos := 0
	for ; pos < len(metricsList) && metricsList[pos].sumSize*remainingWeight <= remainingBudget*metricsList[pos].metricWeight; pos++ { // statIdCount <= totalBudget/remainedStats
		samplingMetric := metricsList[pos]
		// No sampling for this stat - do not add to samplingThresholds
		remainingBudget -= samplingMetric.sumSize
		remainingWeight -= samplingMetric.metricWeight
		// Keep all elements in bucket
		if config.KeepF != nil {
			for _, v := range samplingMetric.items {
				config.KeepF(v.Item, bucket.Time)
			}
		}
	}
	for i := pos; i < len(metricsList); i++ {
		samplingMetric := metricsList[i]
		if samplingMetric.noSampleAgent {
			if config.KeepF != nil {
				for _, v := range samplingMetric.items {
					config.KeepF(v.Item, bucket.Time)
				}
			}
			continue
		}
		sf := float64(samplingMetric.sumSize*remainingWeight) / float64(samplingMetric.metricWeight*remainingBudget)
		if samplingMetric.roundFactors {
			sf = roundSampleFactor(sf, config.Rand)
			if sf <= 1 { // Many sample factors are between 1 and 2, so this is worthy optimization
				if config.KeepF != nil {
					for _, v := range samplingMetric.items {
						config.KeepF(v.Item, bucket.Time)
					}
				}
				continue
			}
		}
		sampleFactors = append(sampleFactors, tlstatshouse.SampleFactor{
			Metric: samplingMetric.metricID,
			Value:  float32(sf),
		})
		whalesAllowed := int64(0)
		if samplingMetric.sumSize*remainingWeight > 0 { // should be never but check is cheap
			whalesAllowed = int64(len(samplingMetric.items)) * (samplingMetric.metricWeight * remainingBudget) / (samplingMetric.sumSize * remainingWeight) / 2 // len(items) / sf / 2
		}
		// Motivation - often we have a few rows with dominating counts (whales). If we randomly discard those rows, we get wild fluctuation
		// of sums. On the other hand if we systematically discard rows with small counts, rare events, like errors cannot get through.
		// So we allow half of sampling budget for whales, and the other half is spread fairly between other events.
		// TODO - model this approach. Adjust algorithm parameters.
		if whalesAllowed > 0 {
			if whalesAllowed > int64(len(samplingMetric.items)) { // should be never but check is cheap
				whalesAllowed = int64(len(samplingMetric.items))
			}
			sort.Slice(samplingMetric.items, func(i, j int) bool {
				return samplingMetric.items[i].WhaleWeight > samplingMetric.items[j].WhaleWeight
			})
			// Keep all whale elements in bucket
			if config.KeepF != nil {
				for _, v := range samplingMetric.items[:whalesAllowed] {
					config.KeepF(v.Item, bucket.Time)
				}
			}
			samplingMetric.items = samplingMetric.items[whalesAllowed:]
			sf *= 2 // half of space is occupied by whales now. TODO - we can be more exact here, make permutations and take as many elements as we need, saving lots of rnd calls
		}
		pos := config.SelectF(samplingMetric.items, sf, config.Rand)
		for _, v := range samplingMetric.items[:pos] {
			v.Item.SF = sf // communicate selected factor to next step of processing
			if config.KeepF != nil {
				config.KeepF(v.Item, bucket.Time)
			}
		}
		for _, v := range samplingMetric.items[pos:] {
			v.Item.SF = sf // communicate selected factor to next step of processing
			if config.DiscardF != nil {
				config.DiscardF(v.Item, bucket.Time)
			}
		}
	}
	return sampleFactors
}

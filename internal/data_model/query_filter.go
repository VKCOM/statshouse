package data_model

import (
	"cmp"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse/internal/format"
)

type QueryFilter struct {
	Namespace       string
	MetricMatcher   *labels.Matcher
	MatchingMetrics []*format.MetricMetaValue
	FilterIn        TagFilters
	FilterNotIn     TagFilters
	Resolution      int
}

type TagFilters struct {
	Metrics []*format.MetricMetaValue
	Tags    [format.NewMaxTags]TagFilter
}

type TagFilter struct {
	Values TagValues
	Re2    string
}

type TagValues []TagValue

type TagValue struct {
	flags  TagValueFlags
	Value  string
	Mapped int64
}

type TagValueFlags int

const (
	tagHasValue TagValueFlags = 1 << iota
	tagIsMapped
)

func MetricIDFilter(metricID int32) TagFilters {
	return MetricFilter(&format.MetricMetaValue{MetricID: metricID})
}

func MetricFilter(s ...*format.MetricMetaValue) TagFilters {
	return TagFilters{Metrics: s}
}

func (f *QueryFilter) MatchMetrics(m map[string]*format.MetricMetaValue) {
	switch f.MetricMatcher.Type {
	case labels.MatchEqual:
		k := f.MetricMatcher.Value
		if f.Namespace != "" && !strings.Contains(k, format.NamespaceSeparator) {
			k = f.Namespace + format.NamespaceSeparator + k
		}
		if v := m[k]; v != nil {
			f.FilterIn.Metrics = append(f.FilterIn.Metrics, v)
			if f.Resolution < v.Resolution {
				f.Resolution = v.Resolution
			}
		}
		f.MatchingMetrics = f.FilterIn.Metrics
	case labels.MatchRegexp:
		for k, v := range m {
			if f.Namespace != "" && !strings.Contains(k, format.NamespaceSeparator) {
				k = f.Namespace + format.NamespaceSeparator + k
			}
			if f.MetricMatcher.Matches(k) {
				f.FilterIn.Metrics = append(f.FilterIn.Metrics, v)
				if f.Resolution < v.Resolution {
					f.Resolution = v.Resolution
				}
			}
		}
		f.MatchingMetrics = f.FilterIn.Metrics
	case labels.MatchNotEqual, labels.MatchNotRegexp:
		for k, v := range m {
			if f.Namespace != "" && !strings.Contains(k, format.NamespaceSeparator) {
				k = f.Namespace + format.NamespaceSeparator + k
			}
			if f.MetricMatcher.Matches(k) {
				f.MatchingMetrics = append(f.MatchingMetrics, v)
				if f.Resolution < v.Resolution {
					f.Resolution = v.Resolution
				}
			} else {
				f.FilterNotIn.Metrics = append(f.FilterNotIn.Metrics, v)
			}
		}
	default:
		panic(fmt.Errorf("unknown matcher type: %v", f.MetricMatcher.Type))
	}
}

func (f *TagFilters) AppendValue(tag int, val ...string) {
	s := make(TagValues, len(val))
	for i := 0; i < len(val); i++ {
		s[i] = NewTagValueS(val[i])
	}
	f.Append(tag, s...)
}

func (f *TagFilters) AppendMapped(tag int, val ...int64) {
	s := make(TagValues, len(val))
	for i := 0; i < len(val); i++ {
		s[i] = NewTagValueM(val[i])
	}
	f.Append(tag, s...)
}

func (f *TagFilters) Append(tag int, filter ...TagValue) {
	f.Tags[tag].Values = append(f.Tags[tag].Values, filter...)
}

func (f *TagFilters) Contains(tag int) bool {
	return 0 <= tag && int(tag) < len(f.Tags) && len(f.Tags[tag].Values) != 0
}

func (f *TagFilter) Empty() bool {
	return len(f.Values) == 0 && f.Re2 == ""
}

func (v TagValues) Sort() {
	sort.Slice(v, func(i, j int) bool {
		if n := cmp.Compare(v[i].Value, v[j].Value); n != 0 {
			return n < 0
		}
		return v[i].Mapped < v[j].Mapped
	})
}

func NewTagValue(s string, n int64) TagValue {
	return TagValue{
		flags:  tagHasValue | tagIsMapped,
		Value:  s,
		Mapped: n,
	}
}

func NewTagValueS(s string) TagValue {
	return TagValue{
		flags: tagHasValue,
		Value: s,
	}
}

func NewTagValueM(n int64) TagValue {
	return TagValue{
		flags:  tagIsMapped,
		Mapped: n,
	}
}

func (v TagValue) HasValue() bool {
	return v.flags&tagHasValue != 0
}

func (v TagValue) IsMapped() bool {
	return v.flags&tagIsMapped != 0
}

func (v TagValue) Empty() bool {
	return v.HasValue() && v.IsMapped() && v.Value == "" && v.Mapped == 0
}

func (v TagValue) String() string {
	if v.HasValue() {
		return v.Value
	}
	if v.IsMapped() {
		return strconv.FormatInt(v.Mapped, 10)
	}
	return ""
}

package api

import (
	"fmt"
	"sort"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

func (b *queryBuilder) getOrBuildCacheKey() string {
	if b.cacheKey != "" {
		return b.cacheKey
	}
	b.WriteString(`{"v":`)
	switch b.version {
	case Version1:
		b.WriteString(Version1)
	default:
		b.WriteString(Version3)
	}
	b.WriteString(`,"m":`)
	b.WriteString(fmt.Sprint(b.metricID()))
	b.WriteString(`,"pk":"`)
	b.WriteString(fmt.Sprint(b.preKeyTagX()))
	b.WriteString(`","st":`)
	b.WriteString(fmt.Sprint(b.isStringTop()))
	b.WriteString(`,"what":[`)
	whatComma := b.newListComma()
	lastWhat := data_model.DigestUnspecified
	for j := 0; b.what.specifiedAt(j); j++ {
		if lastWhat == b.what[j].What {
			continue
		}
		lastWhat = b.what[j].What
		whatComma.maybeWrite()
		b.WriteString(`"`)
		b.WriteString(b.what[j].What.String())
		b.WriteString(`"`)
	}
	if b.minMaxHost[0] {
		whatComma.maybeWrite()
		b.WriteString(`"minhost"`)
	}
	if b.minMaxHost[1] {
		whatComma.maybeWrite()
		b.WriteString(`"maxhost"`)
	}
	b.WriteString(`],"by":[`)
	if len(b.by) != 0 {
		byComma := b.newListComma()
		sort.Ints(b.by)
		for i := 0; i < len(b.by); i++ {
			byComma.maybeWrite()
			b.WriteString(`"`)
			b.WriteString(fmt.Sprint(b.by[i]))
			b.WriteString(`"`)
		}
	}
	b.WriteString(`],"inc":`)
	s := make([]string, 0, 16)
	s = b.writeTagFiltersCacheKey(b.filterIn, s)
	b.WriteString(`,"exl":`)
	b.writeTagFiltersCacheKey(b.filterNotIn, s)
	b.WriteString(`,"sort":`)
	b.WriteString(fmt.Sprint(int(b.sort)))
	b.WriteString("}")
	b.cacheKey = b.String()
	b.Reset()
	return b.cacheKey
}

func (b *queryBuilder) writeTagFiltersCacheKey(f data_model.TagFilters, s []string) []string {
	keyComma := b.newListComma()
	b.WriteString("{")
	for i, filter := range f.Tags {
		if filter.Empty() {
			continue
		}
		keyComma.maybeWrite()
		b.WriteString(`"`)
		b.WriteString(fmt.Sprint(i))
		b.WriteString(`":`)
		if filter.Re2 != "" {
			b.WriteString(`"`)
			b.WriteString(filter.Re2)
			b.WriteString(`"`)
		} else if len(filter.Values) != 0 {
			s := s[:0]
			for _, v := range filter.Values {
				s = append(s, v.String())
			}
			sort.Strings(s)
			b.WriteString(`["`)
			b.WriteString(s[0])
			for i := 1; i < len(s); i++ {
				b.WriteString(`","`)
				b.WriteString(s[i])
			}
			b.WriteString(`"]`)
		}
	}
	if stringTop := &f.Tags[format.StringTopTagIndexV3]; stringTop.Re2 != "" {
		keyComma.maybeWrite()
		b.WriteString(`"_s":"`)
		b.WriteString(stringTop.Re2)
		b.WriteString(`"`)
	} else if len(stringTop.Values) != 0 {
		for _, v := range stringTop.Values {
			s = append(s[:0], v.Value)
		}
		sort.Strings(s)
		keyComma.maybeWrite()
		b.WriteString(`"_s":[`)
		stopComma := b.newListComma()
		for i := 0; i < len(s); i++ {
			stopComma.maybeWrite()
			b.WriteString(`"`)
			b.WriteString(s[i])
			b.WriteString(`"`)
		}
		b.WriteString(`]`)
	}
	b.WriteString("}")
	return s
}

package api

import (
	"fmt"
	"sort"
	"strings"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
)

func (b *queryBuilder) getOrBuildCacheKey() string {
	if b.cacheKey != "" {
		return b.cacheKey
	}
	var sb strings.Builder
	sb.WriteString(`{"v":`)
	switch b.version {
	case Version1:
		sb.WriteString(Version1)
	default:
		sb.WriteString(Version3)
	}
	sb.WriteString(`,"m":`)
	sb.WriteString(fmt.Sprint(b.metricID()))
	sb.WriteString(`,"pk":"`)
	sb.WriteString(fmt.Sprint(b.preKeyTagX()))
	sb.WriteString(`","st":`)
	sb.WriteString(fmt.Sprint(b.isStringTop()))
	sb.WriteString(`,"what":[`)
	whatComma := b.newListComma()
	lastWhat := data_model.DigestUnspecified
	for j := 0; b.what.specifiedAt(j); j++ {
		if lastWhat == b.what[j].What {
			continue
		}
		lastWhat = b.what[j].What
		whatComma.maybeWrite(&sb)
		sb.WriteString(`"`)
		sb.WriteString(b.what[j].What.String())
		sb.WriteString(`"`)
	}
	if b.minMaxHost[0] {
		whatComma.maybeWrite(&sb)
		sb.WriteString(`"minhost"`)
	}
	if b.minMaxHost[1] {
		whatComma.maybeWrite(&sb)
		sb.WriteString(`"maxhost"`)
	}
	sb.WriteString(`],"by":[`)
	if len(b.by) != 0 {
		byComma := b.newListComma()
		sort.Ints(b.by)
		for i := 0; i < len(b.by); i++ {
			byComma.maybeWrite(&sb)
			sb.WriteString(`"`)
			sb.WriteString(fmt.Sprint(b.by[i]))
			sb.WriteString(`"`)
		}
	}
	sb.WriteString(`],"inc":`)
	s := make([]string, 0, 16)
	s = b.writeTagFiltersCacheKey(&sb, b.filterIn, s)
	sb.WriteString(`,"exl":`)
	b.writeTagFiltersCacheKey(&sb, b.filterNotIn, s)
	sb.WriteString(`,"sort":`)
	sb.WriteString(fmt.Sprint(int(b.sort)))
	sb.WriteString("}")
	b.cacheKey = sb.String()
	return b.cacheKey
}

func (b *queryBuilder) writeTagFiltersCacheKey(sb *strings.Builder, f data_model.TagFilters, s []string) []string {
	keyComma := b.newListComma()
	sb.WriteString("{")
	for i, filter := range f.Tags {
		if filter.Empty() {
			continue
		}
		keyComma.maybeWrite(sb)
		sb.WriteString(`"`)
		sb.WriteString(fmt.Sprint(i))
		sb.WriteString(`":`)
		if filter.Re2 != "" {
			sb.WriteString(`"`)
			sb.WriteString(filter.Re2)
			sb.WriteString(`"`)
		} else if len(filter.Values) != 0 {
			s := s[:0]
			for _, v := range filter.Values {
				s = append(s, v.String())
			}
			sort.Strings(s)
			sb.WriteString(`["`)
			sb.WriteString(s[0])
			for i := 1; i < len(s); i++ {
				sb.WriteString(`","`)
				sb.WriteString(s[i])
			}
			sb.WriteString(`"]`)
		}
	}
	if stringTop := &f.Tags[format.StringTopTagIndexV3]; stringTop.Re2 != "" {
		keyComma.maybeWrite(sb)
		sb.WriteString(`"_s":"`)
		sb.WriteString(stringTop.Re2)
		sb.WriteString(`"`)
	} else if len(stringTop.Values) != 0 {
		for _, v := range stringTop.Values {
			s = append(s[:0], v.Value)
		}
		sort.Strings(s)
		keyComma.maybeWrite(sb)
		sb.WriteString(`"_s":[`)
		stopComma := b.newListComma()
		for i := 0; i < len(s); i++ {
			stopComma.maybeWrite(sb)
			sb.WriteString(`"`)
			sb.WriteString(s[i])
			sb.WriteString(`"`)
		}
		sb.WriteString(`]`)
	}
	sb.WriteString("}")
	return s
}

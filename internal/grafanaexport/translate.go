// Copyright 2026 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package grafanaexport

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/VKCOM/statshouse/internal/format"
)

// shape is the Prometheus form a StatsHouse metric is assumed to have in VictoriaMetrics:
// a counter is a plain counter series, everything else is a summary/histogram (_count, _sum, _bucket).
type shape int

const (
	shapeCounter shape = iota
	shapeSummary
)

func shapeOf(m *Meta) shape {
	if m != nil && m.Kind == format.MetricKindCounter {
		return shapeCounter
	}
	return shapeSummary
}

var quantiles = map[string]float64{
	format.ParamQueryFnMin:  0,
	format.ParamQueryFnP0_1: 0.001,
	format.ParamQueryFnP1:   0.01,
	format.ParamQueryFnP5:   0.05,
	format.ParamQueryFnP10:  0.1,
	format.ParamQueryFnP25:  0.25,
	format.ParamQueryFnP50:  0.5,
	format.ParamQueryFnP75:  0.75,
	format.ParamQueryFnP90:  0.9,
	format.ParamQueryFnP95:  0.95,
	format.ParamQueryFnP99:  0.99,
	format.ParamQueryFnP999: 0.999,
	format.ParamQueryFnMax:  1,
}

var whatAliases = map[string]string{
	"countsec": format.ParamQueryFnCountNorm,
	"sumsec":   format.ParamQueryFnSumNorm,
}

var identRe = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

func quote(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s)
}

func selector(series string, matchers []string) string {
	if identRe.MatchString(series) {
		if len(matchers) == 0 {
			return series
		}
		return series + "{" + strings.Join(matchers, ", ") + "}"
	}
	all := append([]string{`__name__="` + quote(series) + `"`}, matchers...)
	return "{" + strings.Join(all, ", ") + "}"
}

func matcher(label string, values []string, negate bool) string {
	if len(values) == 1 {
		op := "="
		if negate {
			op = "!="
		}
		return label + op + `"` + quote(values[0]) + `"`
	}
	esc := make([]string, len(values))
	for i, v := range values {
		esc[i] = regexp.QuoteMeta(v)
	}
	op := "=~"
	if negate {
		op = "!~"
	}
	return label + op + `"` + quote(strings.Join(esc, "|")) + `"`
}

func sanitizeLabel(name string) string {
	var b strings.Builder
	for _, c := range name {
		if c < 128 && (c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
			b.WriteRune(c)
		} else {
			b.WriteByte('_')
		}
	}
	res := b.String()
	if res == "" || res[0] >= '0' && res[0] <= '9' {
		res = "_" + res
	}
	return res
}

// tagLabel is the VictoriaMetrics label assumed for a StatsHouse tag: its custom name from the metric
// metadata, otherwise the legacy StatsHouse tag name (key0..key47, skey).
func tagLabel(m *Meta, tag string) string {
	if tag == format.StringTopTagID {
		if m != nil && m.StringTopName != "" {
			return sanitizeLabel(m.StringTopName)
		}
		return format.LegacyStringTopTagID
	}
	i, _ := strconv.Atoi(tag)
	if m != nil && i < len(m.TagNames) && m.TagNames[i] != "" {
		return sanitizeLabel(m.TagNames[i])
	}
	return format.TagIDLegacy(i)
}

func unitOf(m *Meta) string {
	if m != nil {
		switch m.MetricType {
		case format.MetricSecond:
			return "s"
		case format.MetricMillisecond:
			return "ms"
		case format.MetricMicrosecond:
			return "µs"
		case format.MetricNanosecond:
			return "ns"
		case format.MetricByte:
			return "bytes"
		case format.MetricByteAsBits:
			return "bits"
		}
	}
	return "short"
}

type queryCtx struct {
	metric   string
	shape    shape
	unit     string
	by       []string
	matchers []string
}

func (q *queryCtx) series(role string) string {
	if q.shape == shapeCounter {
		return q.metric
	}
	return q.metric + "_" + role
}

func (q *queryCtx) sumBy(inner string, extra ...string) string {
	by := append(append([]string{}, q.by...), extra...)
	if len(by) == 0 {
		return "sum (" + inner + ")"
	}
	return "sum by (" + strings.Join(by, ", ") + ") (" + inner + ")"
}

func (q *queryCtx) rate(role, fn, window string) string {
	return q.sumBy(fn + "(" + selector(q.series(role), q.matchers) + "[" + window + "])")
}

type translation struct {
	expr string
	unit string
	note string // approximation worth telling the user about
}

// translate maps a StatsHouse aggregation to a MetricsQL expression. A non-empty reason means it cannot be translated.
func (q *queryCtx) translate(what string) (t translation, reason string) {
	if alias, ok := whatAliases[what]; ok {
		what = alias
	}
	counter := q.shape == shapeCounter
	sumUnit := q.unit
	if counter {
		sumUnit = "short"
	}
	notDefined := "is not defined for a counter metric (only counts and sums)"
	switch what {
	case format.ParamQueryFnCountNorm:
		return translation{expr: q.rate("count", "rate", rateWindow), unit: "ops"}, ""
	case format.ParamQueryFnCount:
		return translation{expr: q.rate("count", "increase", rateWindow), unit: "short"}, ""
	case format.ParamQueryFnSumNorm:
		return translation{expr: q.rate("sum", "rate", rateWindow), unit: "short"}, ""
	case format.ParamQueryFnSum:
		return translation{expr: q.rate("sum", "increase", rateWindow), unit: sumUnit}, ""
	case format.ParamQueryFnCumulCount:
		return translation{expr: "running_sum(" + q.rate("count", "increase", "1i") + ")", unit: "short"}, ""
	case format.ParamQueryFnCumulSum:
		return translation{expr: "running_sum(" + q.rate("sum", "increase", "1i") + ")", unit: sumUnit}, ""
	case format.ParamQueryFnAvg:
		if counter {
			return t, notDefined
		}
		return translation{expr: "(" + q.rate("sum", "rate", rateWindow) + ") / (" + q.rate("count", "rate", rateWindow) + ")", unit: q.unit}, ""
	}
	if quantile, ok := quantiles[what]; ok {
		if counter {
			return t, notDefined
		}
		inner := "rate(" + selector(q.series("bucket"), q.matchers) + "[" + rateWindow + "])"
		t = translation{
			expr: "histogram_quantile(" + strconv.FormatFloat(quantile, 'g', -1, 64) + ", " + q.sumBy(inner, "le", "vmrange") + ")",
			unit: q.unit,
		}
		if what == format.ParamQueryFnMin || what == format.ParamQueryFnMax {
			t.note = fmt.Sprintf("`%s` is approximated by a bucket quantile", what)
		}
		return t, ""
	}
	return t, "has no VictoriaMetrics equivalent"
}

// varBinding links a dashboard variable to a tag of a plot.
type varBinding struct {
	name   string
	negate bool
	group  bool
}

type panelQuery struct {
	targets  []Target
	units    []string
	interval string
	unit     string
}

func refID(i int) string {
	if i < 26 {
		return string(rune('A' + i))
	}
	return "Q" + strconv.Itoa(i)
}

func (e *exporter) buildPlot(p *plot, vars map[string]varBinding) *panelQuery {
	meta := e.meta(p.metric)
	q := &queryCtx{metric: p.metric, shape: shapeOf(meta), unit: unitOf(meta)}

	for _, f := range []struct {
		filters map[string][]string
		negate  bool
	}{{p.filterIn, false}, {p.filterNotIn, true}} {
		for _, tag := range sortedTags(f.filters) {
			if _, bound := vars[tag]; bound {
				continue
			}
			values := make([]string, len(f.filters[tag]))
			for i, v := range f.filters[tag] {
				values[i] = tagValue(v)
			}
			q.matchers = append(q.matchers, matcher(tagLabel(meta, tag), values, f.negate))
		}
	}
	for _, tag := range sortedTags(vars) {
		op := "=~"
		if vars[tag].negate {
			op = "!~"
		}
		q.matchers = append(q.matchers, tagLabel(meta, tag)+op+`"${`+vars[tag].name+`}"`)
	}

	byTags := append([]string{}, p.groupBy...)
	for _, tag := range sortedTags(vars) {
		if vars[tag].group {
			byTags = appendUnique(byTags, tag)
		}
	}
	for _, tag := range byTags {
		q.by = appendUnique(q.by, tagLabel(meta, tag))
	}

	res := &panelQuery{unit: q.unit}
	for _, what := range p.what {
		t, reason := q.translate(what)
		if reason != "" {
			e.warn.addPlot(p, "aggregation `%s` %s, skipped", what, reason)
			continue
		}
		if t.note != "" {
			e.warn.addPlot(p, "%s", t.note)
		}
		expr := t.expr
		if len(q.by) != 0 && p.numSeries != 0 {
			fn, n := "topk_avg", p.numSeries
			if n < 0 {
				fn, n = "bottomk_avg", -n
			}
			expr = fmt.Sprintf("%s(%d, %s)", fn, n, expr)
		}
		legend := make([]string, len(q.by))
		for i, l := range q.by {
			legend[i] = "{{" + l + "}}"
		}
		if len(p.what) > 1 {
			legend = append(legend, what)
		}
		res.targets = append(res.targets, Target{
			RefID:        refID(len(res.targets)),
			Datasource:   e.datasource,
			Expr:         expr,
			EditorMode:   "code",
			Range:        true,
			LegendFormat: strings.Join(legend, " "),
		})
		res.units = appendUnique(res.units, t.unit)
	}
	if p.customAgg > 0 {
		res.interval = strconv.Itoa(p.customAgg) + "s"
	}
	if p.eventsShown {
		e.warn.addPlot(p, "event overlays are not reproduced")
	}
	if p.maxHost {
		e.warn.addPlot(p, "max host is not reproduced")
	}
	return res
}

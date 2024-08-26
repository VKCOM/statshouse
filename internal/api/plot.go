// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"
	"unicode"

	"github.com/vkcom/statshouse/internal/format"
)

const (
	defaultRenderWidth     = 1080
	defaultTextRenderWidth = 79
	titleHeight            = 40
	maxRenderWidth         = 4096
	goldenRatio            = 1.61803398875
	blankRenderInterval    = 24 * time.Hour

	// language=GoTemplate
	gnuplotTemplate = `
{{if eq .Format "png" -}}
set terminal pngcairo font "Open Sans, 10" background "#ffffff" noenhanced fontscale 1.0 size {{.Width}}, {{.Height}}
{{else if eq .Format "svg" -}}
set terminal svg font "Open Sans, 10" background "#ffffff" noenhanced fontscale 1.0 size {{.Width}}, {{.Height}}
{{else if eq .Format "text" -}}
set terminal dumb noenhanced mono size {{.Width}}, {{.Height}}
{{- end}}
set multiplot layout {{.Rows}},{{.Cols}}
{{range $_, $d := .Plots}}
{{if $d.Title -}}
set title "{{$d.Header}}" offset 0, -0.5
{{- end}}
set size ratio {{$d.Ratio}}
set encoding utf8

{{$l := len $d.Legend}}
{{- if ne $l 0 -}}
{{with index $d.Legend 0 -}}
{{- if eq "second" .MetricType -}}
set format y "%gs"
{{- else if eq "millisecond" .MetricType -}}
set format y "%gms"
{{- else if eq "microsecond" .MetricType -}}
set format y "%gus"
{{- else if eq "nanosecond" .MetricType -}}
set format y "%gns"
{{- else if eq "byte" .MetricType -}}
set format y "%.1b%BB"
{{- else -}}
set format y "%.1s%c"
{{- end -}}
{{- end -}}
{{- end}}

set style line 1 linewidth 0.3 linecolor rgb '#adb5bd' # thin grey
set grid linestyle 1
set border 0
set tics scale 0
set key outside bmargin right Right noreverse horizontal samplen 2
set style fill transparent solid 0.15 noborder
set xdata time
set timefmt "%s"
set format x "%H:%M\n%d/%m"
set xrange [*:*] noextend
set yrange [*<-0:*] noextend
set datafile missing "NaN"

{{range $i, $meta := $d.Data.Series.SeriesMeta -}}
set style line {{$d.LineStyle $i}} linetype 1 linecolor rgb '{{$meta.Color}}'
{{end}}

$data << EOD
{{range $i, $meta := $d.Data.Series.SeriesMeta -}}
"{{$d.MetaToLabel $meta}}"
{{$d.WriteData $i}}
{{end}}
EOD

set xrange [{{$d.TimeFrom}}:{{$d.TimeTo}}]
set yrange [{{$d.YL}}:{{$d.YH}}]
{{if $d.Data.Series.SeriesMeta -}}
plot for [n=0:{{$d.Data.Series.SeriesMeta | len}}] $data index n using 1:2 with fillsteps notitle linestyle (10+n), \
     for [n=0:{{$d.Data.Series.SeriesMeta | len}}] $data index n using 1:2 with points notitle linestyle (10+n) linewidth 0.7 pointtype 7 pointsize 0.2, \
     for [n=0:{{$d.Data.Series.SeriesMeta | len}}] $data index n using 1:2 with steps notitle linestyle (10+n) linewidth 0.7 \
     {{range $i, $meta := $d.Legend -}}, NaN with points pt 5 ps 2 lc rgb "{{$meta.Color}}" title "{{$d.MetaToLabel $meta}}"{{end}}
{{else -}}
set key off
set yrange [0:100]
plot 1/0
{{end}}
reset
{{end}}
unset multiplot
`
)

var (
	// XXX: keep in sync with TypeScript
	palette = [...][]string{
		{
			"blue",
			"#0d6efd", // blue-500
			"#6ea8fe", // blue-300
			"#084298", // blue-700
			"#3d8bfd", // blue-400
			"#0a58ca", // blue-600
		},
		{
			"purple",
			"#6610f2", // indigo-500
			"#a370f7", // indigo-300
			"#3d0a91", // indigo-700
			"#8540f5", // indigo-400
			"#520dc2", // indigo-600
		},
		{
			"purple",
			"#6f42c1", // purple-500
			"#a98eda", // purple-300
			"#432874", // purple-700
			"#8c68cd", // purple-400
			"#59359a", // purple-600
		},
		{
			"red",
			"#d63384", // pink-500
			"#e685b5", // pink-300
			"#801f4f", // pink-700
			"#de5c9d", // pink-400
			"#ab296a", // pink-600
		},
		{
			"red",
			"#dc3545", // red-500
			"#ea868f", // red-300
			"#842029", // red-700
			"#e35d6a", // red-400
			"#b02a37", // red-600
		},
		{
			"yellow",
			"#fd7e14", // orange-500
			"#feb272", // orange-300
			"#984c0c", // orange-700
			"#fd9843", // orange-400
			"#ca6510", // orange-600
		},
		{
			"yellow",
			"#ffc107", // yellow-500
			"#ffda6a", // yellow-300
			"#997404", // yellow-700
			"#ffda6a", // yellow-400
			"#cc9a06", // yellow-600
		},
		{
			"green",
			"#198754", // green-500
			"#75b798", // green-300
			"#0f5132", // green-700
			"#479f76", // green-400
			"#146c43", // green-600
		},
		{
			"green",
			"#20c997", // teal-500
			"#79dfc1", // teal-300
			"#13795b", // teal-700
			"#4dd4ac", // teal-400
			"#1aa179", // teal-600
		},
		{
			"blue",
			"#0dcaf0", // cyan-500
			"#6edff6", // cyan-300
			"#087990", // cyan-700
			"#3dd5f3", // cyan-400
			"#0aa2c0", // cyan-600
		},
	}
)

type gnuplotTemplateData struct {
	Format   string
	Title    bool
	Metric   string
	Width    int
	Height   int
	Ratio    float64
	Data     *SeriesResponse
	Legend   []QuerySeriesMetaV2
	TimeFrom int64
	TimeTo   int64
	YL, YH   string // Y scale range

	usedColorIndices map[string]int
	uniqueWhat       map[string]struct{}
	utcOffset        int64

	// The buffer which template is printed into,
	// methods generating large texts might write directly into it
	// (see "WriteData" function below).
	wr io.Writer
}

func (d *gnuplotTemplateData) WriteData(i int) string {
	var blank bool
	for j, v := range *d.Data.Series.SeriesData[i] {
		if math.IsNaN(v) {
			if !blank {
				// blank line tells GNUPlot to not connect adjacent points
				fmt.Fprint(d.wr, "\n")
				// adjacent blank lines are merged
				blank = true
			}
			continue
		}
		fmt.Fprint(d.wr, d.Data.Series.Time[j]+d.utcOffset)
		fmt.Fprint(d.wr, " ")
		fmt.Fprint(d.wr, v)
		fmt.Fprint(d.wr, "\n")
		blank = false
	}
	return "\n"
}

func (d *gnuplotTemplateData) LineStyle(i int) int {
	return 10 + i
}

func (d *gnuplotTemplateData) GetUniqueWhat() map[string]struct{} {
	if len(d.uniqueWhat) > 0 {
		return d.uniqueWhat
	}

	for _, meta := range d.Data.Series.SeriesMeta {
		d.uniqueWhat[meta.What] = struct{}{}
	}

	return d.uniqueWhat
}

func (d *gnuplotTemplateData) Header() string {
	uniqueWhat := d.GetUniqueWhat()
	lineFns := make([]string, 0, len(uniqueWhat))
	for what := range uniqueWhat {
		lineFns = append(lineFns, WhatToWhatDesc(what))
	}

	return fmt.Sprintf("%s: %s", d.Metric, strings.Join(lineFns, ", "))
}

func (h *Handler) colorize(resp *SeriesResponse) {
	if resp == nil {
		return
	}
	graphCount := 0
	uniqueWhat := make(map[string]struct{})
	for _, meta := range resp.Series.SeriesMeta {
		uniqueWhat[meta.What] = struct{}{}
		if meta.TimeShift == 0 {
			graphCount++
		}
	}
	colorKeyAt := func(i int) string {
		var (
			meta             = resp.Series.SeriesMeta[i]
			oneGraph         = graphCount == 1
			uniqueWhatLength = len(uniqueWhat)
			label            = MetaToLabel(meta, uniqueWhatLength, h.utcOffset)
			baseLabel        = MetaToBaseLabel(meta, uniqueWhatLength, h.utcOffset)
			isValue          = strings.Index(baseLabel, "Value") == 0
			metricName       string
			prefColor        = 9 // it`s magic prefix
			colorKey         string
		)
		if isValue {
			metricName = meta.Name
		}
		if oneGraph {
			colorKey = fmt.Sprintf("%d%s%s", prefColor, metricName, label)
		} else {
			colorKey = fmt.Sprintf("%d%s%s", prefColor, metricName, baseLabel)
		}
		return colorKey
	}
	type indexKey struct {
		index int
		key   string
	}
	i := 0
	top := make([]indexKey, 0, len(palette))
	for ; i < len(resp.Series.SeriesMeta) && i < len(palette); i++ {
		top = append(top, indexKey{i, colorKeyAt(i)})
	}
	sort.Slice(top, func(i, j int) bool {
		return top[i].key < top[j].key
	})
	usedColorIndices := make(map[string]int, len(resp.Series.SeriesMeta))
	for _, v := range top {
		resp.Series.SeriesMeta[v.index].Color = selectColor(v.key, usedColorIndices)
	}
	for ; i < len(resp.Series.SeriesMeta); i++ {
		resp.Series.SeriesMeta[i].Color = selectColor(colorKeyAt(i), usedColorIndices)
	}
}

func (d *gnuplotTemplateData) MetaToLabel(meta QuerySeriesMetaV2) string {
	return MetaToLabel(meta, len(d.GetUniqueWhat()), d.utcOffset)
}

func plotSize(format string, title bool, width int) (int, int) {
	if width == 0 {
		width = defaultRenderWidth
		if format == dataFormatText {
			width = defaultTextRenderWidth
		}
	}
	if width > maxRenderWidth {
		width = maxRenderWidth
	}
	height := int(math.Ceil(float64(width) / goldenRatio))
	if title {
		height += titleHeight
	}
	return width, height
}

func plot(ctx context.Context, format string, title bool, data []*SeriesResponse, utcOffset int64, metric []seriesRequest, width int, tmpl *template.Template) ([]byte, error) {
	width, height := plotSize(format, title, width)
	var (
		rows = 1
		cols = 1
	)
	if 1 < len(data) {
		cols = 2
		rows = (len(data) + 1) / cols
		width /= cols
		height /= cols
	}
	var (
		buf bytes.Buffer
		td  = make([]*gnuplotTemplateData, len(data))
	)
	utcOffset %= (24 * 3600) // ignore the part we use to align start of week
	for i := 0; i < len(data); i++ {
		var (
			legend       = data[i].Series.SeriesMeta
			legendMaxLen = 15
		)
		if len(legend) > legendMaxLen {
			legend = data[i].Series.SeriesMeta[:legendMaxLen]
		}
		effectiveName := metric[i].metricName
		if len(metric[i].customMetricName) > 0 {
			effectiveName = metric[i].customMetricName
		}
		td[i] = &gnuplotTemplateData{
			Format:           format,
			Title:            title,
			Metric:           effectiveName,
			Width:            width,
			Height:           height,
			Ratio:            1 / goldenRatio,
			Data:             data[i],
			TimeFrom:         metric[i].from.Unix() + utcOffset,
			TimeTo:           metric[i].to.Unix() + utcOffset,
			Legend:           legend,
			usedColorIndices: map[string]int{},
			uniqueWhat:       map[string]struct{}{},
			utcOffset:        utcOffset,
			wr:               &buf,
			YL:               metric[i].yl,
			YH:               metric[i].yh,
		}
	}

	templateData := struct {
		Plots  []*gnuplotTemplateData
		Format string
		Width  int
		Height int
		Rows   int
		Cols   int
	}{td, format, width * cols, height * rows, rows, cols}
	err := tmpl.Execute(&buf, templateData)
	if err != nil {
		return nil, fmt.Errorf("failed to form gnuplot input: %w", err)
	}

	gp := exec.CommandContext(ctx, "gnuplot", "-d")
	gp.Stdin = &buf

	out, err := gp.Output()
	if err != nil {
		// timeout or cancelled request
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return nil, fmt.Errorf("failed to execute gnuplot: %w (stderr: %q)", err, ee.Stderr)
		}
		return nil, fmt.Errorf("failed to execute gnuplot: %w", err)
	}

	if format == dataFormatText {
		first := bytes.IndexFunc(out, func(r rune) bool { return !unicode.IsSpace(r) })
		start := bytes.LastIndexByte(out[:first], '\n')
		out = bytes.TrimRightFunc(out[start+1:], unicode.IsSpace)
	}

	return out, nil
}

func fnv1aSum(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// XXX: keep in sync with TypeScript
func selectColor(s string, used map[string]int) string {
	var (
		h = fnv1aSum(s)
		i = int(h % uint32(len(palette)))
		v = palette[i]
	)
	for offset := 0; offset < len(palette); offset++ {
		j := (i + offset) % len(palette)
		if _, ok := used[palette[j][0]]; !ok {
			v = palette[j]
			used[v[0]] = -1
			break
		}
	}
	used[v[0]]++
	return v[1+used[v[0]]%(len(v)-1)]
}

// XXX: keep in sync with TypeScript
func MetaToLabel(meta QuerySeriesMetaV2, uniqueWhatLength int, utcOffset int64) string {
	var (
		desc = MetaToBaseLabel(meta, uniqueWhatLength, utcOffset)
		tsd  = timeShiftDesc(meta.TimeShift)
	)
	if tsd == "" {
		return desc
	}
	return fmt.Sprintf("%s %s", tsd, desc)
}

func MetaToBaseLabel(meta QuerySeriesMetaV2, uniqueWhatLength int, utcOffset int64) string {
	type tagEntry struct {
		key string
		tag SeriesMetaTag
	}
	var sortedTags []tagEntry
	for k, t := range meta.Tags {
		sortedTags = append(sortedTags, tagEntry{k, t})
	}
	sort.Slice(sortedTags, func(i, j int) bool { return sortedTags[i].key < sortedTags[j].key })

	var sortedTagValues []string
	for _, kv := range sortedTags {
		sortedTagValues = append(sortedTagValues, formatTagValue(kv.tag.Value, kv.tag.Comment, kv.tag.Raw, kv.tag.RawKind, utcOffset))
	}

	desc := "Value"
	if len(sortedTagValues) > 0 {
		desc = strings.Join(sortedTagValues, ", ")
	}

	if uniqueWhatLength > 1 {
		desc = fmt.Sprintf("%s: %s", desc, WhatToWhatDesc(meta.What))
	}

	return desc
}

// XXX: keep in sync with TypeScript
func formatTagValue(s string, c string, r bool, k string, utcOffset int64) string {
	if c != "" {
		return c
	}

	if len(s) < 1 || s[0] != ' ' {
		return s
	}
	if r && len(k) != 0 {
		i, _ := strconv.Atoi(s[1:])
		return "⚡ " + convert(k, i, utcOffset)
	}
	i, _ := strconv.Atoi(s[1:])
	switch i {
	case format.TagValueIDUnspecified:
		return "⚡ empty"
	case format.TagValueIDMappingFlood:
		return "⚡ mapping flood"
	default:
		return fmt.Sprintf("⚡ %d", i)
	}
}

// XXX: keep in sync with TypeScript
func convert(kind string, input int, utcOffset int64) string {
	switch kind {
	case "hex":
		return fmt.Sprintf("%#08X", uint32(input))
	case "hex_bswap":
		u := uint(input)
		return fmt.Sprintf("%02X%02X%02X%02X", u&255, (u>>8)&255, (u>>16)&255, (u>>24)&255)
	case "timestamp":
		return time.Unix(int64(input)-utcOffset, 0).Format("2006-01-02 15:04:05")
	case "ip":
		u := uint(input)
		return fmt.Sprintf("%d.%d.%d.%d", (u>>24)&255, (u>>16)&255, (u>>8)&255, u&255)
	case "ip_bswap":
		u := uint(input)
		return fmt.Sprintf("%d.%d.%d.%d", u&255, (u>>8)&255, (u>>16)&255, (u>>24)&255)
	case "uint":
		return fmt.Sprintf("%d", uint(input))
	default:
		return fmt.Sprintf("%d", input)
	}
}

// XXX: keep in sync with TypeScript
func WhatToWhatDesc(what string) string {
	switch what {
	case ParamQueryFnP999:
		return "p99.9"
	case ParamQueryFnCountNorm:
		return "count/sec"
	case ParamQueryFnCumulCount:
		return "count (cumul)"
	case ParamQueryFnCardinalityNorm:
		return "cardinality/sec"
	case ParamQueryFnCumulCardinality:
		return "cardinality (cumul)"
	case ParamQueryFnCumulAvg:
		return "avg (cumul)"
	case ParamQueryFnSumNorm:
		return "sum/sec"
	case ParamQueryFnCumulSum:
		return "sum (cumul)"
	case ParamQueryFnUniqueNorm:
		return "unique/sec"
	case ParamQueryFnMaxCountHost:
		return "max(count)@host"
	case ParamQueryFnDerivativeCount:
		return "count (derivative)"
	case ParamQueryFnDerivativeSum:
		return "sum (derivative)"
	case ParamQueryFnDerivativeAvg:
		return "avg (derivative)"
	case ParamQueryFnDerivativeCountNorm:
		return "count/sec (derivative)"
	case ParamQueryFnDerivativeSumNorm:
		return "sum/sec (derivative)"
	case ParamQueryFnDerivativeUnique:
		return "unique (derivative)"
	case ParamQueryFnDerivativeUniqueNorm:
		return "unique/sec (derivative)"
	case ParamQueryFnDerivativeMin:
		return "min (derivative)"
	case ParamQueryFnDerivativeMax:
		return "max (derivative)"
	default:
		return what
	}
}

// XXX: keep in sync with TypeScript
func timeShiftDesc(ts int64) string {
	switch ts {
	case 0:
		return ""
	case -24 * 3600:
		return "−24h"
	case -48 * 3600:
		return "−48h"
	case -7 * 24 * 3600:
		return "−1w"
	case -7 * 2 * 24 * 3600:
		return "−2w"
	case -7 * 3 * 24 * 3600:
		return "−3w"
	case -7 * 4 * 24 * 3600:
		return "−4w"
	case -365 * 24 * 3600:
		return "−1y"
	default:
		return strings.ReplaceAll(fmt.Sprintf("%ds", ts), "-", "−")
	}
}

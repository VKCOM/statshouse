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
set yrange [*:*] noextend
set datafile missing "NaN"

{{range $i, $meta := $d.Data.Series.SeriesMeta -}}
set style line {{$d.LineStyle $i}} linetype 1 linecolor rgb '{{$d.LineColor $meta}}'
{{end}}

$data << EOD
{{range $i, $meta := $d.Data.Series.SeriesMeta -}}
"{{$d.MetaToLabel $meta}}"
{{range $j, $t := $d.Data.Series.Time -}}
{{$d.APITime $t}} {{index $d.Data.Series.SeriesData $i $j}}
{{end}}

{{end}}
EOD

{{if $d.Data.Series.SeriesMeta -}}
plot for [n=0:{{$d.Data.Series.SeriesMeta | len}}] $data index n using 1:2 with fillsteps notitle               linestyle (10+n), \
     for [n=0:{{$d.Data.Series.SeriesMeta | len}}] $data index n using 1:2 with     steps title columnheader(1) linestyle (10+n) linewidth 0.7
{{else -}}
set key off
set xrange [{{$d.BlankFrom}}:{{$d.BlankTo}}]
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
	palette = [...][2]string{
		{"blue", "#0d6efd"},   // blue
		{"purple", "#6610f2"}, // indigo
		{"purple", "#6f42c1"}, // purple
		{"red", "#d63384"},    // pink
		{"red", "#dc3545"},    // red
		{"yellow", "#fd7e14"}, // orange
		{"yellow", "#ffc107"}, // yellow
		{"green", "#198754"},  // green
		{"green", "#20c997"},  // teal
		{"blue", "#0dcaf0"},   // cyan
	}
)

type gnuplotTemplateData struct {
	Format    string
	Title     bool
	Metric    string
	Width     int
	Height    int
	Ratio     float64
	Data      *GetQueryResp
	BlankFrom int64
	BlankTo   int64

	usedColorIndices map[string]bool
	uniqueWhat       map[queryFn]struct{}
	utcOffset        int64
}

func (d *gnuplotTemplateData) APITime(t int64) int64 {
	return t + d.utcOffset
}

func (d *gnuplotTemplateData) LineStyle(i int) int {
	return 10 + i
}

func (d *gnuplotTemplateData) GetUniqueWhat() map[queryFn]struct{} {
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

func (d *gnuplotTemplateData) LineColor(meta QuerySeriesMetaV2) string {
	s := fmt.Sprintf("%s: %s", d.Metric, d.MetaToLabel(meta))
	return selectColor(s, d.usedColorIndices)
}

func (d *gnuplotTemplateData) MetaToLabel(meta QuerySeriesMetaV2) string {
	return MetaToLabel(meta, len(d.GetUniqueWhat()))
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

func plot(ctx context.Context, format string, title bool, data []*GetQueryResp, utcOffset int64, metric []getQueryReq, width int, tmpl *template.Template) ([]byte, error) {
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
	td := make([]*gnuplotTemplateData, len(data))
	for i := 0; i < len(data); i++ {
		blankFrom := time.Now().Add(-blankRenderInterval).Unix()
		blankTo := time.Now().Unix()
		if len(data[i].Series.Time) > 1 {
			blankFrom = data[i].Series.Time[0]
			blankTo = data[i].Series.Time[len(data[i].Series.Time)-1]
		}
		td[i] = &gnuplotTemplateData{
			Format:           format,
			Title:            title,
			Metric:           metric[i].metricWithNamespace,
			Width:            width,
			Height:           height,
			Ratio:            1 / goldenRatio,
			Data:             data[i],
			BlankFrom:        blankFrom,
			BlankTo:          blankTo,
			usedColorIndices: map[string]bool{},
			uniqueWhat:       map[queryFn]struct{}{},
			utcOffset:        utcOffset % (24 * 3600), // ignore the part we use to align start of week
		}
	}

	var buf bytes.Buffer
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
func selectColor(s string, used map[string]bool) string {
	h := fnv1aSum(s)
	i := int(h % uint32(len(palette)))
	if used[palette[i][0]] {
		for offset := 1; offset < len(palette); offset++ {
			j := (i + offset) % len(palette)
			if !used[palette[j][0]] {
				i = j
				break
			}
		}
	}
	used[palette[i][0]] = true
	return palette[i][1]
}

// XXX: keep in sync with TypeScript
func MetaToLabel(meta QuerySeriesMetaV2, uniqueWhatLength int) string {
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
		sortedTagValues = append(sortedTagValues, formatTagValue(kv.tag.Value, kv.tag.Comment))
	}

	desc := "Value"
	if len(sortedTagValues) > 0 {
		desc = strings.Join(sortedTagValues, ", ")
	}

	if uniqueWhatLength > 1 {
		desc = fmt.Sprintf("%s: %s", desc, WhatToWhatDesc(meta.What))
	}

	tsd := timeShiftDesc(meta.TimeShift)
	if tsd == "" {
		return desc
	}

	return fmt.Sprintf("%s %s", tsd, desc)
}

// XXX: keep in sync with TypeScript
func formatTagValue(s string, c string) string {
	if c != "" {
		return c
	}

	if len(s) < 1 || s[0] != ' ' {
		return s
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
func WhatToWhatDesc(what queryFn) string {
	switch what {
	case queryFnP999:
		return "p99.9"
	case queryFnCountNorm:
		return "count/sec"
	case queryFnCumulCount:
		return "count (cumul)"
	case queryFnCardinalityNorm:
		return "cardinality/sec"
	case queryFnCumulCardinality:
		return "cardinality (cumul)"
	case queryFnCumulAvg:
		return "avg (cumul)"
	case queryFnSumNorm:
		return "sum/sec"
	case queryFnCumulSum:
		return "sum (cumul)"
	case queryFnUniqueNorm:
		return "unique/sec"
	case queryFnMaxCountHost:
		return "max(count)@host"
	case queryFnDerivativeCount:
		return "count (derivative)"
	case queryFnDerivativeSum:
		return "sum (derivative)"
	case queryFnDerivativeAvg:
		return "avg (derivative)"
	case queryFnDerivativeCountNorm:
		return "count/sec (derivative)"
	case queryFnDerivativeSumNorm:
		return "sum/sec (derivative)"
	case queryFnDerivativeUnique:
		return "unique (derivative)"
	case queryFnDerivativeUniqueNorm:
		return "unique/sec (derivative)"
	case queryFnDerivativeMin:
		return "min (derivative)"
	case queryFnDerivativeMax:
		return "max (derivative)"
	default:
		return what.String()
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

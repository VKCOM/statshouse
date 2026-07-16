// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"math"
	"strconv"
	"time"

	xdraw "golang.org/x/image/draw"
	gplot "gonum.org/v1/plot"
	"gonum.org/v1/plot/font"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
	"gonum.org/v1/plot/vg/vgimg"
)

var gridLineColor = color.NRGBA{R: 0xe9, G: 0xec, B: 0xef, A: 0xff}

func init() {
	// gonum defaults to Liberation Serif, but looks unreadable, so we use mono
	gplot.DefaultFont = font.Font{Typeface: "Liberation", Variant: "Mono"}
	plotter.DefaultFont = gplot.DefaultFont
}

// former plot()
func renderPreviewGonumPNG(data []*SeriesResponse, utcOffset int64, metric []seriesRequest, width int) ([]byte, error) {
	if len(data) == 0 || len(data) != len(metric) {
		return nil, fmt.Errorf("render: expected equal non-zero plots and requests, got %d and %d", len(data), len(metric))
	}
	width, height := plotSize(dataFormatPNG, true, width)
	rows, cols := 1, 1
	if 1 < len(data) {
		cols = 2
		rows = (len(data) + 1) / cols
		width /= cols
		height /= cols
	}
	utcOffset %= 24 * 3600
	plots := make([][]byte, len(data))
	for i := range data {
		img, err := renderSinglePlotGonum(data[i], utcOffset, metric[i], width, height)
		if err != nil {
			return nil, fmt.Errorf("render plot %d: %w", i, err)
		}
		plots[i] = img
	}
	if len(plots) == 1 {
		return plots[0], nil
	}
	return composeGrid(plots, width, height, rows, cols) // reuse the go-analyze grid compositor
}

func renderSinglePlotGonum(data *SeriesResponse, utcOffset int64, req seriesRequest, width, height int) ([]byte, error) {
	yl, yh, err := parseYRange(req.yl, req.yh)
	if err != nil {
		return nil, err
	}
	p := gplot.New()
	p.Title.TextStyle.Font.Size = vg.Points(13)
	p.X.Tick.Label.Font.Size = vg.Points(10)
	p.Y.Tick.Label.Font.Size = vg.Points(10)
	if title := previewTitle(req, data.Series.SeriesMeta, utcOffset); title != "" {
		p.Title.Text = title // gonum centers the title along the top by default
	}
	meta := data.Series.SeriesMeta

	// x-axis: real time, ~6 evenly spaced ticks (gonum's default is too sparse)
	layout := "15:04"
	if n := len(data.Series.Time); n > 1 && data.Series.Time[n-1]-data.Series.Time[0] >= 40*3600 {
		layout = "02/01 15:04"
	}
	p.X.Tick.Marker = timeTicks{layout: layout, n: 6}
	p.X.Min = float64(req.from.Unix() + utcOffset)
	p.X.Max = float64(req.to.Unix() + utcOffset)

	// y-axis: nice rounded bounds with headroom, ~8 unit-formatted labels
	dataMin, dataMax, ok := seriesMinMax(data, yl, yh)
	if !ok {
		dataMin, dataMax = 0, 100
	}
	if yl != nil {
		dataMin = *yl
	}
	if yh != nil {
		dataMax = *yh
	}
	step := niceStep((dataMax - dataMin) / 8)
	yMin, yMax := dataMin, dataMax
	if yl == nil {
		// start at 0 (or a nice negative floor when the series goes below zero)
		yMin = math.Min(0, math.Floor(dataMin/step)*step)
	}
	if yh == nil {
		// adding 0.5*step to the height, so that the peaks are visible
		yMax = math.Ceil((dataMax+0.5*step)/step) * step
	}
	if yMax <= yMin {
		yMax = yMin + step
	}
	p.Y.Min, p.Y.Max = yMin, yMax
	p.Y.Tick.Marker = niceTicks{step: step, format: metricValueFormatter(firstMetricType(meta))}

	// helper grid lines
	grid := plotter.NewGrid()
	grid.Horizontal.Color, grid.Horizontal.Width = gridLineColor, vg.Points(0.5)
	grid.Vertical.Color, grid.Vertical.Width = gridLineColor, vg.Points(0.5)
	p.Add(grid)

	// legend; without positioning renders as an overlay, so we place it top left
	legend := gplot.NewLegend()
	legend.TextStyle.Font.Size = vg.Points(11)
	legend.ThumbnailWidth = vg.Points(18)
	legend.Padding = vg.Points(3)
	legend.Top, legend.Left = true, true

	const legendMaxLen = 15
	uniqueWhat := map[string]struct{}{}
	for _, m := range meta {
		uniqueWhat[m.What] = struct{}{}
	}
	for i := range meta {
		lineColor := parseHexColor(meta[i].Color)
		fillColor := lineColor
		fillColor.A = 38

		runs := gonumRuns(data.Series.Time, *data.Series.SeriesData[i], utcOffset, yl, yh)
		hasLine := len(runs) > 0

		// draw a line + polyfill for each run
		for _, run := range runs {
			// fill the area beneath the line
			p.Add(stepAreaToZero{xys: run, color: fillColor})

			// draw the line
			line, err := plotter.NewLine(run)
			if err != nil {
				return nil, err
			}
			line.StepStyle = plotter.PostStep // hold the value, then step
			line.Color = lineColor
			line.Width = vg.Points(1)
			p.Add(line)
		}

		// if we drew at least 1 line (run), draw the legend for this graph
		if hasLine && i < legendMaxLen {
			legend.Add(MetaToLabel(meta[i], len(uniqueWhat), utcOffset), filledThumb{color: lineColor})
		}
	}

	return drawPNG(p, legend, width, height)
}

func previewTitle(req seriesRequest, meta []QuerySeriesMetaV2, utcOffset int64) string {
	name := req.metricName
	if req.customMetricName != "" {
		name = req.customMetricName
	}
	if name == "" {
		return ""
	}
	uniqueWhat := map[string]struct{}{}
	whats := make([]string, 0, 2)
	for _, m := range meta {
		if _, ok := uniqueWhat[m.What]; !ok {
			uniqueWhat[m.What] = struct{}{}
			whats = append(whats, WhatToWhatDesc(m.What))
		}
	}
	if len(whats) == 0 {
		return name
	}
	title := name + ": " + whats[0]
	for _, w := range whats[1:] {
		title += ", " + w
	}
	return title
}

func composeGrid(plots [][]byte, width, height, rows, cols int) ([]byte, error) {
	dst := image.NewRGBA(image.Rect(0, 0, width*cols, height*rows))
	xdraw.Draw(dst, dst.Bounds(), image.White, image.Point{}, xdraw.Src)
	for i, b := range plots {
		img, err := png.Decode(bytes.NewReader(b))
		if err != nil {
			return nil, fmt.Errorf("decode sub-plot %d: %w", i, err)
		}
		x, y := (i%cols)*width, (i/cols)*height
		xdraw.Draw(dst, image.Rect(x, y, x+width, y+height), img, img.Bounds().Min, xdraw.Over)
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, dst); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// fills the area between the plot line and y=0, matching
// gnuplot's fillsteps (positive values fill down to 0, negatives up to 0),
// gonum's built-in fill always closes to the axis minimum.
// NOTE: we're never supposed to have negative values
// but in case we ever do, we support reasonable fill
type stepAreaToZero struct {
	xys   plotter.XYs
	color color.Color
}

func (s stepAreaToZero) Plot(c draw.Canvas, plt *gplot.Plot) {
	if len(s.xys) == 0 {
		return
	}
	trX, trY := plt.Transforms(&c)
	zeroY := trY(0)
	poly := make([]vg.Point, 0, len(s.xys)*2+2)
	poly = append(poly, vg.Point{X: trX(s.xys[0].X), Y: zeroY})
	for i := range s.xys {
		poly = append(poly, vg.Point{X: trX(s.xys[i].X), Y: trY(s.xys[i].Y)})
		if i+1 < len(s.xys) { // post-step: hold this value until the next x
			poly = append(poly, vg.Point{X: trX(s.xys[i+1].X), Y: trY(s.xys[i].Y)})
		}
	}
	poly = append(poly, vg.Point{X: trX(s.xys[len(s.xys)-1].X), Y: zeroY})
	c.FillPolygon(s.color, c.ClipPolygonXY(poly))
}

// draws a solid color square for a legend entry
// gonum's default is a thin line stub.
type filledThumb struct{ color color.Color }

func (t filledThumb) Thumbnail(c *draw.Canvas) {
	pts := []vg.Point{
		{X: c.Min.X, Y: c.Min.Y}, {X: c.Min.X, Y: c.Max.Y},
		{X: c.Max.X, Y: c.Max.Y}, {X: c.Max.X, Y: c.Min.Y},
	}
	c.FillPolygon(t.color, c.ClipPolygonY(pts))
}

// min and max value clamped to y range
func seriesMinMax(data *SeriesResponse, yl, yh *float64) (lo, hi float64, ok bool) {
	lo, hi = math.Inf(1), math.Inf(-1)
	for i := range data.Series.SeriesMeta {
		for _, v := range *data.Series.SeriesData[i] {
			if !v.IsDefined() {
				continue
			}
			f := clampToRange(float64(v), yl, yh)
			lo, hi = math.Min(lo, f), math.Max(hi, f)
		}
	}
	return lo, hi, !math.IsInf(lo, 1)
}

// rounding y step down to the 1/2/5 * nearest_power_of_10
func niceStep(raw float64) float64 {
	if raw <= 0 || math.IsNaN(raw) || math.IsInf(raw, 0) {
		return 1
	}
	exp := math.Floor(math.Log10(raw))
	frac := raw / math.Pow(10, exp)
	var nice float64
	switch {
	case frac <= 1:
		nice = 1
	case frac <= 2:
		nice = 2
	case frac <= 5:
		nice = 5
	default:
		nice = 10
	}
	return nice * math.Pow(10, exp)
}

// splits a series into non-NaN segments (runs) as (time, value) pairs, clamping into the y range
// default gonum doesn't handle NaNs so this is how we simulate gaps
func gonumRuns(t []int64, src []Float64, utcOffset int64, yl, yh *float64) []plotter.XYs {
	var runs []plotter.XYs
	for i := 0; i < len(src) && i < len(t); {
		for i < len(src) && !src[i].IsDefined() {
			i++
		}
		var run plotter.XYs
		for i < len(src) && i < len(t) && src[i].IsDefined() {
			run = append(run, plotter.XY{
				X: float64(t[i] + utcOffset),
				Y: clampToRange(float64(src[i]), yl, yh),
			})
			i++
		}
		if len(run) > 0 {
			runs = append(runs, run)
		}
	}
	return runs
}

// struct to emit a labeled tick every step
// format() rounds the value and can add a suffix for readability like "3G", "1.4GiB", "0.1s", etc.
type niceTicks struct {
	step   float64
	format func(float64) string
}

func (t niceTicks) Ticks(min, max float64) []gplot.Tick {
	if t.step <= 0 {
		return gplot.DefaultTicks{}.Ticks(min, max)
	}
	var ticks []gplot.Tick
	start := math.Ceil(min/t.step) * t.step
	max = max + t.step*1e-9 // safety net for floats
	for v := start; v <= max; v += t.step {
		ticks = append(ticks, gplot.Tick{Value: v, Label: t.format(v)})
	}
	return ticks
}

// emits n+1 evenly spaced ticks
type timeTicks struct {
	layout string
	n      int
}

func (t timeTicks) Ticks(min, max float64) []gplot.Tick {
	ticks := make([]gplot.Tick, 0, t.n+1)
	for i := 0; i <= t.n; i++ {
		v := min + (max-min)*float64(i)/float64(t.n)
		ticks = append(ticks, gplot.Tick{
			Value: v,
			Label: time.Unix(int64(v), 0).UTC().Format(t.layout),
		})
	}
	return ticks
}

func parseHexColor(s string) color.NRGBA {
	c := color.NRGBA{A: 255}
	if len(s) == 7 && s[0] == '#' {
		v := func(b byte) uint8 {
			switch {
			case b >= '0' && b <= '9':
				return b - '0'
			case b >= 'a' && b <= 'f':
				return b - 'a' + 10
			case b >= 'A' && b <= 'F':
				return b - 'A' + 10
			}
			return 0
		}
		c.R = v(s[1])<<4 | v(s[2])
		c.G = v(s[3])<<4 | v(s[4])
		c.B = v(s[5])<<4 | v(s[6])
	}
	return c
}

const previewDPI = 96

// renders the plot plus its legend and returns a width x height PNG.
// renders at 2x resolution and downscales for a sharper look
// (gonum's default 1x raster looks blurry)
// the legend is drawn to the right of the graph in a reserved space (gonum's default makes it an overlay)
func drawPNG(p *gplot.Plot, legend gplot.Legend, width, height int) ([]byte, error) {
	const ss = 2 // supersample factor
	c := vgimg.NewWith(
		vgimg.UseWH(vg.Length(width)*vg.Inch/previewDPI, vg.Length(height)*vg.Inch/previewDPI),
		vgimg.UseDPI(previewDPI*ss),
	)

	// margin on all sides so the plot and its labels don't touch the edge
	margin := vg.Points(8)
	area := draw.Crop(draw.New(c), margin, -margin, margin, -margin)

	legendW := legend.Rectangle(area).Size().X
	if legendW > 0 {
		legendW += vg.Points(8) // padding around the legend
		if maxW := (area.Max.X - area.Min.X) * 0.30; legendW > maxW {
			legendW = maxW
		}
		p.Draw(draw.Crop(area, 0, -legendW, 0, 0))
		legend.Draw(draw.Crop(area, (area.Max.X-area.Min.X)-legendW, 0, 0, 0))
	} else {
		p.Draw(area)
	}

	// downscale the supersampled buffer to target size using CatmullRom kernel
	src := c.Image()
	dst := image.NewRGBA(image.Rect(0, 0, width, height))
	xdraw.Draw(dst, dst.Bounds(), image.White, image.Point{}, xdraw.Src)
	xdraw.CatmullRom.Scale(dst, dst.Bounds(), src, src.Bounds(), xdraw.Over, nil)
	var buf bytes.Buffer
	if err := png.Encode(&buf, dst); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func parseYRange(yl, yh string) (*float64, *float64, error) {
	var minP, maxP *float64
	if yl != "" {
		v, err := strconv.ParseFloat(yl, 64)
		if err != nil {
			return nil, nil, httpErr(400, fmt.Errorf("failed to parse y-axis lower bound %q: %w", yl, err))
		}
		minP = &v
	}
	if yh != "" {
		v, err := strconv.ParseFloat(yh, 64)
		if err != nil {
			return nil, nil, httpErr(400, fmt.Errorf("failed to parse y-axis upper bound %q: %w", yh, err))
		}
		maxP = &v
	}
	return minP, maxP, nil
}

func clampToRange(v float64, yl, yh *float64) float64 {
	if yl != nil && v < *yl {
		return *yl
	}
	if yh != nil && v > *yh {
		return *yh
	}
	return v
}

func firstMetricType(meta []QuerySeriesMetaV2) string {
	if len(meta) == 0 {
		return ""
	}
	return meta[0].MetricType
}

func metricValueFormatter(metricType string) func(float64) string {
	switch metricType {
	case "second":
		return func(v float64) string { return shortFloat(v) + "s" }
	case "millisecond":
		return func(v float64) string { return shortFloat(v) + "ms" }
	case "microsecond":
		return func(v float64) string { return shortFloat(v) + "us" }
	case "nanosecond":
		return func(v float64) string { return shortFloat(v) + "ns" }
	case "byte":
		return byteValueFormatter
	default:
		return siValueFormatter
	}
}

// float with 4 decimal points; exponential notation for large numbers
func shortFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', 4, 64)
}

// prefixes for ticks; for large values use letter suffixes, for <1 values use their plain decimal form
var siPrefixes = []struct {
	factor float64
	suffix string
}{
	{1e12, "T"},
	{1e9, "G"},
	{1e6, "M"},
	{1e3, "K"},
}

var bytePrefixes = []struct {
	factor float64
	suffix string
}{
	{1 << 40, "TiB"},
	{1 << 30, "GiB"},
	{1 << 20, "MiB"},
	{1 << 10, "KiB"},
}

func siValueFormatter(v float64) string {
	a := math.Abs(v)
	if v == 0 || math.IsNaN(a) || math.IsInf(a, 0) {
		return shortFloat(v)
	}
	for _, p := range siPrefixes {
		if a >= p.factor {
			return strconv.FormatFloat(v/p.factor, 'g', 4, 64) + p.suffix
		}
	}
	return shortFloat(v)
}

func byteValueFormatter(v float64) string {
	a := math.Abs(v)
	for _, p := range bytePrefixes {
		if a >= p.factor {
			return strconv.FormatFloat(v/p.factor, 'f', 1, 64) + p.suffix
		}
	}
	// for bytes we use maximum precision needed
	return strconv.FormatFloat(v, 'f', -1, 64) + "B"
}

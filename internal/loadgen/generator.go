package loadgen

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/vkcom/statshouse/internal/api"
	"github.com/vkcom/statshouse/internal/format"

	"pgregory.net/rand"

	"github.com/vkcom/statshouse-go"
)

const metricPrefixG = "loadgen_"

// tag names signify how often they change
const constTag = "const"    // 1 tag
const secTag = "sec"        // 2 tag
const minTag = "min"        // 3 tag
const tenMinTag = "ten_min" // 4 tag
const rawTag = "raw"        // 5 tag
const hostTag = "_h"

type GenericMetric interface {
	Write(c *statshouse.Client)
	Update(now time.Time, rng *rand.Rand)
	Ensure(ctx context.Context, c *api.Client)
}

type valueMetric struct {
	name         string
	tags         statshouse.NamedTags
	resolution   int
	isPercentile bool // use values normally distributed around 10 and 100 instead of random walk
	changingHost bool

	value float64
}

func (m *valueMetric) Write(c *statshouse.Client) {
	c.NamedValue(m.name, m.tags, m.value)
}

func (m *valueMetric) Update(now time.Time, rng *rand.Rand) {
	updateNamedTags(m.tags, now)
	if m.changingHost {
		for i := range m.tags {
			if m.tags[i][0] == hostTag {
				m.tags[i][1] = "min_" + now.Format("15:04")
			}
		}
	}
	if m.isPercentile {
		// 90% of values are around 10, rest around 100
		// so we can look at median and 95 percentile to validate
		if rng.Float64() < 0.9 {
			m.value = 10
		} else {
			m.value = 100
		}
		m.value += rng.NormFloat64()
		if m.value < 0 {
			m.value = 0
		}
		return
	}
	if rng.Float64() < 0.5 {
		m.value += rng.Float64()
	} else {
		// encourage metrics to go up
		m.value -= rng.Float64() / 2
	}
}

func (m *valueMetric) Ensure(ctx context.Context, c *api.Client) {
	metric, err := c.GetMetric(ctx, m.name)
	if err != nil {
		log.Printf("error getting metric: %v", err)
	}
	setCommonMetricValues(&metric.Metric)
	metric.Metric.Name = m.name
	metric.Metric.Resolution = m.resolution
	if m.isPercentile {
		metric.Metric.Kind = format.MetricKindValuePercentiles
		metric.Metric.HasPercentiles = true
	} else {
		metric.Metric.Kind = format.MetricKindValue
	}
	err = c.PostMetric(ctx, metric)
	if err != nil {
		log.Printf("error creating metric: %v", err)
	}
}

type countMetric struct {
	name       string
	tags       statshouse.NamedTags
	resolution int
	count      int
}

func (m *countMetric) Write(c *statshouse.Client) {
	c.NamedCount(m.name, m.tags, float64(m.count))
}

func (m *countMetric) Update(now time.Time, rng *rand.Rand) {
	if rng.Float64() < 0.5 {
		m.count++
	}
	updateNamedTags(m.tags, now)
}

func (m *countMetric) Ensure(ctx context.Context, c *api.Client) {
	metric, err := c.GetMetric(ctx, m.name)
	if err != nil {
		log.Printf("error getting metric: %v", err)
	}
	setCommonMetricValues(&metric.Metric)
	metric.Metric.Name = m.name
	metric.Metric.Resolution = m.resolution
	metric.Metric.Kind = format.MetricKindCounter
	err = c.PostMetric(ctx, metric)
	if err != nil {
		log.Printf("error creating metric: %v", err)
	}
}

type stringTopMetric struct {
	name       string
	tags       statshouse.NamedTags
	resolution int
	card       int
	stringTop  string
}

func (m *stringTopMetric) Write(c *statshouse.Client) {
	c.NamedStringsTop(m.name, m.tags, []string{m.stringTop})
}

func (m *stringTopMetric) Update(now time.Time, rng *rand.Rand) {
	var v int
	for i := 0; i < m.card; i++ {
		if rng.Float64() < 0.5 {
			v++
		}
	}
	m.stringTop = fmt.Sprint(now.Format("15:04")+"value_", v)
	updateNamedTags(m.tags, now)
}

func (m *stringTopMetric) Ensure(ctx context.Context, c *api.Client) {
	metric, err := c.GetMetric(ctx, m.name)
	if err != nil {
		log.Printf("error getting metric: %v", err)
	}
	setCommonMetricValues(&metric.Metric)
	metric.Metric.Name = m.name
	metric.Metric.Resolution = m.resolution
	metric.Metric.Kind = format.MetricKindMixed
	metric.Metric.StringTopDescription = "string_top"
	err = c.PostMetric(ctx, metric)
	if err != nil {
		log.Printf("error creating metric: %v", err)
	}
}

func setCommonMetricValues(mv *format.MetricMetaValue) {
	mv.Visible = true
	mv.Tags = []format.MetricMetaTag{
		{
			Description: "environment",
			Raw:         false,
		},
		{
			Name: constTag,
		},
		{
			Name: secTag,
		},
		{
			Name: minTag,
		},
		{
			Name: tenMinTag,
		},
		{
			Name: rawTag,
			Raw:  true,
		},
	}
}

func updateNamedTags(tags statshouse.NamedTags, now time.Time) {
	for i := range tags {
		switch tags[i][0] {
		case constTag:
			continue
		case secTag:
			tags[i][1] = now.Format("15:04:05")
		case minTag:
			tags[i][1] = now.Format("15:04")
		case tenMinTag:
			tags[i][1] = now.Truncate(10 * time.Minute).Format("15:04")
		}
	}
}

type Generator struct {
	rng     *rand.Rand
	metrics []GenericMetric
	clients []*statshouse.Client
}

func (g *Generator) goRun(ctx context.Context, frequency time.Duration, metrics []GenericMetric) {
	t := time.NewTicker(frequency)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case s := <-t.C:
			for _, m := range metrics {
				m.Update(s, g.rng)
				for _, c := range g.clients {
					m.Write(c)
				}
			}
		}
	}
}

func (g *Generator) AddConstCounter(resolution int) {
	m := countMetric{
		name:       metricPrefixG + "const_cnt_" + fmt.Sprint(resolution),
		tags:       statshouse.NamedTags{{constTag, "constant"}},
		resolution: resolution,
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddConstValue(resolution int) {
	m := valueMetric{
		name:       metricPrefixG + "const_val_" + fmt.Sprint(resolution),
		tags:       statshouse.NamedTags{{constTag, "constant"}},
		resolution: resolution,
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddConstValueHost(resolution int, host string) {
	m := valueMetric{
		name:       metricPrefixG + "const_val_host_" + fmt.Sprint(resolution),
		tags:       statshouse.NamedTags{{constTag, "constant"}, {hostTag, host}},
		resolution: resolution,
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddChangingCounter(resolution int) {
	m := countMetric{
		name: metricPrefixG + "changing_cnt_" + fmt.Sprint(resolution),
		tags: statshouse.NamedTags{
			{constTag, "constant"},
			{secTag, ""},
			{minTag, ""},
			{tenMinTag, ""},
		},
		resolution: resolution,
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddChangingValue(resolution int) {
	m := valueMetric{
		name: metricPrefixG + "changing_val_" + fmt.Sprint(resolution),
		tags: statshouse.NamedTags{
			{constTag, "constant"},
			{secTag, ""},
			{minTag, ""},
			{tenMinTag, ""},
		},
		resolution: resolution,
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddChangingValueHost(resolution int) {
	m := valueMetric{
		name: metricPrefixG + "changing_val_host_" + fmt.Sprint(resolution),
		tags: statshouse.NamedTags{
			{constTag, "constant"},
			{secTag, ""},
			{minTag, ""},
			{tenMinTag, ""},
			{hostTag, ""},
		},
		resolution:   resolution,
		changingHost: true,
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddChangingStringTop(resolution int, card int) {
	m := stringTopMetric{
		name: metricPrefixG + "changing_top_" + fmt.Sprint(resolution),
		tags: statshouse.NamedTags{
			{constTag, "constant"},
		},
		resolution: resolution,
		card:       card,
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddConstPercentile(resolution int) {
	m := valueMetric{
		name:         metricPrefixG + "const_per_" + fmt.Sprint(resolution),
		tags:         statshouse.NamedTags{{constTag, "constant"}},
		resolution:   resolution,
		isPercentile: true,
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddChangingPercentile(resolution int) {
	m := valueMetric{
		name: metricPrefixG + "changing_per_" + fmt.Sprint(resolution),
		tags: statshouse.NamedTags{
			{constTag, "constant"},
			{secTag, ""},
			{minTag, ""},
			{tenMinTag, ""},
		},
		resolution:   resolution,
		isPercentile: true,
	}
	g.metrics = append(g.metrics, &m)
}

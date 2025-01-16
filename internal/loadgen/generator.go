package loadgen

import (
	"context"
	"fmt"
	"github.com/vkcom/statshouse-go"
	"pgregory.net/rand"
	"time"
)

const metricPrefixG = "loadgen_"

// tag names signify how often they change
const constTag = "const"
const secTag = "sec"
const minTag = "min"
const tenMinTag = "ten_min"

type GenericMetric interface {
	Write(c *statshouse.Client)
	Update(now time.Time, rng *rand.Rand)
	// TODO: ensure exists
}

type valueMetric struct {
	name         string
	tags         statshouse.NamedTags
	value        float64
	isPercentile bool // use values normally distributed around 10 and 100 instead of random walk
}

func (m *valueMetric) Write(c *statshouse.Client) {
	c.NamedValue(m.name, m.tags, m.value)
}

func (m *valueMetric) Update(now time.Time, rng *rand.Rand) {
	updateNamedTags(m.tags, now)
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

type countMetric struct {
	name  string
	tags  statshouse.NamedTags
	count int
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

type stringTopMetric struct {
	name      string
	tags      statshouse.NamedTags
	card      int
	stringTop string
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
	m.stringTop = fmt.Sprint("value_", v)
	updateNamedTags(m.tags, now)
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

func (g *Generator) goRun(ctx context.Context) {
	t := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			for _, m := range g.metrics {
				m.Update(now, g.rng)
				for _, c := range g.clients {
					m.Write(c)
				}
			}
		}
	}
}

func (g *Generator) AddConstCounter(namePostfix string) {
	m := countMetric{
		name: metricPrefixG + "const_cnt_" + namePostfix,
		tags: statshouse.NamedTags{{constTag, "constant"}},
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddConstValue(namePostfix string) {
	m := valueMetric{
		name: metricPrefixG + "const_val_" + namePostfix,
		tags: statshouse.NamedTags{{constTag, "constant"}},
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddChangingCounter(namePostfix string) {
	m := countMetric{
		name: metricPrefixG + "changing_cnt_" + namePostfix,
		tags: statshouse.NamedTags{
			{constTag, "constant"},
			{secTag, ""},
			{minTag, ""},
			{tenMinTag, ""},
		},
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddChangingValue(namePostfix string) {
	m := valueMetric{
		name: metricPrefixG + "changing_val_" + namePostfix,
		tags: statshouse.NamedTags{
			{constTag, "constant"},
			{secTag, ""},
			{minTag, ""},
			{tenMinTag, ""},
		},
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddChangingStringTop(namePostfix string, card int) {
	m := stringTopMetric{
		name: metricPrefixG + "changing_top_" + namePostfix,
		tags: statshouse.NamedTags{
			{constTag, "constant"},
		},
		card: card,
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddConstPercentile(namePostfix string) {
	m := valueMetric{
		name:         metricPrefixG + "const_per_" + namePostfix,
		tags:         statshouse.NamedTags{{constTag, "constant"}},
		isPercentile: true,
	}
	g.metrics = append(g.metrics, &m)
}

func (g *Generator) AddChangingPercentile(namePostfix string) {
	m := valueMetric{
		name: metricPrefixG + "changing_per_" + namePostfix,
		tags: statshouse.NamedTags{
			{constTag, "constant"},
			{secTag, ""},
			{minTag, ""},
			{tenMinTag, ""},
		},
		isPercentile: true,
	}
	g.metrics = append(g.metrics, &m)
}

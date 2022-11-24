// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"context"
)

type testSyncer struct {
	mx     sync.Mutex
	groups [][]promTarget
}

func (t *testSyncer) SyncTargets(ctx context.Context) ([]promTarget, error) {
	t.mx.Lock()
	defer t.mx.Unlock()
	if len(t.groups) > 0 {
		groups := t.groups[0]
		t.groups = t.groups[1:]
		return groups, nil
	}
	<-ctx.Done()
	return nil, nil
}

type metricType int

const (
	counter metricType = iota
	gauge              = iota
	testJob            = "test_job"
)

type metrics struct {
	mx sync.Mutex
	m  map[string]*metric
}

type metric struct {
	name  string
	help  string
	typ   metricType
	value float64
	tags  [][2]string
}

type metricPusherTestImpl struct {
	mx       sync.Mutex
	metrics  []tlstatshouse.MetricBytes
	counters map[string]float64
	values   map[string][]float64
}

func (m *metrics) add(name string, v float64) {
	m.mx.Lock()
	defer m.mx.Unlock()
	vOld, ok := m.m[name]
	if ok {
		vOld.value += v
	}
}

func newMetricPusherTestImpl() *metricPusherTestImpl {
	return &metricPusherTestImpl{counters: map[string]float64{}, values: map[string][]float64{}}
}

func getUniqStringRaw(metric string, tags [][2]string) string {
	s := metric
	sort.Slice(tags, func(i, j int) bool {
		return tags[i][0] < tags[j][0]
	})
	s += "{"
	for i, tag := range tags {
		s += tag[0] + "=\"" + tag[1] + "\""
		if i != len(tags)-1 {
			s += ","
		}
	}
	s += "}"
	return s
}

func getUniqStringMetricWithKeys(m *metric, job, address string) string {
	t := make([][2]string, len(m.tags))
	copy(t, m.tags)
	t = append(t, [2]string{model.JobLabel, job})
	t = append(t, [2]string{model.InstanceLabel, address})
	return getUniqStringRaw(m.name, t)
}

func getUniqStringMetric(m *metric) string {
	return getUniqStringRaw(m.name, m.tags)
}

func getUniqString(metric *tlstatshouse.MetricBytes) string {
	s := string(metric.Name)
	tags := make([][2]string, 0)
	for _, tag := range metric.Tags {
		tags = append(tags, [2]string{string(tag.Key), string(tag.Value)})
	}
	return getUniqStringRaw(s, tags)
}

func (m *metricPusherTestImpl) PushLocal(metric *tlstatshouse.MetricBytes) {
	if strings.HasPrefix(string(metric.Name), "__") {
		return
	}
	m.mx.Lock()
	defer m.mx.Unlock()
	m.metrics = append(m.metrics, *metric)
	series := getUniqString(metric)
	if metric.Counter > 0 {
		count := m.counters[series]
		m.counters[series] = count + metric.Counter
	}
	if len(metric.Value) > 0 {
		m.values[series] = append(m.values[series], metric.Value...)
	}
}

func (m *metricPusherTestImpl) IsLocal() bool {
	return true
}

const scrapeIntervalValue = time.Second

func counterMetricBytes(help, name string, tags [][2]string, value float64) string {
	series := getUniqStringRaw(name, tags)
	str := fmt.Sprintf(`# HELP %s %s
# TYPE %s counter
%s %d`+"\n", name, help, name, series, int(value+1))
	return str
}

func gaugeMetricBytes(help, name string, tags [][2]string, value float64) string {
	series := getUniqStringRaw(name, tags)
	str := fmt.Sprintf(`# HELP %s %s
# TYPE %s gauge
%s %d`+"\n", name, help, name, series, int(value+1))
	return str
}

func runScraper(t *testing.T, addr string, scrapeInterval time.Duration) (*Scraper, *metricPusherTestImpl) {
	pusher := newMetricPusherTestImpl()
	uri := "http://" + addr + "/metrics"
	syncer := &testSyncer{
		groups: [][]promTarget{{{
			jobName:               testJob,
			url:                   uri,
			labels:                nil,
			scrapeInterval:        scrapeInterval,
			honorTimestamps:       false,
			honorLabels:           false,
			scrapeTimeout:         scrapeInterval,
			bodySizeLimit:         0,
			LabelLimit:            0,
			LabelNameLengthLimit:  0,
			LabelValueLengthLimit: 0,
			instance:              addr,
		}}},
	}
	scraper := NewScraper(pusher, syncer, log.Default(), log.Default())
	go scraper.Run()
	return scraper, pusher
}

func runPromClient(t *testing.T, m *metrics) (*http.Server, net.Listener) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		m.mx.Lock()
		defer m.mx.Unlock()
		buffer := bytes.NewBuffer(nil)
		for _, metric := range m.m {
			if metric.typ == counter {
				str := counterMetricBytes(metric.help, metric.name, metric.tags, metric.value)
				buffer.WriteString(str)
			} else if metric.typ == gauge {
				str := gaugeMetricBytes(metric.help, metric.name, metric.tags, metric.value)
				buffer.WriteString(str)
			}
		}
		w.WriteHeader(200)
		_, _ = w.Write(buffer.Bytes())
	})
	server := http.Server{
		Addr:    ":0",
		Handler: mux,
	}
	ln, err := net.Listen("tcp4", ":0")
	require.NoError(t, err)
	go func() {
		_ = server.Serve(ln)
	}()
	return &server, ln
}

func T1estScrapeCounter(t *testing.T) {
	metric1 := &metric{
		name:  "test_counter",
		help:  "test metric",
		typ:   counter,
		value: 3,
		tags:  [][2]string{{"k", "v"}},
	}
	metric2 := &metric{
		name:  "test_counter",
		help:  "test metric",
		typ:   counter,
		value: 3,
		tags:  [][2]string{{"k", "v2"}},
	}
	metric1Series := getUniqStringMetric(metric1)
	metric2Series := getUniqStringMetric(metric2)

	metrics := &metrics{m: map[string]*metric{
		metric1Series: metric1,
		metric2Series: metric2,
	}}
	server, ln := runPromClient(t, metrics)
	defer server.Close()
	addr := ln.Addr().String()
	metric1SeriesAfterHandle := getUniqStringMetricWithKeys(metric1, testJob, addr)
	metric2SeriesAfterHandle := getUniqStringMetricWithKeys(metric2, testJob, addr)
	scraper, pusher := runScraper(t, addr, scrapeIntervalValue)
	defer scraper.Stop()
	time.Sleep(scrapeIntervalValue + time.Second*2)
	pusher.mx.Lock()
	_, containsMetric1 := pusher.counters[metric1SeriesAfterHandle]
	_, containsMetric2 := pusher.counters[metric2SeriesAfterHandle]
	pusher.mx.Unlock()
	require.False(t, containsMetric1)
	require.False(t, containsMetric2)
	metrics.add(metric1Series, 1)
	time.Sleep(scrapeIntervalValue + time.Second*2)
	pusher.mx.Lock()
	count, containsMetric1 := pusher.counters[metric1SeriesAfterHandle]
	pushedMetrics := pusher.metrics
	pusher.mx.Unlock()
	require.Len(t, pushedMetrics, 1)
	require.True(t, containsMetric1)
	require.Equal(t, 1, int(count))
	metrics.add(metric2Series, 1)
	time.Sleep(scrapeIntervalValue + time.Second)
	pusher.mx.Lock()
	count, containsMetric2 = pusher.counters[metric2SeriesAfterHandle]
	pushedMetrics = pusher.metrics
	pusher.mx.Unlock()
	require.True(t, containsMetric2)
	require.Len(t, pushedMetrics, 2)
	require.Equal(t, 1, int(count))
}

func T1estScrapeGauge(t *testing.T) {
	metric1 := &metric{
		name:  "test_gauge",
		help:  "test metric",
		typ:   gauge,
		value: 3,
	}
	metric1Series := getUniqStringMetric(metric1)

	metrics := &metrics{m: map[string]*metric{
		metric1Series: metric1,
	}}
	server, ln := runPromClient(t, metrics)
	defer server.Close()
	addr := ln.Addr().String()
	metric1SeriesAfterHandle := getUniqStringMetricWithKeys(metric1, testJob, addr)
	scraper, pusher := runScraper(t, addr, scrapeIntervalValue)
	defer scraper.Stop()
	time.Sleep(scrapeIntervalValue + scrapeIntervalValue/2)
	pusher.mx.Lock()
	values, containsMetric := pusher.values[metric1SeriesAfterHandle]
	pushedMetrics := pusher.metrics
	pusher.mx.Unlock()
	require.True(t, containsMetric)
	require.GreaterOrEqual(t, len(pushedMetrics), 1)
	require.GreaterOrEqual(t, len(values), 1)
	require.Equal(t, 4, int(values[0]))
}

func Test_filterLabels(t *testing.T) {
	tests := []struct {
		name string
		args labels.Labels
		want labels.Labels
	}{
		{"empty", labels.Labels{}, labels.Labels{}},
		{"single_ok", labels.Labels{{Name: "k", Value: "a"}}, labels.Labels{{Name: "k", Value: "a"}}},
		{"single_bad", labels.Labels{{Name: "__k", Value: "a"}}, labels.Labels{}},
		{"multiple_1", labels.Labels{{Name: "__k", Value: "a"}, {Name: "k", Value: "a"}}, labels.Labels{{Name: "k", Value: "a"}}},
		{"multiple_2", labels.Labels{{Name: "k", Value: "a"}, {Name: "__k", Value: "a"}}, labels.Labels{{Name: "k", Value: "a"}}},
		{"multiple_3", labels.Labels{{Name: "__k", Value: "a"}, {Name: "__k1", Value: "a"}}, labels.Labels{}},
		{"multiple_3", labels.Labels{{Name: "k", Value: "a"}, {Name: "k1", Value: "a"}}, labels.Labels{{Name: "k", Value: "a"}, {Name: "k1", Value: "a"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filterLabels(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filterLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

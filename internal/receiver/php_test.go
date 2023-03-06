// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package receiver_test

import (
	"bytes"
	"math"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/mapping"
	"github.com/vkcom/statshouse/internal/receiver"
)

var (
	_ *phpMachine // for staticcheck: type phpMachine is unused (U1000)
)

// cd internal/receiver
// скопировать data/packages/statlogs-writer/src/StatsHouse.php в эту папку
// поставить пхп
// STATSHOUSE_RUN_PHP_INTEGRATION_TEST=1 go test -run=PHPRoundtrip

const (
	phpPrologue = `<?php
function tuple(...$args) { return $args; } // KPHP polyfill

require 'StatsHouse.php';
use VK\Statlogs\StatsHouse;
$sh = new StatsHouse('udp://127.0.0.1:13337');
`
	phpStatsHouseAddr = "127.0.0.1:13337"
	recvTimeout       = 1 * time.Second

	maxPacketSize = 1232
	maxTSSize     = maxPacketSize - format.MaxStringLen /* value */ - 4*10 /* headers etc. */
)

var (
	writeValueTmpl = template.Must(template.New("writeValue").Parse( /* language=GoTemplate */ `
$fn = function() use ($sh) {
  $err = $sh->{{.Func}}('{{.Name}}', [{{range $k, $v := .Tags}} '{{$v.K}}' => '{{$v.V}}', {{end}}], {{.Count}}, {{.Timestamp}} );
  if ($err !== null) {
    print_r($err);
    exit(1);
  }
};
if ({{.Shutdown}}) {
  register_shutdown_function($fn);
} else {
  $fn();
}
`))

	writeValuesTmpl = template.Must(template.New("writeValues").Parse( /* language=GoTemplate */ `
$fn = function() use ($sh) {
  $err = $sh->{{.Func}}('{{.Name}}', [{{range $k, $v := .Tags}} '{{$v.K}}' => '{{$v.V}}', {{end}}], [{{range .Values}} {{.}}, {{end}}], {{.Count}}, {{.Timestamp}});
  if ($err !== null) {
    print_r($err);
    exit(1);
  }
};
if ({{.Shutdown}}) {
  register_shutdown_function($fn);
} else {
  $fn();
}
`))
)

type valuesInfo struct {
	Func      string
	Name      string
	Tags      []tag
	Values    interface{}
	Count     float64
	Timestamp int64
	Shutdown  int
}

func ident() *rapid.Generator[string] {
	return identLike(format.MaxStringLen)
}

func identLike(maxLen int) *rapid.Generator[string] {
	return rapid.StringMatching("[a-zA-Z][a-zA-Z0-9_]*").Filter(func(s string) bool { return len(s) <= maxLen })
}

func ts(name string, tags []tag) string {
	sortSlice(tags)

	var b strings.Builder
	b.WriteString(name)
	b.WriteRune('{')
	for _, tag := range tags {
		b.WriteString(tag.K)
		b.WriteRune('=')
		b.WriteString(tag.V)
		b.WriteRune(' ')
	}
	b.WriteRune('}')

	return b.String()
}

type intsMap map[string][]int64

func (m intsMap) merge(key string, values []int64) {
	v := append(m[key], values...) //nolint:gocritic // appendAssign: append result not assigned to the same slice
	sort.Slice(v, func(i int, j int) bool { return v[i] < v[j] })
	m[key] = v
}

func (m intsMap) count(key string, value int64) {
	if len(m[key]) > 0 {
		m[key][0] += value
	} else {
		m[key] = []int64{value}
	}
}

func (m intsMap) size() int {
	n := 0
	for _, v := range m {
		n += len(v)
	}
	return n
}

type floatsMap map[string][]float64

func (m floatsMap) merge(key string, values []float64) {
	v := append(m[key], values...) //nolint:gocritic // appendAssign: append result not assigned to the same slice
	sort.Slice(v, func(i int, j int) bool { return v[i] < v[j] })
	m[key] = v
}

func (m floatsMap) count(key string, value float64) {
	if len(m[key]) > 0 {
		m[key][0] += value
	} else {
		m[key] = []float64{value}
	}
}

func (m floatsMap) size() int {
	n := 0
	for _, v := range m {
		n += len(v)
	}
	return n
}

func (m floatsMap) sum() float64 {
	s := 0.0
	c := 0.0 // Kahan summation
	for _, v := range m {
		for _, f := range v {
			y := f - c
			t := s + y
			c = (t - s) - y
			s = t
		}
	}
	return s
}

type stringsMap map[string][]string

func (m stringsMap) merge(key string, values []string) {
	v := append(m[key], values...) //nolint:gocritic // appendAssign: append result not assigned to the same slice
	sort.Strings(v)
	m[key] = v
}

func (m stringsMap) size() int {
	n := 0
	for _, v := range m {
		n += len(v)
	}
	return n
}

type phpMachine struct {
	numCounts      int
	sumCountsOther floatsMap
	sumTimestamp   intsMap
	counterMetrics floatsMap
	valueMetrics   floatsMap
	uniqueMetrics  intsMap
	stopMetrics    stringsMap
	code           []string
}

func (p *phpMachine) Init(_ *rapid.T) {
	p.numCounts = 0
	p.sumCountsOther = floatsMap{}
	p.sumTimestamp = intsMap{}
	p.counterMetrics = floatsMap{}
	p.valueMetrics = floatsMap{}
	p.uniqueMetrics = intsMap{}
	p.stopMetrics = stringsMap{}
}

func (p *phpMachine) AppendWriteCount(t *rapid.T) {
	info := valuesInfo{
		Func:      "writeCount",
		Name:      ident().Draw(t, "name"),
		Tags:      tagsSlice().Draw(t, "tags"),
		Values:    []float64{rapid.Float64Range(0, math.MaxInt32).Draw(t, "value")},
		Shutdown:  rapid.IntRange(0, 1).Draw(t, "shutdown"),
		Count:     float64(rapid.IntRange(1, 10).Draw(t, "count")),
		Timestamp: int64(rapid.IntRange(0, 1).Draw(t, "timestamp")),
	}

	k := ts(info.Name, info.Tags)
	if len(k) > maxTSSize {
		return
	}

	p.counterMetrics.count(k, info.Count)
	p.sumTimestamp.count(k, info.Timestamp)
	p.numCounts++

	var buf bytes.Buffer
	err := writeValueTmpl.Execute(&buf, info)
	require.NoError(t, err)

	p.code = append(p.code, buf.String())
}

func (p *phpMachine) AppendWriteValue(t *rapid.T) {
	info := valuesInfo{
		Func:      "writeValue",
		Name:      ident().Draw(t, "name"),
		Tags:      tagsSlice().Draw(t, "tags"),
		Values:    rapid.SliceOfN(rapid.Float64Range(0, math.MaxFloat32), 1, -1).Draw(t, "values"),
		Shutdown:  rapid.IntRange(0, 1).Draw(t, "shutdown"),
		Count:     float64(rapid.IntRange(1, 10).Draw(t, "count")),
		Timestamp: int64(rapid.IntRange(0, 1).Draw(t, "timestamp")),
	}

	k := ts(info.Name, info.Tags)
	if len(k) > maxTSSize {
		return
	}
	p.sumCountsOther.count(k, info.Count)
	p.sumTimestamp.count(k, info.Timestamp)
	p.valueMetrics.merge(k, info.Values.([]float64))

	var buf bytes.Buffer
	err := writeValuesTmpl.Execute(&buf, info)
	require.NoError(t, err)

	p.code = append(p.code, buf.String())
}

func (p *phpMachine) AppendWriteUnique(t *rapid.T) {
	info := valuesInfo{
		Func:      "writeUnique",
		Name:      ident().Draw(t, "name"),
		Tags:      tagsSlice().Draw(t, "tags"),
		Values:    rapid.SliceOfN(rapid.Int64(), 1, -1).Draw(t, "values"),
		Shutdown:  rapid.IntRange(0, 1).Draw(t, "shutdown"),
		Count:     float64(rapid.IntRange(1, 10).Draw(t, "count")),
		Timestamp: int64(rapid.IntRange(0, 1).Draw(t, "timestamp")),
	}

	k := ts(info.Name, info.Tags)
	if len(k) > maxTSSize {
		return
	}
	p.sumCountsOther.count(k, info.Count)
	p.sumTimestamp.count(k, info.Timestamp)
	p.uniqueMetrics.merge(k, info.Values.([]int64))

	var buf bytes.Buffer
	err := writeValuesTmpl.Execute(&buf, info)
	require.NoError(t, err)

	p.code = append(p.code, buf.String())
}

func (p *phpMachine) AppendWriteSTop(t *rapid.T) {
	info := valuesInfo{
		Func:      "writeSTop",
		Name:      ident().Draw(t, "name"),
		Tags:      tagsSlice().Draw(t, "tags"),
		Values:    rapid.SliceOfN(identLike(format.MaxStringLen), 1, -1).Draw(t, "values"), // not arbitrary strings for easier text encoding
		Shutdown:  rapid.IntRange(0, 1).Draw(t, "shutdown"),
		Count:     float64(rapid.IntRange(1, 10).Draw(t, "count")),
		Timestamp: int64(rapid.IntRange(0, 1).Draw(t, "timestamp")),
	}

	k := ts(info.Name, info.Tags)
	if len(k) > maxTSSize {
		return
	}
	p.sumCountsOther.count(k, info.Count)
	p.sumTimestamp.count(k, info.Timestamp)
	values := info.Values.([]string)
	p.stopMetrics.merge(k, values)

	for i := range values {
		values[i] = "'" + values[i] + "'" // no need to quote anything
	}
	var buf bytes.Buffer
	err := writeValuesTmpl.Execute(&buf, info)
	require.NoError(t, err)

	p.code = append(p.code, buf.String())
}

func (p *phpMachine) Run(t *rapid.T) {
	total := p.numCounts + p.valueMetrics.size() + p.uniqueMetrics.size() + p.stopMetrics.size()
	if total == 0 {
		t.Skip("no metrics to send/receive")
	}

	recv, err := receiver.ListenUDP(phpStatsHouseAddr, receiver.DefaultConnBufSize, false, nil, nil)
	t.Logf("listen err %v for %v", err, phpStatsHouseAddr)
	require.NoError(t, err)
	defer func() { _ = recv.Close() }()

	var (
		recvErr        error
		numCounts      = 0
		sumMults       = floatsMap{}
		sumTimestamp   = intsMap{}
		counterMetrics = floatsMap{}
		valueMetrics   = floatsMap{}
		uniqueMetrics  = intsMap{}
		stopMetrics    = stringsMap{}
		serveErr       = make(chan error)
	)
	go func() {
		timer := time.AfterFunc(recvTimeout, func() { _ = recv.Close() })
		defer timer.Stop()
		serveErr <- recv.Serve(receiver.CallbackHandler{
			Metrics: func(m *tlstatshouse.MetricBytes, cb mapping.MapCallbackFunc) (h data_model.MappedMetricHeader, done bool) {
				sumTimestamp.count(ts(string(m.Name), receivedSlice(m.Tags, nil)), int64(m.Ts))
				switch {
				case len(m.Value) > 0:
					valueMetrics.merge(ts(string(m.Name), receivedSlice(m.Tags, nil)), m.Value)
					sumMults.count(ts(string(m.Name), receivedSlice(m.Tags, nil)), m.Counter)
				case len(m.Unique) > 0:
					uniqueMetrics.merge(ts(string(m.Name), receivedSlice(m.Tags, nil)), m.Unique)
					sumMults.count(ts(string(m.Name), receivedSlice(m.Tags, nil)), m.Counter)
				case len(m.Value) == 0 && len(m.Unique) == 0 && m.Counter > 0:
					counterMetrics.count(ts(string(m.Name), receivedSlice(m.Tags, nil)), m.Counter)
					numCounts++
				}
				if recvErr != nil || counterMetrics.size()+valueMetrics.size()+uniqueMetrics.size()+stopMetrics.size() >= total {
					_ = recv.Close()
				}
				return h, true
			},
			ParseError: func(pkt []byte, err error) {
				if recvErr == nil {
					recvErr = err
				}
				_ = recv.Close()
			},
		})
	}()

	code := strings.Join(append([]string{phpPrologue}, p.code...), "\n")
	p.code = nil

	cmd := exec.Command("php")
	cmd.Stdin = strings.NewReader(code)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		t.Logf("php: %q", out)
	}
	require.NoError(t, err)

	require.NoError(t, <-serveErr)
	require.NoError(t, recvErr)

	require.Equal(t, p.counterMetrics, counterMetrics)
	require.Equal(t, p.valueMetrics, valueMetrics)
	require.Equal(t, p.uniqueMetrics, uniqueMetrics)
	require.Equal(t, p.stopMetrics, stopMetrics)
	require.Equal(t, p.numCounts, numCounts)

	p.Init(t)
}

func (p *phpMachine) Check(*rapid.T) {}

func receivedSlice(m []tl.DictionaryFieldStringBytes, skey []byte) []tag {
	res := make([]tag, 0, len(m))
	for i := range m {
		res = append(res, tag{K: string(m[i].Key), V: string(m[i].Value)})
	}
	if len(skey) != 0 {
		res = append(res, tag{K: "skey", V: string(skey)})
	}
	return res
}

func sortSlice(res []tag) {
	sort.Slice(res, func(i, j int) bool {
		if res[i].K < res[j].K {
			return true
		} else if res[i].K > res[j].K {
			return false
		}
		return res[i].V < res[j].V
	})
}

func TestPHPRoundtrip(t *testing.T) {
	if os.Getenv("STATSHOUSE_RUN_PHP_INTEGRATION_TEST") != "1" {
		t.Skip("PHP integration test requires configured PHP environment and client source code")
	}

	rapid.Check(t, rapid.Run[*phpMachine]())
}

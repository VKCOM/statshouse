// Copyright 2026 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package grafanaexport

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/format"
)

func testMeta(metric string) *Meta {
	switch metric {
	case "cnt":
		return &Meta{Kind: format.MetricKindCounter}
	case "val":
		return &Meta{Kind: format.MetricKindValue, MetricType: format.MetricMillisecond, TagNames: []string{"", "env"}}
	}
	return nil
}

func exportParams(params ...string) Result {
	in := Input{Name: "test", Datasource: Datasource{Type: DefaultDatasourceType, UID: DefaultDatasourceUID}}
	for i := 0; i+1 < len(params); i += 2 {
		in.SearchParams = append(in.SearchParams, [2]string{params[i], params[i+1]})
	}
	return Export(in, testMeta)
}

func panelsOf(t *testing.T, r Result) []Panel {
	var res []Panel
	for _, p := range r.Dashboard.Panels {
		if pn, ok := p.(Panel); ok {
			res = append(res, pn)
		}
	}
	return res
}

func exprOf(t *testing.T, params ...string) string {
	r := exportParams(params...)
	ps := panelsOf(t, r)
	require.Len(t, ps, 1, "warnings: %v", r.Warnings)
	require.NotEmpty(t, ps[0].Targets)
	return ps[0].Targets[0].Expr
}

func TestEmptyDashboard(t *testing.T) {
	r := Export(Input{Name: "x"}, nil)
	require.Len(t, r.Warnings, 1)
	require.Empty(t, r.Dashboard.Panels)
	_, err := json.Marshal(r)
	require.NoError(t, err)
}

func TestTranslateRows(t *testing.T) {
	tests := []struct {
		name   string
		params []string
		want   string
	}{
		{"count_norm summary", []string{"s", "val", "qw", "count_norm"}, `sum (rate(val_count[$__rate_interval]))`},
		{"count counter", []string{"s", "cnt", "qw", "count"}, `sum (increase(cnt[$__rate_interval]))`},
		{"avg", []string{"s", "val", "qw", "avg"}, `(sum (rate(val_sum[$__rate_interval]))) / (sum (rate(val_count[$__rate_interval])))`},
		{"p99", []string{"s", "val", "qw", "p99"}, `histogram_quantile(0.99, sum by (le, vmrange) (rate(val_bucket[$__rate_interval])))`},
		{"cumul_count", []string{"s", "cnt", "qw", "cu_count"}, `running_sum(sum (increase(cnt[1i])))`},
		{
			"group by custom tag, filter, top",
			[]string{"s", "val", "qw", "count_norm", "qb", "1", "qf", "0-a", "n", "3"},
			`topk_avg(3, sum by (env) (rate(val_count{key0="a"}[$__rate_interval])))`,
		},
		{
			"bottom, not in, several values",
			[]string{"s", "val", "qw", "count_norm", "qb", "1", "qf", "0~a", "qf", "0~b", "n", "-2"},
			`bottomk_avg(2, sum by (env) (rate(val_count{key0!~"a|b"}[$__rate_interval])))`,
		},
		{"no top without group", []string{"s", "val", "qw", "count_norm", "n", "3"}, `sum (rate(val_count[$__rate_interval]))`},
		{"unknown metric is a summary", []string{"s", "a:b", "qw", "sum"}, `sum (increase(a:b_sum[$__rate_interval]))`},
		{"non-identifier metric uses __name__", []string{"s", "a.b", "qw", "sum"}, `sum (increase({__name__="a.b_sum"}[$__rate_interval]))`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, exprOf(t, tc.params...))
		})
	}
}

func TestCounterRejectsDistributionFns(t *testing.T) {
	r := exportParams("s", "cnt", "qw", "p99")
	require.Empty(t, panelsOf(t, r))
	require.NotEmpty(t, r.Warnings)
}

func TestSkippedPlots(t *testing.T) {
	r := exportParams(
		"s", "val", "qw", "count_norm",
		"t1.s", "val", "t1.qt", "1",
		"t2.q", "up",
	)
	require.Len(t, panelsOf(t, r), 1)
	require.Len(t, r.Warnings, 2)
}

func TestVariables(t *testing.T) {
	r := exportParams(
		"s", "val", "qw", "count_norm",
		"v0.n", "env", "v0.l", "0.1", "v.env", "prod", "v.env.g", "1",
	)
	require.Len(t, r.Dashboard.Templating.List, 1)
	v := r.Dashboard.Templating.List[0]
	require.Equal(t, "env", v.Name)
	require.Equal(t, "label_values(val_count, env)", v.Definition)
	require.Equal(t, []string{"prod"}, v.Current.Value)
	require.Equal(t, ".*", v.AllValue)
	ps := panelsOf(t, r)
	require.Len(t, ps, 1)
	require.Equal(t, `topk_avg(5, sum by (env) (rate(val_count{env=~"${env}"}[$__rate_interval])))`, ps[0].Targets[0].Expr)
}

func TestGrafanaTimeRange(t *testing.T) {
	tests := []struct{ from, to, wantFrom, wantTo string }{
		{"-7200", "0", "now-2h", "now"},
		{"-86400", "ed", "now/d+1d-1d", "now/d"},
		{"-604800", "ew", "now/w+1w-7d", "now/w"},
		{"-3600", "-1800", "now-90m", "now-30m"},
		{"-3600", "1700000000", "1699996400000", "1700000000000"},
		{"1699990000", "1700000000", "1699990000000", "1700000000000"},
		{"", "", "now-2h", "now"},
	}
	for _, tc := range tests {
		from, to := grafanaTimeRange(tc.from, tc.to)
		require.Equal(t, tc.wantFrom, from, "%+v", tc)
		require.Equal(t, tc.wantTo, to, "%+v", tc)
	}
}

func TestPlace(t *testing.T) {
	cells := []cell{
		{layout: layout{0, 0, 12, 8}},
		{layout: layout{0, 0, 12, 8}},
		{layout: layout{12, 0, 40, 4}},
	}
	placed := place(cells)
	for i, a := range placed {
		require.LessOrEqual(t, a.x+a.w, gridColumns)
		for _, b := range placed[:i] {
			require.False(t, a.x < b.x+b.w && b.x < a.x+a.w && a.y < b.y+b.h && b.y < a.y+a.h, "%+v overlaps %+v", a, b)
		}
	}
}

func TestGroupsAndDeterminism(t *testing.T) {
	params := []string{
		"s", "val", "qw", "count_norm", "dg", "0",
		"t1.s", "cnt", "t1.qw", "count", "t1.dg", "1",
		"g1.t", "Second", "g1.v", "0",
		"og", "0.1",
	}
	r := exportParams(params...)
	require.Len(t, r.Dashboard.Panels, 3)
	row, ok := r.Dashboard.Panels[2].(Row)
	require.True(t, ok)
	require.True(t, row.Collapsed)
	require.Len(t, row.Panels, 1)

	a, err := json.Marshal(r)
	require.NoError(t, err)
	b, err := json.Marshal(exportParams(params...))
	require.NoError(t, err)
	require.Equal(t, string(a), string(b))
}

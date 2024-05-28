// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

const (
	RoutePrefix                 = "/api/"
	EndpointMetric              = "metric"
	EndpointMetricList          = "metrics-list"
	EndpointMetricTagValues     = "metric-tag-values"
	EndpointQuery               = "query"
	EndpointTable               = "table"
	EndpointPoint               = "point"
	EndpointRender              = "render"
	EndpointResetFlood          = "reset-flood"
	EndpointLegacyRedirect      = "legacy-redirect"
	EndpointDashboard           = "dashboard"
	EndpointDashboardList       = "dashboards-list"
	EndpointGroup               = "group"
	EndpointNamespace           = "namespace"
	EndpointNamespaceList       = "namespace-list"
	EndpointGroupList           = "group-list"
	EndpointPrometheus          = "prometheus"
	EndpointPrometheusGenerated = "prometheus-generated"
	EndpointKnownTags           = "known-tags"
	EndpointStatistics          = "stat"
	endpointChunk               = "chunk"
	EndpointHistory             = "history"

	userTokenName = "user"
)

type endpointStat struct {
	timestamp  time.Time
	endpoint   string
	protocol   int
	method     string
	dataFormat string
	lane       string
	laneMutex  sync.Mutex // we access lane from main and badges query
	metric     string
	tokenName  string
	user       string
	priority   int
	timings    ServerTimingHeader
}

func newEndpointStatHTTP(endpoint, method string, metricID int32, dataFormat string, priorityStr string) *endpointStat {
	priority, _ := strconv.Atoi(priorityStr)
	return &endpointStat{
		timestamp:  time.Now(),
		endpoint:   endpoint,
		protocol:   format.TagValueIDHTTP,
		method:     method,
		metric:     strconv.Itoa(int(metricID)), // metric ID key is considered "raw"
		dataFormat: dataFormat,
		priority:   priority,
		timings:    ServerTimingHeader{Timings: make(map[string][]time.Duration), started: time.Now()},
	}
}

func newEndpointStatRPC(endpoint, method string) *endpointStat {
	return &endpointStat{
		timestamp:  time.Now(),
		endpoint:   endpoint,
		protocol:   format.TagValueIDRPC,
		method:     method,
		dataFormat: "TL",
	}
}

func (es *endpointStat) reportServiceTime(code int, err error) {
	if len(es.metric) != 0 {
		statshouse.Metric(
			format.BuiltinMetricNameAPIMetricUsage,
			statshouse.Tags{
				1: strconv.FormatInt(int64(es.protocol), 10),
				2: es.user,
				3: es.metric,
			},
		).Count(1)
	}
	if es.protocol == format.TagValueIDRPC && code == 0 {
		switch e := err.(type) {
		case rpc.Error:
			code = int(e.Code)
		case nil:
			// code = 0
		default:
			code = -1
		}
	}
	es.report(code, format.BuiltinMetricNameAPIServiceTime)
}

func (es *endpointStat) setAccessInfo(ai accessInfo) {
	es.user = ai.user
	es.tokenName = getStatTokenName(ai.user)
}

func (es *endpointStat) setMetricMeta(metricMeta *format.MetricMetaValue) {
	if metricMeta != nil {
		es.metric = strconv.Itoa(int(metricMeta.MetricID))
	}
}

func (es *endpointStat) reportResponseTime(code int) {
	es.report(code, format.BuiltinMetricNameAPIResponseTime)
}

func (es *endpointStat) report(code int, metric string) {
	v := time.Since(es.timestamp).Seconds()
	t := statshouse.Tags{
		1:  es.endpoint,
		2:  strconv.Itoa(es.protocol),
		3:  es.method,
		4:  es.dataFormat,
		5:  es.lane,
		6:  srvfunc.HostnameForStatshouse(),
		7:  es.tokenName,
		8:  strconv.Itoa(code),
		10: strconv.Itoa(es.priority),
	}
	statshouse.Metric(metric, t).Value(v)
}

func getStatTokenName(user string) string {
	if strings.Contains(user, "@") {
		return userTokenName
	}
	return user
}

func CurrentChunksCount(brs *BigResponseStorage) func(*statshouse.Client) {
	return func(c *statshouse.Client) {
		c.Metric(
			format.BuiltinMetricNameAPIBRS,
			statshouse.Tags{
				1: srvfunc.HostnameForStatshouse(),
			},
		).Value(float64(brs.Count()))
	}
}

func ChSelectMetricDuration(duration time.Duration, metricID int32, user, table, kind string, isFast, isLight bool, err error) {
	ok := "ok"
	if err != nil {
		ok = "error"
	}
	statshouse.Metric(
		format.BuiltinMetricNameAPISelectDuration,
		statshouse.Tags{
			1: modeStr(isFast, isLight),
			2: strconv.Itoa(int(metricID)),
			3: table,
			4: kind,
			5: ok,
			6: getStatTokenName(user),
			7: user,
		},
	).Value(duration.Seconds())
}

func ChSelectProfile(isFast, isLight bool, info proto.Profile, err error) {
	chSelectPushMetric(format.BuiltinMetricNameAPISelectBytes, isFast, isLight, float64(info.Bytes), err)
	chSelectPushMetric(format.BuiltinMetricNameAPISelectRows, isFast, isLight, float64(info.Rows), err)
}

func modeStr(isFast, isLight bool) string {
	mode := "slow"
	if isFast {
		mode = "fast"
	}
	if isLight {
		mode += "light"
	} else {
		mode += "heavy"
	}
	return mode
}

func chSelectPushMetric(metric string, isFast, isLight bool, data float64, err error) {
	m := statshouse.Metric(
		metric,
		statshouse.Tags{
			1: modeStr(isFast, isLight),
		},
	)
	m.Value(data)
	if err != nil {
		m.StringTop(err.Error())
	}
}

func ChCacheRate(cachedRows, chRows int, metricID int32, table, kind string) {
	statshouse.Metric(
		format.BuiltinMetricNameAPICacheHit,
		statshouse.Tags{
			1: "cache",
			2: strconv.Itoa(int(metricID)),
			3: table,
			4: kind,
		},
	).Value(float64(cachedRows))

	statshouse.Metric(
		format.BuiltinMetricNameAPICacheHit,
		statshouse.Tags{
			1: "clickhouse",
			2: strconv.Itoa(int(metricID)),
			3: table,
			4: kind,
		},
	).Value(float64(chRows))
}

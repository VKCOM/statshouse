// Copyright 2025 V Kontakte LLC
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

	"github.com/VKCOM/statshouse/internal/chutil"

	"github.com/VKCOM/statshouse-go"

	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
	"github.com/VKCOM/statshouse/internal/vkgo/srvfunc"
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
	EndpointHealthcheck         = "healthcheck"

	userTokenName = "user"
)

type endpointStat struct {
	timestamp  time.Time
	endpoint   string
	protocol   int
	method     string
	dataFormat string
	// TODO: currently lane comes from the first query that sets it, it's non deterministic
	lane      string
	laneMutex sync.Mutex // we access lane from main and badges query
	metric    string
	tokenName string
	user      string
	priority  string
	timings   ServerTimingHeader
}

func (es *endpointStat) reportServiceTime(code int, err error) {
	if len(es.metric) != 0 {
		statshouse.Count(
			format.BuiltinMetricMetaAPIMetricUsage.Name,
			statshouse.Tags{
				1: strconv.FormatInt(int64(es.protocol), 10),
				2: es.user,
				3: es.metric,
			}, 1)
	}
	if es.protocol == format.TagValueIDRPC && code == 0 {
		switch e := err.(type) {
		case *rpc.Error:
			code = int(e.Code)
		case nil:
			// code = 0
		default:
			code = -1
		}
	}
	es.report(code, format.BuiltinMetricMetaAPIServiceTime.Name)
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

func (es *endpointStat) report(code int, metric string) {
	v := time.Since(es.timestamp).Seconds()
	es.laneMutex.Lock()
	defer es.laneMutex.Unlock()
	t := statshouse.Tags{
		1:  es.endpoint,
		2:  strconv.Itoa(es.protocol),
		3:  es.method,
		4:  es.dataFormat,
		5:  es.lane,
		6:  srvfunc.HostnameForStatshouse(),
		7:  es.tokenName,
		8:  strconv.Itoa(code),
		9:  es.metric,
		10: es.priority,
	}
	statshouse.Value(metric, t, v)
}

func (es *endpointStat) reportQueryKind(isFast, isLight, isHardware bool) {
	es.laneMutex.Lock()
	defer es.laneMutex.Unlock()
	if len(es.lane) == 0 {
		es.lane = strconv.Itoa(chutil.QueryKind(isFast, isLight, isHardware))
	}
}

func (es *endpointStat) reportTiming(name string, dur time.Duration) {
	es.timings.Report(name, dur)
}

func getStatTokenName(user string) string {
	if strings.Contains(user, "@") {
		return userTokenName
	}
	return user
}

func CurrentChunksCount(brs *BigResponseStorage) func(*statshouse.Client) {
	return func(c *statshouse.Client) {
		c.Value(
			format.BuiltinMetricMetaAPIBRS.Name,
			statshouse.Tags{
				1: srvfunc.HostnameForStatshouse(),
			},
			float64(brs.Count()))
	}
}

func ChSelectMetricDuration(duration time.Duration, metric *format.MetricMetaValue, user, table, kind string, isFast, isLight, isHardware bool, err error) {
	ok := "ok"
	if err != nil {
		ok = "error"
	}
	var metricID int32
	if metric != nil {
		metricID = metric.MetricID
	}
	statshouse.Value(
		format.BuiltinMetricMetaAPISelectDuration.Name,
		statshouse.Tags{
			1: modeStr(isFast, isLight, isHardware),
			2: strconv.Itoa(int(metricID)),
			3: table,
			4: kind,
			5: ok,
			6: getStatTokenName(user),
			7: user,
			8: strconv.Itoa(int(uint32(metricID) % 16)), // experimental to see load distribution if we shard data by metricID
		},
		duration.Seconds())
}

func ChRequestsMetric(shard int, aggHost string, table string, ok bool) {
	status := "1"
	if !ok {
		status = "2"
	}
	statshouse.Count(
		format.BuiltinMetricMetaApiChRequests.Name,
		statshouse.Tags{
			1: srvfunc.HostnameForStatshouse(),
			2: strconv.Itoa(shard),
			3: aggHost,
			4: table,
			5: status,
		}, 1)
}

func ChSelectProfile(isFast, isLight, isHardware bool, info proto.Profile, err error) {
	chSelectPushMetric(format.BuiltinMetricMetaAPISelectBytes.Name, isFast, isLight, isHardware, float64(info.Bytes), err)
	chSelectPushMetric(format.BuiltinMetricMetaAPISelectRows.Name, isFast, isLight, isHardware, float64(info.Rows), err)
}

func modeStr(isFast, isLight, isHardware bool) string {
	mode := "slow"
	if isFast {
		mode = "fast"
	}
	if isHardware {
		return mode + "_hardware"
	}
	if isLight {
		mode += "light"
	} else {
		mode += "heavy"
	}
	return mode
}

func chSelectPushMetric(metric string, isFast, isLight, isHardware bool, data float64, err error) {
	m := statshouse.GetMetricRef(
		metric,
		statshouse.Tags{
			1: modeStr(isFast, isLight, isHardware),
		},
	)
	m.Value(data)
	if err != nil {
		m.StringTop(err.Error())
	}
}

func ChCacheRate(cachedRows, chRows int, metricID int32, table, kind string) {
	statshouse.Value(
		format.BuiltinMetricMetaAPICacheHit.Name,
		statshouse.Tags{
			1: "cache",
			2: strconv.Itoa(int(metricID)),
			3: table,
			4: kind,
		},
		float64(cachedRows))

	statshouse.Value(
		format.BuiltinMetricMetaAPICacheHit.Name,
		statshouse.Tags{
			1: "clickhouse",
			2: strconv.Itoa(int(metricID)),
			3: table,
			4: kind,
		},
		float64(chRows))
}

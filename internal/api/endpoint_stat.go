// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/vkcom/statshouse-go"

	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

const (
	RoutePrefix             = "/api/"
	EndpointMetric          = "metric"
	EndpointMetricList      = "metrics-list"
	EndpointMetricTagValues = "metric-tag-values"
	EndpointQuery           = "query"
	EndpointTable           = "table"
	EndpointPoint           = "point"
	EndpointRender          = "render"
	EndpointResetFlood      = "reset-flood"
	EndpointLegacyRedirect  = "legacy-redirect"
	EndpointDashboard       = "dashboard"
	EndpointDashboardList   = "dashboards-list"
	EndpointGroup           = "group"
	EndpointNamespace       = "namespace"
	EndpointNamespaceList   = "namespace-list"
	EndpointGroupList       = "group-list"
	EndpointPrometheus      = "prometheus"
	EndpointStatistics      = "stat"
	endpointChunk           = "chunk"

	userTokenName = "user"
)

type endpointStat struct {
	endpoint   string
	method     string
	metric     string
	startTime  time.Time
	tokenName  string
	user       string
	dataFormat string
	lane       string
}

func (es *endpointStat) serviceTime(code int) {
	LogMetric(format.TagValueIDHTTP, es.user, es.metric)
	es.logEvent(format.BuiltinMetricNameAPIServiceTime, code)
	es.logDeprecatedEvent(format.BuiltinMetricNameAPIEndpointServiceTime, code)
}

func (es *endpointStat) responseTime(code int) {
	es.logEvent(format.BuiltinMetricNameAPIResponseTime, code)
	es.logDeprecatedEvent(format.BuiltinMetricNameAPIEndpointResponseTime, code)
}

func (es *endpointStat) logEvent(statName string, code int) {
	var (
		v = time.Since(es.startTime).Seconds()
		t = statshouse.Tags{
			1: es.endpoint,
			2: strconv.Itoa(int(format.TagValueIDHTTP)),
			3: es.method,
			4: es.dataFormat,
			5: es.lane,
			6: srvfunc.HostnameForStatshouse(),
			7: es.tokenName,
			8: strconv.Itoa(code),
			9: es.metric,
		}
	)
	statshouse.Metric(statName, t).Value(v)
}

func (es *endpointStat) logDeprecatedEvent(statName string, code int) {
	v := time.Since(es.startTime).Seconds()
	statshouse.Metric(
		statName,
		statshouse.Tags{
			1: es.endpoint,
			2: es.metric,
			3: strconv.Itoa(code),
			4: es.tokenName,
			5: es.dataFormat,
			6: es.method,
		},
	).Value(v)
}

func (es *endpointStat) setTokenName(user string) {
	es.tokenName = getStatTokenName(user)
	es.user = user
}

func getStatTokenName(user string) string {
	if strings.Contains(user, "@") {
		return userTokenName
	}

	return user
}

func newEndpointStat(endpoint, method string, metricID int32, dataFormat string) *endpointStat {
	return &endpointStat{
		endpoint:   endpoint,
		metric:     strconv.Itoa(int(metricID)), // metric ID key is considered "raw"
		startTime:  time.Now(),
		dataFormat: dataFormat,
		method:     method,
	}
}

type rpcMethodStat struct {
	startTime time.Time
	endpoint  string
	method    string
	lane      string
}

func newRPCMethodStat(endpoint, method string) *rpcMethodStat {
	return &rpcMethodStat{startTime: time.Now(), endpoint: endpoint, method: method}
}

func (ms *rpcMethodStat) serviceTime(ai accessInfo, meta *format.MetricMetaValue, err error) {
	var errorCode string
	switch e := err.(type) {
	case rpc.Error:
		errorCode = strconv.FormatInt(int64(e.Code), 10)
	case nil:
		errorCode = "0"
	default:
		errorCode = "-1"
	}
	var (
		v = time.Since(ms.startTime).Seconds()
		t = statshouse.Tags{
			1: ms.endpoint,
			2: strconv.Itoa(int(format.TagValueIDRPC)),
			3: ms.method,
			4: "TL",
			5: ms.lane,
			6: srvfunc.HostnameForStatshouse(),
			7: getStatTokenName(ai.user),
			8: errorCode,
		}
	)
	if meta != nil {
		t[9] = strconv.Itoa(int(meta.MetricID))
	}
	statshouse.Metric(format.BuiltinMetricNameAPIServiceTime, t).Value(v)
}

func (ms *rpcMethodStat) serviceTimeDeprecated(ai accessInfo, err error) {
	var errorCode string
	switch e := err.(type) {
	case rpc.Error:
		errorCode = strconv.FormatInt(int64(e.Code), 10)
	case nil:
		errorCode = "0"
	default:
		errorCode = "-1"
	}
	v := time.Since(ms.startTime).Seconds()
	statshouse.Metric(
		format.BuiltinMetricNameAPIRPCServiceTime,
		statshouse.Tags{
			1: ms.method,
			2: errorCode,
			3: getStatTokenName(ai.user),
			4: srvfunc.HostnameForStatshouse(),
		},
	).Value(v)
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

func ChSelectMetricDuration(duration time.Duration, metricID int32, token, table, kind string, isFast, isLight bool, err error) {
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
			6: getStatTokenName(token),
			7: token,
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
		"ch_video_select_test",
		statshouse.Tags{
			1: "cache",
			2: strconv.Itoa(int(metricID)),
			3: table,
			4: kind,
		},
	).Value(float64(cachedRows))

	statshouse.Metric(
		"ch_video_select_test",
		statshouse.Tags{
			1: "clickhouse",
			2: strconv.Itoa(int(metricID)),
			3: table,
			4: kind,
		},
	).Value(float64(chRows))
}

func LogMetric(type_ int64, user string, metricID string) {
	statshouse.Metric(
		format.BuiltinMetricNameAPIMetricUsage,
		statshouse.Tags{
			1: strconv.FormatInt(type_, 10),
			2: user,
			3: metricID,
		},
	).Count(1)
}

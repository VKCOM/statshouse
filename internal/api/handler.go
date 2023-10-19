// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"math"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	ttemplate "text/template"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/vkcom/statshouse-go"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/mailru/easyjson"
	_ "github.com/mailru/easyjson/gen" // https://github.com/mailru/easyjson/issues/293

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/promql"
	"github.com/vkcom/statshouse/internal/util"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
	"github.com/vkcom/statshouse/internal/vkgo/vkuth"

	"pgregory.net/rand"
)

//go:generate easyjson -no_std_marshalers httputil.go handler.go
// after generating, you should manually change
//	out.Float64(float64(v17))
//	...
//	out.Float64(float64(v36))
// to
//	if math.IsNaN(float64(v17)) {
//		out.RawString("null")
//	} else {
//		out.Float64(float64(v17))
//	}
//	...
//	if math.IsNaN(float64(v36)) {
//		out.RawString("null")
//	} else {
//		out.Float64(float64(v36))
//	}

// also remove code which saves and loads UpdateTime

const (
	ParamVersion    = "v"
	ParamNumResults = "n"
	ParamMetric     = "s"
	ParamID         = "id"

	ParamTagID        = "k"
	ParamFromTime     = "f"
	ParamToTime       = "t"
	ParamWidth        = "w"
	ParamWidthAgg     = "g" // supported only for better compatibility between UI and API URLs
	ParamTimeShift    = "ts"
	ParamQueryWhat    = "qw"
	ParamQueryBy      = "qb"
	ParamQueryFilter  = "qf"
	ParamQueryVerbose = "qv"
	ParamAvoidCache   = "ac"
	paramRenderWidth  = "rw"
	paramDataFormat   = "df"
	paramTabNumber    = "tn"
	paramMaxHost      = "mh"
	paramFromRow      = "fr"
	paramToRow        = "tr"
	paramPromQuery    = "q"
	paramFromEnd      = "fe"
	paramExcessPoints = "ep"
	paramLegacyEngine = "legacy"
	paramQueryType    = "qt"
	paramDashboardID  = "id"

	Version1       = "1"
	Version2       = "2"
	dataFormatPNG  = "png"
	dataFormatSVG  = "svg"
	dataFormatText = "text"
	dataFormatCSV  = "csv"

	defSeries     = 10
	maxSeries     = 10_000
	defTagValues  = 100
	maxTagValues  = 100_000
	maxSeriesRows = 10_000_000
	maxTableRows  = 100_000

	maxTableRowsPage = 10_000
	maxTimeShifts    = 10
	maxFunctions     = 10

	cacheInvalidateCheckInterval = 1 * time.Second
	cacheInvalidateCheckTimeout  = 5 * time.Second
	cacheInvalidateMaxRows       = 100_000
	cacheDefaultDropEvery        = 90 * time.Second

	queryClientCache               = 1 * time.Second
	queryClientCacheStale          = 9 * time.Second // ~ v2 lag
	queryClientCacheImmutable      = 7 * 24 * time.Hour
	queryClientCacheStaleImmutable = 0

	QuerySelectTimeoutDefault = 55 * time.Second // TODO: querySelectTimeout must be longer than the longest normal query. And must be consistent with NGINX's or another reverse proxy's timeout
	fastQueryTimeInterval     = (86400 + 3600) * 2

	maxEntityHTTPBodySize     = 256 << 10
	maxPromConfigHTTPBodySize = 500 * 1024

	defaultCacheTTL = 1 * time.Second

	maxConcurrentPlots = 8
	plotRenderTimeout  = 5 * time.Second

	descriptionFieldName = "__description"
	journalUpdateTimeout = 2 * time.Second
)

type (
	JSSettings struct {
		VkuthAppName             string              `json:"vkuth_app_name"`
		DefaultMetric            string              `json:"default_metric"`
		DefaultMetricFilterIn    map[string][]string `json:"default_metric_filter_in"`
		DefaultMetricFilterNotIn map[string][]string `json:"default_metric_filter_not_in"`
		DefaultMetricWhat        []string            `json:"default_metric_what"`
		DefaultMetricGroupBy     []string            `json:"default_metric_group_by"`
		EventPreset              []string            `json:"event_preset"`
		DefaultNumSeries         int                 `json:"default_num_series"`
		DisableV1                bool                `json:"disabled_v1"`
	}

	Handler struct {
		verbose               bool
		protectedPrefixes     []string
		showInvisible         bool
		utcOffset             int64
		staticDir             http.FileSystem
		indexTemplate         *template.Template
		indexSettings         string
		ch                    map[string]*util.ClickHouse
		metricsStorage        *metajournal.MetricsStorage
		tagValueCache         *pcache.Cache
		tagValueIDCache       *pcache.Cache
		cache                 *tsCacheGroup
		pointsCache           *pointsCache
		pointRowsPool         sync.Pool
		pointFloatsPool       sync.Pool
		cacheInvalidateTicker *time.Ticker
		cacheInvalidateStop   chan chan struct{}
		metadataLoader        *metajournal.MetricMetaLoader
		jwtHelper             *vkuth.JWTHelper
		localMode             bool
		insecureMode          bool
		plotRenderSem         *semaphore.Weighted
		plotTemplate          *ttemplate.Template
		location              *time.Location
		readOnly              bool
		rUsage                syscall.Rusage // accessed without lock by first shard addBuiltIns
		rmID                  int
		promEngine            promql.Engine
		promEngineOn          bool
		accessManager         *accessManager
		querySelectTimeout    time.Duration
	}

	//easyjson:json
	GetMetricsListResp struct {
		Metrics []metricShortInfo `json:"metrics"`
	}

	//easyjson:json
	GetDashboardListResp struct {
		Dashboards []dashboardShortInfo `json:"dashboards"`
	}

	//easyjson:json
	GetGroupListResp struct {
		Groups []groupShortInfo `json:"groups"`
	}

	//easyjson:json
	GetNamespaceListResp struct {
		Namespaces []namespaceShortInfo `json:"namespaces"`
	}

	metricShortInfo struct {
		Name string `json:"name"`
	}

	dashboardShortInfo struct {
		Id          int32  `json:"id"`
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	groupShortInfo struct {
		Id     int32   `json:"id"`
		Name   string  `json:"name"`
		Weight float64 `json:"weight"`
	}

	namespaceShortInfo struct {
		Id     int32   `json:"id"`
		Name   string  `json:"name"`
		Weight float64 `json:"weight"`
	}

	//easyjson:json
	MetricInfo struct {
		Metric format.MetricMetaValue `json:"metric"`
	}

	//easyjson:json
	DashboardInfo struct {
		Dashboard DashboardMetaInfo `json:"dashboard"`
		Delete    bool              `json:"delete_mark"`
	}

	//easyjson:json
	MetricsGroupInfo struct {
		Group   format.MetricsGroup `json:"group"`
		Metrics []string            `json:"metrics"`
	}

	//easyjson:json
	NamespaceInfo struct {
		Namespace format.NamespaceMeta `json:"namespace"`
	}

	DashboardMetaInfo struct {
		DashboardID int32                  `json:"dashboard_id"`
		Name        string                 `json:"name"`
		Version     int64                  `json:"version,omitempty"`
		UpdateTime  uint32                 `json:"update_time"`
		DeletedTime uint32                 `json:"deleted_time"`
		Description string                 `json:"description"`
		JSONData    map[string]interface{} `json:"data"`
	}

	//easyjson:json
	DashboardData struct {
		Plots      []DashboardPlot     `json:"plots"`
		Vars       []DashboardVar      `json:"variables"`
		TabNum     int                 `json:"tabNum"`
		TimeRange  DashboardTimeRange  `json:"timeRange"`
		TimeShifts DashboardTimeShifts `json:"timeShifts"`
	}

	DashboardPlot struct {
		UseV2       bool                `json:"useV2"`
		NumSeries   int                 `json:"numSeries"`
		MetricName  string              `json:"metricName"`
		Width       int                 `json:"customAgg"`
		PromQL      string              `json:"promQL"`
		What        []string            `json:"what"`
		GroupBy     []string            `json:"groupBy"`
		FilterIn    map[string][]string `json:"filterIn"`
		FilterNotIn map[string][]string `json:"filterNotIn"`
		MaxHost     bool                `json:"maxHost"`
		Type        int                 `json:"type"`
	}

	DashboardVar struct {
		Name string           `json:"name"`
		Args DashboardVarArgs `json:"args"`
		Vals []string         `json:"values"`
		Link [][]int          `json:"link"`
	}

	DashboardVarArgs struct {
		Group  bool `json:"groupBy"`
		Negate bool `json:"negative"`
	}

	DashboardTimeRange struct {
		From int64
		To   string
	}

	DashboardTimeShifts []string

	getMetricTagValuesReq struct {
		ai                  accessInfo
		version             string
		numResults          string
		metricWithNamespace string
		tagID               string
		from                string
		to                  string
		what                string
		filter              []string
	}

	//easyjson:json
	GetMetricTagValuesResp struct {
		TagValues     []MetricTagValueInfo `json:"tag_values"`
		TagValuesMore bool                 `json:"tag_values_more"`
	}

	MetricTagValueInfo struct {
		Value string  `json:"value"`
		Count float64 `json:"count"`
	}

	tableRequest struct {
		version             string
		metricWithNamespace string
		from                string
		to                  string
		width               string
		widthAgg            string
		what                []string
		by                  []string
		filterIn            map[string][]string
		filterNotIn         map[string][]string
		maxHost             bool
		avoidCache          bool
		fromEnd             bool
		fromRow             RowMarker
		toRow               RowMarker
		limit               int
	}

	seriesRequest struct {
		version             string
		numResults          int
		metricWithNamespace string
		from                time.Time
		to                  time.Time
		width               int
		widthKind           int
		promQL              string
		shifts              []time.Duration
		what                []string
		by                  []string
		filterIn            map[string][]string
		filterNotIn         map[string][]string
		maxHost             bool
		avoidCache          bool
		verbose             bool
		expandToLODBoundary bool
		format              string
	}

	seriesRequestOptions struct {
		debugQueries       bool
		testPromql         bool
		metricNameCallback func(string)
		rand               *rand.Rand
		stat               *endpointStat
		timeNow            time.Time
		vars               map[string]promql.Variable
	}

	//easyjson:json
	SeriesResponse struct {
		Series                querySeries             `json:"series"`
		SamplingFactorSrc     float64                 `json:"sampling_factor_src"` // average
		SamplingFactorAgg     float64                 `json:"sampling_factor_agg"` // average
		ReceiveErrors         float64                 `json:"receive_errors"`      // count/sec
		ReceiveWarnings       float64                 `json:"receive_warnings"`    // count/sec
		MappingErrors         float64                 `json:"mapping_errors"`      // count/sec
		PromQL                string                  `json:"promql"`              // equivalent PromQL query
		DebugQueries          []string                `json:"__debug_queries"`     // private, unstable: SQL queries executed
		DebugPromQLTestFailed bool                    `json:"promqltestfailed"`
		ExcessPointLeft       bool                    `json:"excess_point_left"`
		ExcessPointRight      bool                    `json:"excess_point_right"`
		MetricMeta            *format.MetricMetaValue `json:"metric"`
		immutable             bool
		queries               map[lodInfo]int // not nil if testPromql option set (see getQueryReqOptions)
	}

	//easyjson:json
	GetPointResp struct {
		PointMeta    []QueryPointsMeta `json:"point_meta"`      // M
		PointData    []float64         `json:"point_data"`      // M
		DebugQueries []string          `json:"__debug_queries"` // private, unstable: SQL queries executed
	}

	//easyjson:json
	GetTableResp struct {
		Rows         []queryTableRow `json:"rows"`
		What         []queryFn       `json:"what"`
		FromRow      string          `json:"from_row"`
		ToRow        string          `json:"to_row"`
		More         bool            `json:"more"`
		DebugQueries []string        `json:"__debug_queries"` // private, unstable: SQL queries executed, can be null
	}

	renderRequest struct {
		ai            accessInfo
		seriesRequest []seriesRequest
		vars          map[string]promql.Variable
		renderWidth   string
		renderFormat  string
	}

	renderResponse struct {
		format string
		data   []byte
	}

	querySeries struct {
		Time       []int64             `json:"time"`        // N
		SeriesMeta []QuerySeriesMetaV2 `json:"series_meta"` // M
		SeriesData []*[]float64        `json:"series_data"` // MxN
	}

	//easyjson:json
	queryTableRow struct {
		Time    int64                    `json:"time"`
		Data    []float64                `json:"data"`
		Tags    map[string]SeriesMetaTag `json:"tags"`
		row     tsSelectRow
		rowRepr RowMarker
	}

	QuerySeriesMeta struct {
		TimeShift int64             `json:"time_shift"`
		Tags      map[string]string `json:"tags"`
		MaxHosts  []string          `json:"max_hosts"` // max_host for now
		What      queryFn           `json:"what"`
	}

	QuerySeriesMetaV2 struct {
		TimeShift  int64                    `json:"time_shift"`
		Tags       map[string]SeriesMetaTag `json:"tags"`
		MaxHosts   []string                 `json:"max_hosts"` // max_host for now
		Name       string                   `json:"name"`
		Color      string                   `json:"color"`
		What       queryFn                  `json:"what"`
		Total      int                      `json:"total"`
		MetricType string                   `json:"metric_type"`
	}

	QueryPointsMeta struct {
		TimeShift int64                    `json:"time_shift"`
		Tags      map[string]SeriesMetaTag `json:"tags"`
		MaxHost   string                   `json:"max_host"` // max_host for now
		Name      string                   `json:"name"`
		What      queryFn                  `json:"what"`
		FromSec   int64                    `json:"from_sec"` // rounded from sec
		ToSec     int64                    `json:"to_sec"`   // rounded to sec
	}

	SeriesMetaTag struct {
		Value   string `json:"value"`
		Comment string `json:"comment,omitempty"`
		Raw     bool   `json:"raw,omitempty"`
		RawKind string `json:"raw_kind,omitempty"`
	}

	RawTag struct {
		Index int   `json:"index"`
		Value int32 `json:"value"`
	}

	RowMarker struct {
		Time int64    `json:"time"`
		Tags []RawTag `json:"tags"`
		SKey string   `json:"skey"`
	}

	cacheInvalidateLogRow struct {
		T  int64 `ch:"time"` // time of insert
		At int64 `ch:"key1"` // seconds inserted (changed), which should be invalidated
	}
)

var errTooManyRows = fmt.Errorf("can't fetch more than %v rows", maxSeriesRows)

func NewHandler(verbose bool, staticDir fs.FS, jsSettings JSSettings, protectedPrefixes []string, showInvisible bool, utcOffsetSec int64, approxCacheMaxSize int, chV1 *util.ClickHouse, chV2 *util.ClickHouse, metadataClient *tlmetadata.Client, diskCache *pcache.DiskCache, jwtHelper *vkuth.JWTHelper, location *time.Location, localMode, readOnly, insecureMode bool, querySelectTimeout time.Duration) (*Handler, error) {
	metadataLoader := metajournal.NewMetricMetaLoader(metadataClient, metajournal.DefaultMetaTimeout)
	diskCacheSuffix := metadataClient.Address // TODO - use cluster name or something here

	tmpl, err := template.ParseFS(staticDir, "index.html")
	if err != nil {
		return nil, fmt.Errorf("failed to parse index.html template: %w", err)
	}
	settings, err := json.Marshal(jsSettings)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal settings to JSON: %w", err)
	}
	metricStorage := metajournal.MakeMetricsStorage(diskCacheSuffix, diskCache, nil)
	metricStorage.Journal().Start(nil, nil, metadataLoader.LoadJournal)
	h := &Handler{
		verbose:           verbose,
		protectedPrefixes: protectedPrefixes,
		showInvisible:     showInvisible,
		utcOffset:         utcOffsetSec,
		staticDir:         http.FS(staticDir),
		indexTemplate:     tmpl,
		indexSettings:     string(settings),
		metadataLoader:    metadataLoader,
		ch: map[string]*util.ClickHouse{
			Version1: chV1,
			Version2: chV2,
		},
		metricsStorage: metricStorage,
		tagValueCache: &pcache.Cache{
			Loader: tagValueInverseLoader{
				loadTimeout: metajournal.DefaultMetaTimeout,
				metaClient:  metadataClient,
			}.load,
			DiskCache:               diskCache,
			DiskCacheNamespace:      data_model.TagValueInvertDiskNamespace + diskCacheSuffix,
			MaxMemCacheSize:         data_model.MappingMaxMemCacheSize,
			SpreadCacheTTL:          true,
			DefaultCacheTTL:         data_model.MappingCacheTTLMinimum,
			DefaultNegativeCacheTTL: data_model.MappingNegativeCacheTTL,
			LoadMinInterval:         data_model.MappingMinInterval,
			LoadBurst:               1000,
			Empty: func() pcache.Value {
				var empty pcache.StringValue
				return &empty
			},
		},
		tagValueIDCache: &pcache.Cache{
			Loader: tagValueLoader{
				loadTimeout: metajournal.DefaultMetaTimeout,
				metaClient:  metadataClient,
			}.load,
			DiskCache:               diskCache,
			DiskCacheNamespace:      data_model.TagValueDiskNamespace + diskCacheSuffix,
			MaxMemCacheSize:         data_model.MappingMaxMemCacheSize,
			SpreadCacheTTL:          true,
			DefaultCacheTTL:         data_model.MappingCacheTTLMinimum,
			DefaultNegativeCacheTTL: data_model.MappingNegativeCacheTTL,
			LoadMinInterval:         data_model.MappingMinInterval,
			LoadBurst:               1000,
			Empty: func() pcache.Value {
				var empty pcache.Int32Value
				return &empty
			},
		},
		cacheInvalidateTicker: time.NewTicker(cacheInvalidateCheckInterval),
		cacheInvalidateStop:   make(chan chan struct{}),
		jwtHelper:             jwtHelper,
		localMode:             localMode,
		plotRenderSem:         semaphore.NewWeighted(maxConcurrentPlots),
		plotTemplate:          ttemplate.Must(ttemplate.New("").Parse(gnuplotTemplate)),
		location:              location,
		readOnly:              readOnly,
		insecureMode:          insecureMode,
		accessManager:         &accessManager{metricStorage.GetGroupByMetricName},
		querySelectTimeout:    querySelectTimeout,
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &h.rUsage)

	h.cache = newTSCacheGroup(approxCacheMaxSize, lodTables, h.utcOffset, h.loadPoints, cacheDefaultDropEvery)
	h.pointsCache = newPointsCache(approxCacheMaxSize, h.utcOffset, h.loadPoint, time.Now)
	go h.invalidateLoop()
	h.rmID = statshouse.StartRegularMeasurement(func(client *statshouse.Client) { // TODO - stop
		prevRUsage := h.rUsage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &h.rUsage)
		userTime := float64(h.rUsage.Utime.Nano()-prevRUsage.Utime.Nano()) / float64(time.Second)
		sysTime := float64(h.rUsage.Stime.Nano()-prevRUsage.Stime.Nano()) / float64(time.Second)

		userMetric := client.Metric(format.BuiltinMetricNameUsageCPU, statshouse.Tags{1: strconv.Itoa(format.TagValueIDComponentAPI), 2: strconv.Itoa(format.TagValueIDCPUUsageUser)})
		userMetric.Value(userTime)
		sysMetric := client.Metric(format.BuiltinMetricNameUsageCPU, statshouse.Tags{1: strconv.Itoa(format.TagValueIDComponentAPI), 2: strconv.Itoa(format.TagValueIDCPUUsageSys)})
		sysMetric.Value(sysTime)

		var rss float64
		if st, _ := srvfunc.GetMemStat(0); st != nil {
			rss = float64(st.Res)
		}
		memMetric := client.Metric(format.BuiltinMetricNameUsageMemory, statshouse.Tags{1: strconv.Itoa(format.TagValueIDComponentAPI)})
		memMetric.Value(rss)

		writeActiveQuieries := func(ch *util.ClickHouse, versionTag string) {
			if ch != nil {
				fastLight := client.Metric(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneFastLight), 4: srvfunc.HostnameForStatshouse()})
				fastLight.Value(float64(ch.SemaphoreCountFastLight()))

				fastHeavy := client.Metric(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneFastHeavy), 4: srvfunc.HostnameForStatshouse()})
				fastHeavy.Value(float64(ch.SemaphoreCountFastHeavy()))

				slowLight := client.Metric(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneSlowLight), 4: srvfunc.HostnameForStatshouse()})
				slowLight.Value(float64(ch.SemaphoreCountSlowLight()))

				slowHeavy := client.Metric(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneSlowHeavy), 4: srvfunc.HostnameForStatshouse()})
				slowHeavy.Value(float64(ch.SemaphoreCountSlowHeavy()))
			}
		}
		writeActiveQuieries(chV1, "1")
		writeActiveQuieries(chV2, "2")
	})
	h.promEngine = promql.NewEngine(h, location)
	h.promEngineOn = true
	return h, nil
}

func (h *Handler) Close() error {
	statshouse.StopRegularMeasurement(h.rmID)
	h.cacheInvalidateTicker.Stop()

	ch := make(chan struct{})
	h.cacheInvalidateStop <- ch
	<-ch

	return nil
}

func (h *Handler) invalidateLoop() {
	var (
		from = time.Now().Unix()
		seen map[cacheInvalidateLogRow]struct{}
	)
	for {
		select {
		case ch := <-h.cacheInvalidateStop:
			close(ch)
			return
		case <-h.cacheInvalidateTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), cacheInvalidateCheckTimeout)
			from, seen = h.invalidateCache(ctx, from, seen)
			cancel()
		}
	}
}

func (h *Handler) invalidateCache(ctx context.Context, from int64, seen map[cacheInvalidateLogRow]struct{}) (int64, map[cacheInvalidateLogRow]struct{}) {
	uncertain := time.Now().Add(-invalidateLinger).Unix()
	if from > uncertain {
		from = uncertain
	}

	queryBody, err := util.BindQuery(fmt.Sprintf(`
SELECT
  toInt64(time) AS time, toInt64(key1) AS key1
FROM
  %s
WHERE
  metric == ? AND time >= ?
GROUP BY
  time, key1
ORDER BY
  time, key1
LIMIT
  ?
SETTINGS
  optimize_aggregation_in_order = 1
`, _1sTableSH2), format.BuiltinMetricIDContributorsLog, from, cacheInvalidateMaxRows)
	if err != nil {
		log.Printf("[error] cache invalidation log query failed: %v", err)
		return from, seen
	}
	// TODO - write metric with len(rows)
	// TODO - code that works if we hit limit above

	var (
		time    proto.ColInt64
		key1    proto.ColInt64
		todo    = map[int64][]int64{}
		newSeen = map[cacheInvalidateLogRow]struct{}{}
	)
	err = h.doSelect(ctx, util.QueryMetaInto{
		IsFast:  true,
		IsLight: true,
		User:    "cache-update",
		Metric:  format.BuiltinMetricIDContributorsLog,
		Table:   _1sTableSH2,
		Kind:    "cache-update",
	}, Version2, ch.Query{
		Body: queryBody,
		Result: proto.Results{
			{Name: "time", Data: &time},
			{Name: "key1", Data: &key1},
		},
		OnResult: func(_ context.Context, b proto.Block) error {
			for i := 0; i < b.Rows; i++ {
				r := cacheInvalidateLogRow{
					T:  time[i],
					At: key1[i],
				}
				newSeen[r] = struct{}{}
				from = r.T
				if _, ok := seen[r]; ok {
					continue
				}
				for lodLevel := range lodTables[Version2] {
					t := roundTime(r.At, lodLevel, h.utcOffset)
					w := todo[lodLevel]
					if len(w) == 0 || w[len(w)-1] != t {
						todo[lodLevel] = append(w, t)
					}
				}
			}
			return nil
		}})
	if err != nil {
		log.Printf("[error] cache invalidation log query failed: %v", err)
		return from, seen
	}

	for lodLevel, times := range todo {
		h.cache.Invalidate(lodLevel, times)
		if lodLevel == _1s {
			h.pointsCache.invalidate(times)
		}
	}

	return from, newSeen
}

func (h *Handler) doSelect(ctx context.Context, meta util.QueryMetaInto, version string, query ch.Query) error {
	if version == Version1 && h.ch[version] == nil {
		return fmt.Errorf("legacy ClickHouse database is disabled")
	}

	saveDebugQuery(ctx, query.Body)

	start := time.Now()
	endpointStatSetQueryKind(ctx, meta.IsFast, meta.IsLight)
	info, err := h.ch[version].Select(ctx, meta, query)
	duration := time.Since(start)
	if h.verbose {
		log.Printf("[debug] SQL for %q done in %v, err: %v", meta.User, duration, err)
	}

	ChSelectMetricDuration(info.Duration, meta.Metric, meta.User, meta.Table, meta.Kind, meta.IsFast, meta.IsLight, err)
	ChSelectProfile(meta.IsFast, meta.IsLight, info.Profile, err)

	return err
}

func (h *Handler) getMetricNameWithNamespace(metricID int32) (string, error) {
	if metricID == format.TagValueIDUnspecified {
		return format.CodeTagValue(format.TagValueIDUnspecified), nil
	}
	if m, ok := format.BuiltinMetrics[metricID]; ok {
		return m.Name, nil
	}
	v := h.metricsStorage.GetMetaMetric(metricID)
	if v == nil {
		return "", fmt.Errorf("metric name for ID %v not found", metricID)
	}
	return v.Name, nil
}

func (h *Handler) getMetricID(ai accessInfo, metricWithNamespace string) (int32, error) {
	if metricWithNamespace == format.CodeTagValue(format.TagValueIDUnspecified) {
		return format.TagValueIDUnspecified, nil
	}
	meta, err := h.getMetricMeta(ai, metricWithNamespace)
	if err != nil {
		return 0, err
	}
	return meta.MetricID, nil
}

// getMetricMeta only checks view access
func (h *Handler) getMetricMeta(ai accessInfo, metricWithNamespace string) (*format.MetricMetaValue, error) {
	if m, ok := format.BuiltinMetricByName[metricWithNamespace]; ok {
		return m, nil
	}
	v := h.metricsStorage.GetMetaMetricByName(metricWithNamespace)
	if v == nil {
		return nil, httpErr(http.StatusNotFound, fmt.Errorf("metric %q not found", metricWithNamespace))
	}
	if !ai.canViewMetric(metricWithNamespace) { // We are OK with sharing this bit of information with clients
		return nil, httpErr(http.StatusForbidden, fmt.Errorf("metric %q forbidden", metricWithNamespace))
	}
	return v, nil
}

func (h *Handler) getMetricNameByID(metricID int32) string {
	meta := format.BuiltinMetrics[metricID]
	if meta != nil {
		return meta.Name
	}
	meta = h.metricsStorage.GetMetaMetric(metricID)
	if meta != nil {
		return meta.Name
	}
	return ""
}

// For stats
func (h *Handler) getMetricIDForStat(metricWithNamespace string) int32 {
	if m, ok := format.BuiltinMetricByName[metricWithNamespace]; ok {
		return m.MetricID
	}
	v := h.metricsStorage.GetMetaMetricByName(metricWithNamespace)
	if v == nil {
		return 0
	}
	return v.MetricID
}

func (h *Handler) getTagValue(tagValueID int32) (string, error) {
	r := h.tagValueCache.GetOrLoad(time.Now(), strconv.FormatInt(int64(tagValueID), 10), nil)
	return pcache.ValueToString(r.Value), r.Err
}

func (h *Handler) getRichTagValue(metricMeta *format.MetricMetaValue, version string, tagID string, tagValueID int32) string {
	// Rich mapping between integers and strings must be perfect (with no duplicates on both sides)
	tag, ok := metricMeta.Name2Tag[tagID]
	if !ok {
		return format.CodeTagValue(tagValueID)
	}
	if tag.IsMetric {
		v, err := h.getMetricNameWithNamespace(tagValueID)
		if err != nil {
			return format.CodeTagValue(tagValueID)
		}
		return v
	}
	if tag.Raw {
		base := int32(0)
		if version == Version1 {
			base = format.TagValueIDRawDeltaLegacy
		}
		return format.CodeTagValue(tagValueID - base)
	}
	if tagValueID == format.TagValueIDMappingFloodLegacy && version == Version1 {
		return format.CodeTagValue(format.TagValueIDMappingFlood)
	}
	switch tagValueID {
	case format.TagValueIDUnspecified, format.TagValueIDMappingFlood:
		return format.CodeTagValue(tagValueID)
	default:
		v, err := h.getTagValue(tagValueID)
		if err != nil {
			return format.CodeTagValue(tagValueID)
		}
		return v
	}
}

func (h *Handler) getTagValueID(tagValue string) (int32, error) {
	r := h.tagValueIDCache.GetOrLoad(time.Now(), tagValue, nil)
	return pcache.ValueToInt32(r.Value), r.Err
}

func (h *Handler) getRichTagValueID(tag *format.MetricMetaTag, version string, tagValue string) (int32, error) {
	id, err := format.ParseCodeTagValue(tagValue)
	if err == nil {
		if version == Version1 && tag.Raw {
			id += format.TagValueIDRawDeltaLegacy
		}
		return id, nil
	}
	if tag.IsMetric {
		return h.getMetricID(accessInfo{insecureMode: true}, tagValue) // we don't consider metric ID to be private
	}
	if tag.Raw {
		value, ok := tag.Comment2Value[tagValue]
		if ok {
			id, err = format.ParseCodeTagValue(value)
			return id, err
		}
		// We could return error, but this will stop rendering, so we try conventional mapping also, even for raw tags
	}
	return h.getTagValueID(tagValue)
}

func (h *Handler) getRichTagValueIDs(metricMeta *format.MetricMetaValue, version string, tagID string, tagValues []string) ([]int32, error) {
	tag, ok := metricMeta.Name2Tag[tagID]
	if !ok {
		return nil, fmt.Errorf("tag with name %s not found for metric %s", tagID, metricMeta.Name)
	}
	ids := make([]int32, 0, len(tagValues))
	for _, v := range tagValues {
		id, err := h.getRichTagValueID(&tag, version, v)
		if err != nil {
			if httpCode(err) == http.StatusNotFound {
				continue // ignore values with no mapping
			}
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func formValueParamMetric(r *http.Request) string {
	const formerBuiltin = "__builtin_" // we renamed builtin metrics, removing prefix
	str := r.FormValue(ParamMetric)
	if strings.HasPrefix(str, formerBuiltin) {
		str = "__" + str[len(formerBuiltin):]
	}
	return str
}

func (h *Handler) resolveFilter(metricMeta *format.MetricMetaValue, version string, f map[string][]string) (map[string][]interface{}, error) {
	m := make(map[string][]interface{}, len(f))
	for k, values := range f {
		if version == Version1 && k == format.EnvTagID {
			continue // we only support production tables for v1
		}
		if k == format.StringTopTagID {
			for _, val := range values {
				m[k] = append(m[k], unspecifiedToEmpty(val))
			}
		} else {
			ids, err := h.getRichTagValueIDs(metricMeta, version, k, values)
			if err != nil {
				return nil, err
			}
			m[k] = []interface{}{}
			for _, id := range ids {
				m[k] = append(m[k], id)
			}
		}
	}
	return m, nil
}

func (h *Handler) HandleStatic(w http.ResponseWriter, r *http.Request) {
	origPath := r.URL.Path
	switch r.URL.Path {
	case "/":
	case "/index.html":
		r.URL.Path = "/"
	default:
		f, err := h.staticDir.Open(r.URL.Path) // stat is more efficient, but will require manual path manipulations
		if f != nil {
			_ = f.Close()
		}

		// 404 -> index.html, for client-side routing
		if err != nil && os.IsNotExist(err) { // TODO - replace with errors.Is(err, fs.ErrNotExist) when jessie is upgraded to go 1.16
			r.URL.Path = "/"
		}
	}

	switch {
	case r.URL.Path == "/":
		// make sure browser does not use stale versions
		w.Header().Set("Cache-Control", "public, no-cache, must-revalidate")
	case strings.HasPrefix(r.URL.Path, "/static/"):
		// everything under /static/ can be cached indefinitely (filenames contain content hashes)
		w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", cacheMaxAgeSeconds))
	}

	w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
	w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if origPath != "/embed" {
		w.Header().Set("X-Frame-Options", "deny")
	}

	if r.URL.Path == "/" {
		data := struct {
			OpenGraph *openGraphInfo
			Settings  string
		}{
			getOpenGraphInfo(r, origPath),
			h.indexSettings,
		}
		if err := h.indexTemplate.Execute(w, data); err != nil {
			log.Printf("[error] failed to write index.html: %v", err)
		}
	} else {
		http.FileServer(h.staticDir).ServeHTTP(w, r)
	}
}

func (h *Handler) parseAccessToken(w http.ResponseWriter, r *http.Request, es *endpointStat) (accessInfo, bool) {
	ai, err := h.accessManager.parseAccessToken(h.jwtHelper, vkuth.GetAccessToken(r), h.protectedPrefixes, h.localMode, h.insecureMode)
	if es != nil {
		es.setTokenName(ai.user)
	}

	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, es)
		return ai, false
	}
	return ai, true
}

func (h *Handler) HandleGetMetricsList(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointMetricList, r.Method, 0, "")
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}
	resp, cache, err := h.handleGetMetricsList(ai)
	respondJSON(w, resp, cache, queryClientCacheStale, err, h.verbose, ai.user, sl)
}

func (h *Handler) handleGetMetricsList(ai accessInfo) (*GetMetricsListResp, time.Duration, error) {
	ret := &GetMetricsListResp{
		Metrics: []metricShortInfo{},
	}
	for _, m := range format.BuiltinMetrics {
		if !h.showInvisible && !m.Visible { // we have invisible builtin metrics
			continue
		}
		ret.Metrics = append(ret.Metrics, metricShortInfo{Name: m.Name})
	}
	for _, v := range h.metricsStorage.GetMetaMetricList(h.showInvisible) {
		if ai.canViewMetric(v.Name) {
			ret.Metrics = append(ret.Metrics, metricShortInfo{Name: v.Name})
		}
	}

	sort.Slice(ret.Metrics, func(i int, j int) bool { return ret.Metrics[i].Name < ret.Metrics[j].Name })

	return ret, defaultCacheTTL, nil
}

func (h *Handler) HandleGetMetric(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointMetric, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), "")
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}
	resp, cache, err := h.handleGetMetric(ai, formValueParamMetric(r), r.FormValue(ParamID))
	respondJSON(w, resp, cache, 0, err, h.verbose, ai.user, sl) // we don't want clients to see stale metadata
}

func (h *Handler) HandleGetPromConfig(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointPrometheus, r.Method, 0, "")
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}
	resp, cache, err := h.handleGetPromConfig(ai)
	respondJSON(w, resp, cache, 0, err, h.verbose, ai.user, sl) // we don't want clients to see stale metadata
}

func (h *Handler) HandlePostMetric(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointMetric, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), "")
	if h.checkReadOnlyMode(w, r) {
		return
	}
	ai, ok := h.parseAccessToken(w, r, nil)
	if !ok {
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxEntityHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if len(res) >= maxEntityHTTPBodySize {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("metric body too big. Max size is %d bytes", maxEntityHTTPBodySize)), h.verbose, ai.user, sl)
		return
	}
	var metric MetricInfo
	if err := easyjson.Unmarshal(res, &metric); err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	m, err := h.handlePostMetric(r.Context(), ai, formValueParamMetric(r), metric.Metric)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	err = h.waitVersionUpdate(r.Context(), m.Version)
	respondJSON(w, &MetricInfo{Metric: m}, defaultCacheTTL, 0, err, h.verbose, ai.user, sl)
}

func handlePostEntity[T easyjson.Unmarshaler](h *Handler, w http.ResponseWriter, r *http.Request, endpoint string, entity T, handleCallback func(ctx context.Context, ai accessInfo, entity T, create bool) (resp interface{}, versionToWait int64, err error)) {
	sl := newEndpointStat(endpoint, r.Method, 0, "")
	if h.checkReadOnlyMode(w, r) {
		return
	}
	ai, ok := h.parseAccessToken(w, r, nil)
	if !ok {
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxEntityHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if len(res) >= maxEntityHTTPBodySize {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("entity body too big. Max size is %d bytes", maxEntityHTTPBodySize)), h.verbose, ai.user, sl)
		return
	}
	if err := easyjson.Unmarshal(res, entity); err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	d, version, err := handleCallback(r.Context(), ai, entity, r.Method == http.MethodPut)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	err = h.waitVersionUpdate(r.Context(), version)
	respondJSON(w, d, defaultCacheTTL, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandlePutPostGroup(w http.ResponseWriter, r *http.Request) {
	var groupInfo MetricsGroupInfo
	handlePostEntity(h, w, r, EndpointGroup, &groupInfo, func(ctx context.Context, ai accessInfo, entity *MetricsGroupInfo, create bool) (resp interface{}, versionToWait int64, err error) {
		response, err := h.handlePostGroup(ctx, ai, entity.Group, create)
		if err != nil {
			return nil, 0, err
		}
		return response, response.Group.Version, nil
	})
}

func (h *Handler) HandlePostNamespace(w http.ResponseWriter, r *http.Request) {
	var namespaceInfo NamespaceInfo
	handlePostEntity(h, w, r, EndpointNamespace, &namespaceInfo, func(ctx context.Context, ai accessInfo, entity *NamespaceInfo, create bool) (resp interface{}, versionToWait int64, err error) {
		response, err := h.handlePostNamespace(ctx, ai, entity.Namespace, create)
		if err != nil {
			return nil, 0, err
		}
		return response, response.Namespace.Version, nil
	})
}

func (h *Handler) HandlePostResetFlood(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointResetFlood, r.Method, 0, "")
	if h.checkReadOnlyMode(w, r) {
		return
	}
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}
	if !ai.isAdmin() {
		err := httpErr(http.StatusForbidden, fmt.Errorf("admin access required"))
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	var limit int32
	if v := r.FormValue("limit"); v != "" {
		i, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, err), h.verbose, ai.user, sl)
			return
		}
		limit = int32(i)
	}
	del, before, after, err := h.metadataLoader.ResetFlood(r.Context(), formValueParamMetric(r), limit)
	if err == nil && !del {
		err = fmt.Errorf("metric flood counter was empty (no flood)")
	}
	respondJSON(w, &struct {
		Before int32 `json:"before"`
		After  int32 `json:"after"`
	}{Before: before, After: after}, 0, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandlePostPromConfig(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointPrometheus, r.Method, 0, "")
	if h.checkReadOnlyMode(w, r) {
		return
	}
	ai, ok := h.parseAccessToken(w, r, nil)
	if !ok {
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxPromConfigHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if len(res) >= maxPromConfigHTTPBodySize {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("confog body too big. Max size is %d bytes", maxPromConfigHTTPBodySize)), h.verbose, ai.user, sl)
		return
	}
	event, err := h.handlePostPromConfig(r.Context(), ai, string(res))
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	err = h.waitVersionUpdate(r.Context(), event.Version)
	respondJSON(w, struct {
		Version int64 `json:"version"`
	}{event.Version}, defaultCacheTTL, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) handleGetMetric(ai accessInfo, metricWithNamespace string, metricIDStr string) (*MetricInfo, time.Duration, error) {
	if metricIDStr != "" {
		metricID, err := strconv.ParseInt(metricIDStr, 10, 32)
		if err != nil {
			return nil, 0, fmt.Errorf("can't parse %s", metricIDStr)
		}
		metricWithNamespace = h.getMetricNameByID(int32(metricID))
		if metricWithNamespace == "" {
			return nil, 0, fmt.Errorf("can't find metric %d", metricID)
		}
	}
	v, err := h.getMetricMeta(ai, metricWithNamespace)
	if err != nil {
		return nil, 0, err
	}
	return &MetricInfo{
		Metric: *v,
	}, defaultCacheTTL, nil
}

func (h *Handler) handleGetPromConfig(ai accessInfo) (string, time.Duration, error) {
	if !ai.isAdmin() {
		return "", 0, httpErr(http.StatusNotFound, fmt.Errorf("config is not found"))
	}
	config := h.metricsStorage.PromConfig()
	return config.Data, defaultCacheTTL, nil
}

func (h *Handler) handlePostPromConfig(ctx context.Context, ai accessInfo, configStr string) (tlmetadata.Event, error) {
	if !ai.isAdmin() {
		return tlmetadata.Event{}, httpErr(http.StatusNotFound, fmt.Errorf("config is not found"))
	}
	event, err := h.metadataLoader.SavePromConfig(ctx, h.metricsStorage.PromConfig().Version, configStr)
	if err != nil {
		return tlmetadata.Event{}, fmt.Errorf("failed to save prometheus config: %w", err)
	}
	return event, nil
}

func (h *Handler) handleGetDashboard(ai accessInfo, id int32) (*DashboardInfo, time.Duration, error) {
	if dash, ok := format.BuiltinDashboardByID[id]; ok {
		return &DashboardInfo{Dashboard: getDashboardMetaInfo(dash)}, defaultCacheTTL, nil
	}
	dash := h.metricsStorage.GetDashboardMeta(id)
	if dash == nil {
		return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("dashboard %d not found", id))
	}
	return &DashboardInfo{Dashboard: getDashboardMetaInfo(dash)}, defaultCacheTTL, nil
}

func (h *Handler) handleGetDashboardList(ai accessInfo) (*GetDashboardListResp, time.Duration, error) {
	dashs := h.metricsStorage.GetDashboardList()
	for _, meta := range format.BuiltinDashboardByID {
		dashs = append(dashs, meta)
	}
	resp := &GetDashboardListResp{}
	for _, dash := range dashs {
		description := ""
		descriptionI := dash.JSONData[descriptionFieldName]
		if descriptionI != nil {
			description, _ = descriptionI.(string)
		}
		resp.Dashboards = append(resp.Dashboards, dashboardShortInfo{
			Id:          dash.DashboardID,
			Name:        dash.Name,
			Description: description,
		})
	}
	return resp, defaultCacheTTL, nil
}

func (h *Handler) handlePostDashboard(ctx context.Context, ai accessInfo, dash DashboardMetaInfo, create, delete bool) (*DashboardInfo, error) {
	if !create {
		if _, ok := format.BuiltinDashboardByID[dash.DashboardID]; ok {
			return &DashboardInfo{}, httpErr(http.StatusBadRequest, fmt.Errorf("can't edit builtin dashboard %d", dash.DashboardID))
		}
		if h.metricsStorage.GetDashboardMeta(dash.DashboardID) == nil {
			return &DashboardInfo{}, httpErr(http.StatusNotFound, fmt.Errorf("dashboard %d not found", dash.DashboardID))
		}
	}
	if dash.JSONData == nil {
		dash.JSONData = map[string]interface{}{}
	}
	dash.JSONData[descriptionFieldName] = dash.Description
	dashboard, err := h.metadataLoader.SaveDashboard(ctx, format.DashboardMeta{
		DashboardID: dash.DashboardID,
		Name:        dash.Name,
		Version:     dash.Version,
		UpdateTime:  dash.UpdateTime,
		DeleteTime:  dash.DeletedTime,
		JSONData:    dash.JSONData,
	}, create, delete)
	if err != nil {
		s := "edit"
		if create {
			s = "create"
		}
		if metajournal.IsUserRequestError(err) {
			return &DashboardInfo{}, httpErr(http.StatusBadRequest, fmt.Errorf("can't %s dashboard: %w", s, err))
		}
		return &DashboardInfo{}, fmt.Errorf("can't %s dashboard: %w", s, err)
	}
	return &DashboardInfo{Dashboard: getDashboardMetaInfo(&dashboard)}, nil
}

func (h *Handler) handleGetGroup(ai accessInfo, id int32) (*MetricsGroupInfo, time.Duration, error) {
	group, ok := h.metricsStorage.GetGroupWithMetricsList(id)
	if !ok {
		return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("group %d not found", id))
	}
	return &MetricsGroupInfo{Group: *group.Group, Metrics: group.Metrics}, defaultCacheTTL, nil
}

func (h *Handler) handleGetGroupsList(ai accessInfo) (*GetGroupListResp, time.Duration, error) {
	groups := h.metricsStorage.GetGroupsList()
	resp := &GetGroupListResp{}
	for _, group := range groups {
		resp.Groups = append(resp.Groups, groupShortInfo{
			Id:     group.ID,
			Name:   group.Name,
			Weight: group.Weight,
		})
	}
	return resp, defaultCacheTTL, nil
}

func (h *Handler) handleGetNamespace(ai accessInfo, id int32) (*NamespaceInfo, time.Duration, error) {
	namespace := h.metricsStorage.GetNamespace(id)
	if namespace == nil {
		return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("namespace %d not found", id))
	}
	return &NamespaceInfo{Namespace: *namespace}, defaultCacheTTL, nil
}

func (h *Handler) handleGetNamespaceList(ai accessInfo) (*GetNamespaceListResp, time.Duration, error) {
	namespaces := h.metricsStorage.GetNamespaceList()
	var namespacesResp []namespaceShortInfo
	for _, namespace := range namespaces {
		namespacesResp = append(namespacesResp, namespaceShortInfo{
			Id:     namespace.ID,
			Name:   namespace.Name,
			Weight: namespace.Weight,
		})
	}
	return &GetNamespaceListResp{Namespaces: namespacesResp}, defaultCacheTTL, nil
}

func (h *Handler) handlePostNamespace(ctx context.Context, ai accessInfo, namespace format.NamespaceMeta, create bool) (*NamespaceInfo, error) {
	if !ai.isAdmin() {
		return nil, httpErr(http.StatusNotFound, fmt.Errorf("namespace %s not found", namespace.Name))
	}
	if !create {
		if h.metricsStorage.GetNamespace(namespace.ID) == nil {
			return &NamespaceInfo{}, httpErr(http.StatusNotFound, fmt.Errorf("namespace %d not found", namespace.ID))
		}
	}
	namespace, err := h.metadataLoader.SaveNamespace(ctx, namespace, create)
	if err != nil {
		s := "edit"
		if create {
			s = "create"
		}
		errReturn := fmt.Errorf("can't %s namespace: %w", s, err)
		if metajournal.IsUserRequestError(err) {
			return &NamespaceInfo{}, httpErr(http.StatusBadRequest, errReturn)
		}
		return &NamespaceInfo{}, errReturn
	}
	return &NamespaceInfo{Namespace: namespace}, nil
}

func (h *Handler) handlePostGroup(ctx context.Context, ai accessInfo, group format.MetricsGroup, create bool) (*MetricsGroupInfo, error) {
	if !ai.isAdmin() {
		return nil, httpErr(http.StatusNotFound, fmt.Errorf("group %s not found", group.Name))
	}
	if !create {
		if h.metricsStorage.GetGroup(group.ID) == nil {
			return &MetricsGroupInfo{}, httpErr(http.StatusNotFound, fmt.Errorf("group %d not found", group.ID))
		}
	}
	if !h.metricsStorage.CanAddOrChangeGroup(group.Name, group.ID) {
		return &MetricsGroupInfo{}, httpErr(http.StatusBadRequest, fmt.Errorf("group name %s is not posible", group.Name))
	}
	group, err := h.metadataLoader.SaveMetricsGroup(ctx, group, create)
	if err != nil {
		s := "edit"
		if create {
			s = "create"
		}
		errReturn := fmt.Errorf("can't %s group: %w", s, err)
		if metajournal.IsUserRequestError(err) {
			return &MetricsGroupInfo{}, httpErr(http.StatusBadRequest, errReturn)
		}
		return &MetricsGroupInfo{}, errReturn
	}
	return &MetricsGroupInfo{Group: group}, nil
}

// TODO - remove metric name from request
func (h *Handler) handlePostMetric(ctx context.Context, ai accessInfo, _ string, metric format.MetricMetaValue) (format.MetricMetaValue, error) {
	create := metric.MetricID == 0
	var resp format.MetricMetaValue
	var err error
	if metric.GroupID != 0 {
		if h.metricsStorage.GetGroup(metric.GroupID) != nil {
			return format.MetricMetaValue{}, fmt.Errorf("invalid group id: %d", metric.GroupID)
		}
	}
	if metric.PreKeyOnly && (metric.PreKeyFrom == 0 || metric.PreKeyTagID == "") {
		return format.MetricMetaValue{}, httpErr(http.StatusBadRequest, fmt.Errorf("use prekey_only with non empty prekey_tag_id"))
	}
	if create {
		if !ai.canEditMetric(true, metric, metric) {
			return format.MetricMetaValue{}, httpErr(http.StatusForbidden, fmt.Errorf("can't create metric %q", metric.Name))
		}
		resp, err = h.metadataLoader.SaveMetric(ctx, metric)
		if err != nil {
			err = fmt.Errorf("error creating metric in sqlite engine: %w", err)
			log.Println(err.Error())
			return format.MetricMetaValue{}, fmt.Errorf("failed to create metric: %w", err)
		}
	} else {
		if _, ok := format.BuiltinMetrics[metric.MetricID]; ok {
			return format.MetricMetaValue{}, httpErr(http.StatusBadRequest, fmt.Errorf("builtin metric cannot be edited"))
		}
		old := h.metricsStorage.GetMetaMetric(metric.MetricID)
		if old == nil {
			return format.MetricMetaValue{}, httpErr(http.StatusNotFound, fmt.Errorf("metric %q not found (id %d)", metric.Name, metric.MetricID))
		}
		if !ai.canEditMetric(false, *old, metric) {
			return format.MetricMetaValue{}, httpErr(http.StatusForbidden, fmt.Errorf("can't edit metric %q", old.Name))
		}
		resp, err = h.metadataLoader.SaveMetric(ctx, metric)
		if err != nil {
			err = fmt.Errorf("error saving metric in sqllite: %w", err)
			log.Println(err.Error())
			return format.MetricMetaValue{}, fmt.Errorf("can't edit metric: %w", err)
		}
	}
	return resp, nil
}

func (h *Handler) HandleGetMetricTagValues(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointMetricTagValues, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), "")
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()

	_ = r.ParseForm() // (*http.Request).FormValue ignores parse errors, too
	resp, immutable, err := h.handleGetMetricTagValues(
		ctx,
		getMetricTagValuesReq{
			ai:                  ai,
			version:             r.FormValue(ParamVersion),
			numResults:          r.FormValue(ParamNumResults),
			metricWithNamespace: formValueParamMetric(r),
			tagID:               r.FormValue(ParamTagID),
			from:                r.FormValue(ParamFromTime),
			to:                  r.FormValue(ParamToTime),
			what:                r.FormValue(ParamQueryWhat),
			filter:              r.Form[ParamQueryFilter],
		})

	cache, cacheStale := queryClientCacheDuration(immutable)
	respondJSON(w, resp, cache, cacheStale, err, h.verbose, ai.user, sl)
}

type selectRow struct {
	valID int32
	val   string
	cnt   float64
}

type tagValuesSelectCols struct {
	meta  tagValuesQueryMeta
	valID proto.ColInt32
	val   proto.ColStr
	cnt   proto.ColFloat64
	res   proto.Results
}

func newTagValuesSelectCols(meta tagValuesQueryMeta) *tagValuesSelectCols {
	// NB! Keep columns selection order and names is sync with sql.go code
	c := &tagValuesSelectCols{meta: meta}
	if meta.stringValue {
		c.res = append(c.res, proto.ResultColumn{Name: "_string_value", Data: &c.val})
	} else {
		c.res = append(c.res, proto.ResultColumn{Name: "_value", Data: &c.valID})
	}
	c.res = append(c.res, proto.ResultColumn{Name: "_count", Data: &c.cnt})
	return c
}

func (c *tagValuesSelectCols) rowAt(i int) selectRow {
	row := selectRow{cnt: c.cnt[i]}
	if c.meta.stringValue {
		pos := c.val.Pos[i]
		row.val = string(c.val.Buf[pos.Start:pos.End])
	} else {
		row.valID = c.valID[i]
	}
	return row
}

func (h *Handler) handleGetMetricTagValues(ctx context.Context, req getMetricTagValuesReq) (resp *GetMetricTagValuesResp, immutable bool, err error) {
	version, err := parseVersion(req.version)
	if err != nil {
		return nil, false, err
	}

	numResults, err := parseNumResults(req.numResults, defTagValues, maxTagValues)
	if err != nil {
		return nil, false, err
	}

	metricMeta, err := h.getMetricMeta(req.ai, req.metricWithNamespace)
	if err != nil {
		return nil, false, err
	}

	err = validateQuery(metricMeta, version)
	if err != nil {
		return nil, false, err
	}

	tagID, err := parseTagID(req.tagID)
	if err != nil {
		return nil, false, err
	}

	from, to, err := parseFromTo(req.from, req.to)
	if err != nil {
		return nil, false, err
	}

	_, kind, err := parseQueryWhat(req.what, false)
	if err != nil {
		return nil, false, err
	}

	filterIn, filterNotIn, err := parseQueryFilter(req.filter)
	if err != nil {
		return nil, false, err
	}
	mappedFilterIn, err := h.resolveFilter(metricMeta, version, filterIn)
	if err != nil {
		return nil, false, err
	}
	mappedFilterNotIn, err := h.resolveFilter(metricMeta, version, filterNotIn)
	if err != nil {
		return nil, false, err
	}

	lods := selectTagValueLODs(
		version,
		int64(metricMeta.PreKeyFrom),
		metricMeta.PreKeyOnly,
		metricMeta.Resolution,
		kind == queryFnKindUnique,
		metricMeta.StringTopDescription != "",
		time.Now().Unix(),
		from.Unix(),
		to.Unix(),
		h.utcOffset,
		h.location,
	)
	pq := &preparedTagValuesQuery{
		version:     version,
		metricID:    metricMeta.MetricID,
		preKeyTagID: metricMeta.PreKeyTagID,
		tagID:       tagID,
		numResults:  numResults,
		filterIn:    mappedFilterIn,
		filterNotIn: mappedFilterNotIn,
	}

	tagInfo := map[selectRow]float64{}
	if version == Version1 && tagID == format.EnvTagID {
		tagInfo[selectRow{valID: format.TagValueIDProductionLegacy}] = 100 // we only support production tables for v1
	} else {
		for _, lod := range lods {
			query, args, err := tagValuesQuery(pq, lod) // we set limit to numResult+1
			if err != nil {
				return nil, false, err
			}
			cols := newTagValuesSelectCols(args)
			isFast := lod.fromSec+fastQueryTimeInterval >= lod.toSec
			err = h.doSelect(ctx, util.QueryMetaInto{
				IsFast:  isFast,
				IsLight: true,
				User:    req.ai.user,
				Metric:  metricMeta.MetricID,
				Table:   lod.table,
				Kind:    "get_mapping",
			}, version, ch.Query{
				Body:   query,
				Result: cols.res,
				OnResult: func(_ context.Context, b proto.Block) error {
					for i := 0; i < b.Rows; i++ {
						tag := cols.rowAt(i)
						tagInfo[selectRow{valID: tag.valID, val: tag.val}] += tag.cnt
					}
					return nil
				}})
			if err != nil {
				return nil, false, err
			}
		}
	}

	data := make([]selectRow, 0, len(tagInfo))
	for k, count := range tagInfo {
		data = append(data, selectRow{valID: k.valID, val: k.val, cnt: count})
	}
	sort.Slice(data, func(i int, j int) bool { return data[i].cnt > data[j].cnt })

	ret := &GetMetricTagValuesResp{
		TagValues: []MetricTagValueInfo{},
	}
	if len(data) > numResults {
		data = data[:numResults]
		ret.TagValuesMore = true
	}
	for _, d := range data {
		v := d.val
		if pq.stringTag() {
			v = emptyToUnspecified(v)
		} else {
			v = h.getRichTagValue(metricMeta, version, tagID, d.valID)
		}
		ret.TagValues = append(ret.TagValues, MetricTagValueInfo{
			Value: v,
			Count: d.cnt,
		})
	}

	immutable = to.Before(time.Now().Add(invalidateFrom))
	return ret, immutable, nil
}

func sumSeries(data *[]float64, missingValue float64) float64 {
	result := 0.0
	for _, c := range *data {
		if math.IsNaN(c) {
			result += missingValue
		} else {
			result += c
		}
	}
	return result
}

func (h *Handler) HandleGetTable(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointTable, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), r.FormValue(paramDataFormat))
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()

	_ = r.ParseForm() // (*http.Request).FormValue ignores parse errors, too
	metricWithNamespace := formValueParamMetric(r)

	if r.FormValue(paramDataFormat) == dataFormatCSV {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("df=csv isn't supported")), h.verbose, ai.user, sl)
		return
	}

	filterIn, filterNotIn, err := parseQueryFilter(r.Form[ParamQueryFilter])
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}

	fromRow, err := parseFromRows(r.FormValue(paramFromRow))
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	toRow, err := parseFromRows(r.FormValue(paramToRow))
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}

	_, avoidCache := r.Form[ParamAvoidCache]
	if avoidCache && !ai.isAdmin() {
		respondJSON(w, nil, 0, 0, httpErr(404, fmt.Errorf("")), h.verbose, ai.user, sl)
		return
	}

	_, fromEnd := r.Form[paramFromEnd]
	var limit int64 = maxTableRowsPage
	if limitStr := r.FormValue(ParamNumResults); limitStr != "" {
		limit, err = strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf(ParamNumResults+" must be a valid number")), h.verbose, ai.user, sl)
			return
		}
	}

	respTable, immutable, err := h.handleGetTable(
		ctx,
		ai,
		true,
		tableRequest{
			version:             r.FormValue(ParamVersion),
			metricWithNamespace: metricWithNamespace,
			from:                r.FormValue(ParamFromTime),
			to:                  r.FormValue(ParamToTime),
			width:               r.FormValue(ParamWidth),
			widthAgg:            r.FormValue(ParamWidthAgg),
			what:                r.Form[ParamQueryWhat],
			by:                  r.Form[ParamQueryBy],
			filterIn:            filterIn,
			filterNotIn:         filterNotIn,
			avoidCache:          avoidCache,
			fromRow:             fromRow,
			toRow:               toRow,
			fromEnd:             fromEnd,
			limit:               int(limit),
		},
		seriesRequestOptions{
			debugQueries: true,
			stat:         sl,
		})
	if h.verbose && err == nil {
		log.Printf("[debug] handled query (%v rows) for %q in %v", len(respTable.Rows), ai.user, time.Since(sl.startTime))
	}

	cache, cacheStale := queryClientCacheDuration(immutable)
	respondJSON(w, respTable, cache, cacheStale, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandleSeriesQuery(w http.ResponseWriter, r *http.Request) {
	// Authenticate
	sl := newEndpointStat(EndpointQuery, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), r.FormValue(paramDataFormat))
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}
	if h.promEngineOn && r.FormValue(paramLegacyEngine) == "" {
		h.handleSeriesQueryPromQL(w, r, sl, ai)
		return
	}
	// Parse request
	qry, _, err := h.parseHTTPRequest(r)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	// Query series and badges
	var (
		ctx, cancel   = context.WithTimeout(r.Context(), h.querySelectTimeout)
		freeBadges    func()
		freePromqlRes func()
		freeRes       func()
		options       = seriesRequestOptions{
			debugQueries: true,
			testPromql:   len(qry.promQL) == 0 && ai.bitDeveloper,
			stat:         sl,
		}
		promqlErr error
		res       *SeriesResponse
		badges    *SeriesResponse
		promqlRes *SeriesResponse
	)
	defer func() {
		cancel()
		if freeBadges != nil {
			freeBadges()
		}
		if freePromqlRes != nil {
			freePromqlRes()
		}
		if freeRes != nil {
			freeRes()
		}
	}()
	if len(qry.promQL) != 0 {
		// PromQL request
		if !qry.verbose {
			res, freeRes, err = h.handlePromqlQuery(withHTTPEndpointStat(ctx, sl), ai, qry, options)
		} else {
			var g *errgroup.Group
			g, ctx = errgroup.WithContext(ctx)
			options.metricNameCallback = func(s string) {
				qry.metricWithNamespace = s
				g.Go(func() error {
					var err error
					badges, freeBadges, err = h.queryBadges(ctx, ai, qry)
					return err
				})
			}
			g.Go(func() error {
				var err error
				res, freeRes, err = h.handlePromqlQuery(withHTTPEndpointStat(ctx, sl), ai, qry, options)
				return err
			})
			err = g.Wait()
		}
	} else if qry.verbose || options.testPromql {
		var g *errgroup.Group
		g, ctx = errgroup.WithContext(ctx)
		if options.testPromql {
			options.rand = rand.New()
			options.timeNow = time.Now()
			g.Go(func() error {
				promqlRes, freePromqlRes, promqlErr = h.handlePromqlQuery(ctx, ai, qry, options)
				return nil // request is still succeedes if PromQL test fail
			})
		}
		g.Go(func() error {
			var err error
			res, freeRes, err = h.handleGetQuery(withHTTPEndpointStat(ctx, sl), ai, qry, options)
			return err
		})
		g.Go(func() error {
			var err error
			badges, freeBadges, err = h.queryBadges(ctx, ai, qry)
			return err
		})
		err = g.Wait()
	} else {
		res, freeRes, err = h.handleGetQuery(withHTTPEndpointStat(ctx, sl), ai, qry, options)
	}
	if err == nil && len(qry.promQL) == 0 {
		res.PromQL, err = getPromQuery(qry, false)
		res.DebugPromQLTestFailed = options.testPromql && (err != nil || promqlErr != nil ||
			!reflect.DeepEqual(res.queries, promqlRes.queries) ||
			!getQueryRespEqual(res, promqlRes))
		if res.DebugPromQLTestFailed {
			log.Printf("promqltestfailed %q %s", r.RequestURI, res.PromQL)
		}
	}
	// Add badges
	if qry.verbose && err == nil && badges != nil && len(badges.Series.Time) > 0 {
		// TODO - skip values outside display range. Badge now does not correspond directly to points displayed.
		for i, meta := range badges.Series.SeriesMeta {
			badgeTypeSamplingFactorSrc := format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeAgentSamplingFactor))
			badgeTypeSamplingFactorAgg := format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeAggSamplingFactor))
			badgeTypeReceiveErrors := format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeIngestionErrors))
			badgeTypeReceiveWarnings := format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeIngestionWarnings))
			badgeTypeMappingErrors := format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeAggMappingErrors))
			if meta.Tags["key2"].Value == qry.metricWithNamespace {
				badgeType := meta.Tags["key1"].Value
				switch {
				case meta.What.String() == ParamQueryFnAvg && badgeType == badgeTypeSamplingFactorSrc:
					res.SamplingFactorSrc = sumSeries(badges.Series.SeriesData[i], 1) / float64(len(badges.Series.Time))
				case meta.What.String() == ParamQueryFnAvg && badgeType == badgeTypeSamplingFactorAgg:
					res.SamplingFactorAgg = sumSeries(badges.Series.SeriesData[i], 1) / float64(len(badges.Series.Time))
				case meta.What.String() == ParamQueryFnCount && badgeType == badgeTypeReceiveErrors:
					res.ReceiveErrors = sumSeries(badges.Series.SeriesData[i], 0)
				case meta.What.String() == ParamQueryFnCount && badgeType == badgeTypeReceiveWarnings:
					res.ReceiveWarnings = sumSeries(badges.Series.SeriesData[i], 0)
				case meta.What.String() == ParamQueryFnCount && badgeType == badgeTypeMappingErrors:
					res.MappingErrors = sumSeries(badges.Series.SeriesData[i], 0)
				}
			}
			// TODO - show badge if some heuristics on # of contributors is triggered
			// if format.IsValueCodeZero(metric) && meta.What.String() == ParamQueryFnCountNorm && badgeType == format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeContributors)) {
			//	sumContributors := sumSeries(respIngestion.Series.SeriesData[i], 0)
			//	fmt.Printf("contributors sum %f\n", sumContributors)
			// }
		}
	}
	// Format and write the response
	h.colorize(res)
	switch {
	case err == nil && qry.format == dataFormatCSV:
		exportCSV(w, res, qry.metricWithNamespace, sl)
	default:
		var cache, cacheStale time.Duration
		if res != nil {
			cache, cacheStale = queryClientCacheDuration(res.immutable)
		}
		respondJSON(w, res, cache, cacheStale, err, h.verbose, ai.user, sl)
	}
}

func (h *Handler) handleSeriesQueryPromQL(w http.ResponseWriter, r *http.Request, sl *endpointStat, ai accessInfo) {
	// Parse request
	qry, vars, err := h.parseHTTPRequest(r)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	err = qry.validate(ai)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	// Query series and badges
	var (
		ctx, cancel = context.WithTimeout(r.Context(), h.querySelectTimeout)
		freeBadges  func()
		freeRes     func()
		res         *SeriesResponse
		badges      *SeriesResponse
	)
	defer func() {
		cancel()
		if freeBadges != nil {
			freeBadges()
		}
		if freeRes != nil {
			freeRes()
		}
	}()
	if qry.verbose {
		var g *errgroup.Group
		g, ctx = errgroup.WithContext(ctx)
		g.Go(func() error {
			var err error
			res, freeRes, err = h.handlePromqlQuery(withHTTPEndpointStat(ctx, sl), ai, qry, seriesRequestOptions{
				debugQueries: true,
				stat:         sl,
				vars:         vars,
				metricNameCallback: func(name string) {
					qry.metricWithNamespace = name
					g.Go(func() error {
						var err error
						badges, freeBadges, err = h.queryBadgesPromQL(ctx, ai, qry)
						return err
					})
				},
			})
			return err
		})
		err = g.Wait()
	} else {
		res, freeRes, err = h.handlePromqlQuery(withHTTPEndpointStat(ctx, sl), ai, qry, seriesRequestOptions{
			debugQueries: true,
			stat:         sl,
			vars:         vars,
		})
	}
	var traces []string
	if res != nil {
		traces = append(traces, res.DebugQueries...)
	}
	if badges != nil {
		traces = append(traces, badges.DebugQueries...)
	}
	// Add badges
	if qry.verbose && err == nil && badges != nil && len(badges.Series.Time) > 0 {
		// TODO - skip values outside display range. Badge now does not correspond directly to points displayed.
		for i, meta := range badges.Series.SeriesMeta {
			badgeTypeSamplingFactorSrc := format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeAgentSamplingFactor))
			badgeTypeSamplingFactorAgg := format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeAggSamplingFactor))
			badgeTypeReceiveErrors := format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeIngestionErrors))
			badgeTypeReceiveWarnings := format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeIngestionWarnings))
			badgeTypeMappingErrors := format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeAggMappingErrors))
			if meta.Tags["key2"].Value == qry.metricWithNamespace {
				badgeType := meta.Tags["key1"].Value
				switch {
				case meta.What.String() == ParamQueryFnAvg && badgeType == badgeTypeSamplingFactorSrc:
					res.SamplingFactorSrc = sumSeries(badges.Series.SeriesData[i], 1) / float64(len(badges.Series.Time))
				case meta.What.String() == ParamQueryFnAvg && badgeType == badgeTypeSamplingFactorAgg:
					res.SamplingFactorAgg = sumSeries(badges.Series.SeriesData[i], 1) / float64(len(badges.Series.Time))
				case meta.What.String() == ParamQueryFnCount && badgeType == badgeTypeReceiveErrors:
					res.ReceiveErrors = sumSeries(badges.Series.SeriesData[i], 0)
				case meta.What.String() == ParamQueryFnCount && badgeType == badgeTypeReceiveWarnings:
					res.ReceiveWarnings = sumSeries(badges.Series.SeriesData[i], 0)
				case meta.What.String() == ParamQueryFnCount && badgeType == badgeTypeMappingErrors:
					res.MappingErrors = sumSeries(badges.Series.SeriesData[i], 0)
				}
			}
			// TODO - show badge if some heuristics on # of contributors is triggered
			// if format.IsValueCodeZero(metric) && meta.What.String() == ParamQueryFnCountNorm && badgeType == format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeContributors)) {
			//	sumContributors := sumSeries(respIngestion.Series.SeriesData[i], 0)
			//	fmt.Printf("contributors sum %f\n", sumContributors)
			// }
		}
	}
	if res != nil {
		res.PromQL, _ = getPromQuery(qry, false)
		res.DebugQueries = traces
		h.colorize(res)
	}
	// Format and write the response
	switch {
	case err == nil && r.FormValue(paramDataFormat) == dataFormatCSV:
		exportCSV(w, res, qry.metricWithNamespace, sl)
	default:
		var cache, cacheStale time.Duration
		if res != nil {
			cache, cacheStale = queryClientCacheDuration(res.immutable)
		}
		respondJSON(w, res, cache, cacheStale, err, h.verbose, ai.user, sl)
	}
}

func (h *Handler) queryBadges(ctx context.Context, ai accessInfo, req seriesRequest) (*SeriesResponse, func(), error) {
	return h.handleGetQuery(
		ctx, ai.withBadgesRequest(),
		seriesRequest{
			version:             Version2,
			numResults:          20,
			metricWithNamespace: format.BuiltinMetricNameBadges,
			from:                req.from,
			to:                  req.to,
			width:               req.width,
			widthKind:           req.widthKind, // TODO - resolution of badge metric (currently 5s)?
			what:                []string{ParamQueryFnCount, ParamQueryFnAvg},
			by:                  []string{"1", "2"},
			filterIn:            map[string][]string{"2": {req.metricWithNamespace, format.AddRawValuePrefix("0")}},
		},
		seriesRequestOptions{})
}

func (h *Handler) queryBadgesPromQL(ctx context.Context, ai accessInfo, req seriesRequest) (*SeriesResponse, func(), error) {
	ai.skipBadgesValidation = true
	return h.handlePromqlQuery(
		ctx, ai,
		seriesRequest{
			version:             Version2,
			numResults:          20,
			metricWithNamespace: format.BuiltinMetricNameBadges,
			from:                req.from,
			to:                  req.to,
			width:               req.width,
			widthKind:           req.widthKind, // TODO - resolution of badge metric (currently 5s)?
			what:                []string{ParamQueryFnCount, ParamQueryFnAvg},
			by:                  []string{"1", "2"},
			filterIn:            map[string][]string{"2": {req.metricWithNamespace, format.AddRawValuePrefix("0")}},
		},
		seriesRequestOptions{debugQueries: true})
}

func (h *Handler) handlePromqlQuery(ctx context.Context, ai accessInfo, req seriesRequest, opt seriesRequestOptions) (*SeriesResponse, func(), error) {
	err := req.validate(ai)
	if err != nil {
		return nil, nil, err
	}
	var promqlGenerated bool
	if len(req.promQL) == 0 {
		req.promQL, err = getPromQuery(req, true)
		if err != nil {
			return nil, nil, httpErr(http.StatusBadRequest, err)
		}
		promqlGenerated = true
	}
	if opt.timeNow.IsZero() {
		opt.timeNow = time.Now()
	}
	var (
		seriesQueries       map[lodInfo]int
		seriesQueryCallback promql.SeriesQueryCallback
	)
	if opt.testPromql {
		seriesQueries = make(map[lodInfo]int)
		seriesQueryCallback = func(version string, key string, pq any, lod any, avoidCache bool) {
			seriesQueries[lod.(lodInfo)]++
		}
	}
	var offsets = make([]int64, 0, len(req.shifts))
	for _, v := range req.shifts {
		offsets = append(offsets, -toSec(v))
	}
	var (
		metricName string
		metricMeta *format.MetricMetaValue
		options    = promql.Options{
			Version:             req.version,
			AvoidCache:          req.avoidCache,
			TimeNow:             opt.timeNow.Unix(),
			ExpandToLODBoundary: req.expandToLODBoundary,
			ExplicitGrouping:    true,
			MaxHost:             req.maxHost,
			Offsets:             offsets,
			Rand:                opt.rand,
			ExprQueriesSingleMetricCallback: func(metric *format.MetricMetaValue) {
				metricMeta = metric
				metricName = metric.Name
				if opt.metricNameCallback != nil {
					opt.metricNameCallback(metricName)
				}
			},
			SeriesQueryCallback: seriesQueryCallback,
			Vars:                opt.vars,
		}
	)
	if req.widthKind == widthAutoRes {
		options.StepAuto = true
	}
	var traces []string
	if opt.debugQueries {
		ctx = debugQueriesContext(ctx, &traces)
		ctx = promql.TraceContext(ctx, &traces)
	}
	parserV, cleanup, err := h.promEngine.Exec(
		withAccessInfo(ctx, &ai),
		promql.Query{
			Start:   req.from.Unix(),
			End:     req.to.Unix(),
			Step:    int64(req.width),
			Expr:    req.promQL,
			Options: options,
		})
	if err != nil {
		return nil, nil, err
	}
	ts, ok := parserV.(*promql.TimeSeries)
	if !ok {
		return nil, nil, fmt.Errorf("string literals are not supported")
	}
	res := &SeriesResponse{
		Series: querySeries{
			Time:       ts.Time,
			SeriesData: make([]*[]float64, 0, len(ts.Series.Data)),
			SeriesMeta: make([]QuerySeriesMetaV2, 0, len(ts.Series.Data)),
		},
		MetricMeta:   metricMeta,
		DebugQueries: traces,
	}
	for _, s := range ts.Series.Data {
		meta := QuerySeriesMetaV2{
			Name:      metricName,
			Tags:      make(map[string]SeriesMetaTag, len(s.Tags.ID2Tag)),
			MaxHosts:  s.Tags.GetSMaxHosts(h),
			TimeShift: -s.Offset,
			Total:     ts.Series.Meta.Total,
		}
		if promqlGenerated {
			meta.What = queryFn(s.What)
		} else {
			meta.What = queryFn(ts.Series.Meta.What)
		}
		if ts.Series.Meta.Metric != nil {
			meta.MetricType = ts.Series.Meta.Metric.MetricType
		}
		if meta.Total == 0 {
			meta.Total = len(ts.Series.Data)
		}
		for id, tag := range s.Tags.ID2Tag {
			if len(tag.SValue) == 0 || tag.ID == labels.MetricName {
				continue
			}
			var (
				k = id
				v = SeriesMetaTag{Value: tag.SValue}
			)
			if ts.Series.Meta.Metric != nil && tag.Index != 0 {
				var (
					name  string
					index = tag.Index - promql.SeriesTagIndexOffset
				)
				if index == format.StringTopTagIndex {
					k = format.LegacyStringTopTagID
					name = format.StringTopTagID
				} else {
					k = format.TagIDLegacy(index)
					name = format.TagID(index)
				}
				if meta, ok := ts.Series.Meta.Metric.Name2Tag[name]; ok {
					v.Comment = meta.ValueComments[v.Value]
					v.Raw = meta.Raw
					v.RawKind = meta.RawKind
				}
			}
			meta.Tags[k] = v
		}
		res.Series.SeriesMeta = append(res.Series.SeriesMeta, meta)
		res.Series.SeriesData = append(res.Series.SeriesData, s.Values)
	}
	if len(ts.Time) != 0 {
		res.ExcessPointLeft = ts.Time[0] < req.from.Unix()
		res.ExcessPointRight = req.to.Unix() < ts.Time[len(ts.Time)-1]
	}
	if res.Series.SeriesData == nil {
		// frontend expects not "null" value
		res.Series.SeriesData = make([]*[]float64, 0)
	}
	// clamp values because JSON doesn't support "Inf" values,
	// extra large values usually don't make sense.
	// TODO: don't lose values, pass large values (including "Inf" as is)
	for _, p := range res.Series.SeriesData {
		row := *p
		for i, v := range row {
			if v < -math.MaxFloat32 {
				row[i] = -math.MaxFloat32
			} else if v > math.MaxFloat32 {
				row[i] = math.MaxFloat32
			}
		}
	}
	if promqlGenerated {
		res.PromQL = req.promQL
	}
	res.queries = seriesQueries
	return res, cleanup, nil
}

func (h *Handler) handleGetQuery(ctx context.Context, ai accessInfo, req seriesRequest, opt seriesRequestOptions) (resp *SeriesResponse, cleanup func(), err error) {
	err = req.validate(ai)
	if err != nil {
		return nil, nil, err
	}

	var seriesQueries map[lodInfo]int
	if opt.testPromql {
		seriesQueries = make(map[lodInfo]int)
	}
	if opt.timeNow.IsZero() {
		opt.timeNow = time.Now()
	}

	metricMeta, err := h.getMetricMeta(ai, req.metricWithNamespace)
	if err != nil {
		return nil, nil, err
	}

	err = validateQuery(metricMeta, req.version)
	if err != nil {
		return nil, nil, err
	}

	queries, err := parseQueries(req.version, req.what, req.by, req.maxHost)
	if err != nil {
		return nil, nil, err
	}

	mappedFilterIn, err := h.resolveFilter(metricMeta, req.version, req.filterIn)
	if err != nil {
		return nil, nil, err
	}
	mappedFilterNotIn, err := h.resolveFilter(metricMeta, req.version, req.filterNotIn)
	if err != nil {
		return nil, nil, err
	}

	if len(req.shifts) == 0 {
		req.shifts = []time.Duration{0}
	} else {
		sort.Slice(req.shifts, func(i, j int) bool { return req.shifts[i] < req.shifts[j] })
		if req.shifts[len(req.shifts)-1] != 0 {
			req.shifts = append(req.shifts, 0)
		}
	}
	oldestShift := req.shifts[0]
	isStringTop := metricMeta.StringTopDescription != ""

	isUnique := false // this parameter has meaning only for the version 1, in other cases it does nothing
	if req.version == Version1 {
		isUnique = queries[0].whatKind == queryFnKindUnique // we always have only one query for version 1
	}

	lods := selectQueryLODs(
		req.version,
		int64(metricMeta.PreKeyFrom),
		metricMeta.PreKeyOnly,
		metricMeta.Resolution,
		isUnique,
		isStringTop,
		opt.timeNow.Unix(),
		shiftTimestamp(req.from.Unix(), int64(req.width), toSec(oldestShift), h.location),
		shiftTimestamp(req.to.Unix(), int64(req.width), toSec(oldestShift), h.location),
		h.utcOffset,
		req.width,
		req.widthKind,
		h.location,
	)

	if len(lods) > 0 {
		// left shift leftmost LOD by one step to facilitate calculation of derivative (if any) in the leftmost requested point
		// NB! don't forget to exclude this extra point on the left on successful return
		lods[0].fromSec -= lods[0].stepSec

		// ensure that we can right-shift the oldest LOD to cover other shifts
		if req.width != _1M {
			step := lods[0].stepSec
			for _, shift := range req.shifts[1:] {
				shiftDelta := toSec(shift - oldestShift)
				if shiftDelta%step != 0 {
					return nil, nil, httpErr(http.StatusBadRequest, fmt.Errorf("invalid time shift sequence %v (shift %v not divisible by %v)", req.shifts, shift, time.Duration(step)*time.Second))
				}
			}
		}
	}

	lodTimes := make([][]int64, 0, len(lods))
	allTimes := make([]int64, 0)
	for _, lod := range lods {
		times := lod.generateTimePoints(toSec(oldestShift))
		lodTimes = append(lodTimes, times)
		allTimes = append(allTimes, times...)
	}

	var (
		// non-nil to ensure that we don't send them as JSON nulls
		meta = make([]QuerySeriesMetaV2, 0)
		data = make([]*[]float64, 0)
		// buffer drawn from the sync.Pool to store response data (will be returned inside `freeQueryResp` if `handleGetQuery` succeeds)
		syncPoolBuffers = make([]*[]float64, 0)
		freeQueryResp   = func() {
			for _, s := range syncPoolBuffers {
				h.putFloatsSlice(s)
			}
		}
	)
	defer func() {
		if err != nil {
			freeQueryResp()
		}
	}()

	var sqlQueries []string
	if opt.debugQueries {
		ctx = debugQueriesContext(ctx, &sqlQueries)
	}

	for _, q := range queries {
		qs := normalizedQueryString(req.metricWithNamespace, q.whatKind, req.by, req.filterIn, req.filterNotIn, false)
		pq := &preparedPointsQuery{
			user:        ai.user,
			version:     req.version,
			metricID:    metricMeta.MetricID,
			preKeyTagID: metricMeta.PreKeyTagID,
			isStringTop: isStringTop,
			kind:        q.whatKind,
			by:          q.by,
			filterIn:    mappedFilterIn,
			filterNotIn: mappedFilterNotIn,
		}

		desiredStepMul := int64(1)
		if req.widthKind == widthLODRes {
			desiredStepMul = int64(req.width)
		}

		for _, shift := range req.shifts {
			type selectRowsPtr *[]*tsSelectRow
			var ( // initialized to suppress Goland's invalid "may be nil" warnings
				tagsToIx      = map[tsTags]int{}           // tags => index
				ixToTags      = make([]*tsTags, 0)         // index => tags
				ixToLodToRows = make([][]selectRowsPtr, 0) // index => ("lod index" => all rows, ordered by time)
				ixToAmount    = make([]float64, 0)         // index => total "amount"
			)

			shiftDelta := toSec(shift - oldestShift)
			for lodIx, lod := range lods {
				if opt.testPromql {
					seriesQueries[lodInfo{
						fromSec:    shiftTimestamp(lod.fromSec, lod.stepSec, shiftDelta, lod.location),
						toSec:      shiftTimestamp(lod.toSec, lod.stepSec, shiftDelta, lod.location),
						stepSec:    lod.stepSec,
						table:      lod.table,
						hasPreKey:  lod.hasPreKey,
						preKeyOnly: lod.preKeyOnly,
						location:   h.location}]++
				}
				m, err := h.cache.Get(ctx, req.version, qs, pq, lodInfo{
					fromSec:    shiftTimestamp(lod.fromSec, lod.stepSec, shiftDelta, lod.location),
					toSec:      shiftTimestamp(lod.toSec, lod.stepSec, shiftDelta, lod.location),
					stepSec:    lod.stepSec,
					table:      lod.table,
					hasPreKey:  lod.hasPreKey,
					preKeyOnly: lod.preKeyOnly,
					location:   h.location,
				}, req.avoidCache)
				if err != nil {
					return nil, nil, err
				}
				reqRaw := req.widthKind == widthAutoRes || lod.stepSec == _1M
				for _, rows := range m {
					for i := range rows {
						ix, ok := tagsToIx[rows[i].tsTags]
						if !ok {
							ix = len(ixToTags)
							tagsToIx[rows[i].tsTags] = ix
							ixToTags = append(ixToTags, &rows[i].tsTags)
							ixToLodToRows = append(ixToLodToRows, make([]selectRowsPtr, len(lods)))
							ixToAmount = append(ixToAmount, 0)
						}
						if ixToLodToRows[ix][lodIx] == nil {
							ixToLodToRows[ix][lodIx] = h.getRowsSlice()
						}
						*ixToLodToRows[ix][lodIx] = append(*ixToLodToRows[ix][lodIx], &rows[i])
						v := math.Abs(selectTSValue(q.what, req.maxHost, reqRaw, desiredStepMul, &rows[i]))
						if q.what.isCumul() {
							ixToAmount[ix] += v
						} else {
							ixToAmount[ix] += v * v * float64(lod.stepSec)
						}
					}
				}
			}

			sortedIxs := make([]int, 0, len(ixToAmount))
			for i := range ixToAmount {
				sortedIxs = append(sortedIxs, i)
			}

			if req.numResults > 0 {
				util.PartialSortIndexByValueDesc(sortedIxs, ixToAmount, req.numResults, opt.rand, nil)
				if len(sortedIxs) > req.numResults {
					sortedIxs = sortedIxs[:req.numResults]
				}
			} else if req.numResults < 0 {
				req.numResults = -req.numResults
				util.PartialSortIndexByValueAsc(sortedIxs, ixToAmount, req.numResults, opt.rand, nil)
				if len(sortedIxs) > req.numResults {
					sortedIxs = sortedIxs[:req.numResults]
				}
			}

			for _, i := range sortedIxs {
				tags := ixToTags[i]
				kvs := make(map[string]SeriesMetaTag, 16)
				for j := 0; j < format.MaxTags; j++ {
					h.maybeAddQuerySeriesTagValue(kvs, metricMeta, req.version, q.by, j, tags.tag[j])
				}
				maybeAddQuerySeriesTagValueString(kvs, q.by, &tags.tagStr)

				ts := h.getFloatsSlice(len(allTimes))
				syncPoolBuffers = append(syncPoolBuffers, ts)

				var maxHosts []string
				if (req.maxHost || q.what == queryFnMaxHost || q.what == queryFnMaxCountHost) && req.version == Version2 {
					maxHosts = make([]string, len(*ts))
				}
				for i := range *ts {
					(*ts)[i] = math.NaN() // will become JSON null
				}
				base := 0
				for lodIx, rows := range ixToLodToRows[i] {
					if rows != nil {
						lod := lods[lodIx]
						reqRaw := lod.stepSec == _1M || req.widthKind == widthAutoRes
						for _, row := range *rows {
							lodTimeIx := lod.getIndexForTimestamp(row.time, shiftDelta)
							(*ts)[base+lodTimeIx] = selectTSValue(q.what, req.maxHost, reqRaw, desiredStepMul, row)
							if maxHosts != nil && row.maxHost != 0 {
								// mapping every time is not optimal, but mapping to store in cache is also not optimal. TODO - optimize?
								label, err := h.getTagValue(row.maxHost)
								if err != nil {
									label = format.CodeTagValue(row.maxHost)
								}
								maxHosts[base+lodTimeIx] = label
							}
						}
					}
					base += len(lodTimes[lodIx])
				}

				if q.what.isCumul() {
					// starts from 1 to exclude extra point on the left
					accumulateSeries((*ts)[1:])
				} else if q.what.isDerivative() {
					// Extra point on the left was needed for this case
					differentiateSeries(*ts)

				}

				// exclude extra point on the left from final slice
				s := (*ts)[1:]
				if maxHosts != nil {
					maxHosts = maxHosts[1:]
				}

				meta = append(meta, QuerySeriesMetaV2{
					TimeShift: toSec(shift),
					Tags:      kvs,
					MaxHosts:  maxHosts,
					Name:      req.metricWithNamespace,
					What:      q.what,
					Total:     len(tagsToIx),
				})
				data = append(data, &s)
			}

			for _, lodToRows := range ixToLodToRows {
				for i, s := range lodToRows {
					if s != nil {
						h.putRowsSlice(s)
						lodToRows[i] = nil
					}
				}
			}
		}
	}
	if len(allTimes) > 0 {
		allTimes = allTimes[1:] // exclude extra point on the left
	}
	resp = &SeriesResponse{
		Series: querySeries{
			Time:       allTimes,
			SeriesMeta: meta,
			SeriesData: data,
		},
		DebugQueries: sqlQueries,
		MetricMeta:   metricMeta,
		immutable:    req.to.Before(opt.timeNow.Add(invalidateFrom)),
		queries:      seriesQueries,
	}
	if h.verbose && opt.stat != nil {
		log.Printf("[debug] handled query (%v series x %v points each) for %q in %v", len(resp.Series.SeriesMeta), len(resp.Series.Time), ai.user, time.Since(opt.stat.startTime))
	}
	return resp, freeQueryResp, nil
}

func (h *Handler) HandleGetPoint(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointPoint, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), r.FormValue(paramDataFormat))
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()

	req, _, err := h.parseHTTPRequest(r)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if req.version == "" {
		req.version = "2"
	}
	options := seriesRequestOptions{
		debugQueries: true,
		stat:         sl,
	}

	resp, immutable, err := h.handleGetPoint(ctx, ai, options, req)

	switch {
	case err == nil && r.FormValue(paramDataFormat) == dataFormatCSV:
		respondJSON(w, resp, 0, 0, httpErr(http.StatusBadRequest, nil), h.verbose, ai.user, sl)
	default:
		cache, cacheStale := queryClientCacheDuration(immutable)
		respondJSON(w, resp, cache, cacheStale, err, h.verbose, ai.user, sl)
	}
}

func (h *Handler) handleGetPoint(ctx context.Context, ai accessInfo, opt seriesRequestOptions, req seriesRequest) (resp *GetPointResp, immutable bool, err error) {
	err = req.validate(ai)
	if err != nil {
		return nil, false, err
	}

	metricMeta, err := h.getMetricMeta(ai, req.metricWithNamespace)
	if err != nil {
		return nil, false, err
	}

	err = validateQuery(metricMeta, req.version)
	if err != nil {
		return nil, false, err
	}

	queries, err := parseQueries(req.version, req.what, req.by, req.maxHost)
	if err != nil {
		return nil, false, err
	}

	for _, q := range queries {
		if !validateQueryPoint(q) {
			return nil, false, fmt.Errorf("function %s isn't supported", q.what.String())
		}
	}

	mappedFilterIn, err := h.resolveFilter(metricMeta, req.version, req.filterIn)
	if err != nil {
		return nil, false, err
	}
	mappedFilterNotIn, err := h.resolveFilter(metricMeta, req.version, req.filterNotIn)
	if err != nil {
		return nil, false, err
	}

	if len(req.shifts) == 0 {
		req.shifts = []time.Duration{0}
	}
	oldestShift := req.shifts[0]
	isStringTop := metricMeta.StringTopDescription != ""

	isUnique := false // this parameter has meaning only for the version 1, in other cases it does nothing
	if req.version == Version1 {
		isUnique = queries[0].whatKind == queryFnKindUnique // we always have only one query for version 1
	}

	var (
		now = time.Now()
		r   *rand.Rand
	)

	var (
		// non-nil to ensure that we don't send them as JSON nulls
		meta = make([]QueryPointsMeta, 0)
		data = make([]float64, 0)
	)

	var sqlQueries []string
	if opt.debugQueries {
		ctx = debugQueriesContext(ctx, &sqlQueries)
	}

	for _, q := range queries {
		queryKey := normalizedQueryString(req.metricWithNamespace, q.whatKind, req.by, req.filterIn, req.filterNotIn, false)
		pq := &preparedPointsQuery{
			user:        ai.user,
			version:     req.version,
			metricID:    metricMeta.MetricID,
			preKeyTagID: metricMeta.PreKeyTagID,
			isStringTop: isStringTop,
			kind:        q.whatKind,
			by:          q.by,
			filterIn:    mappedFilterIn,
			filterNotIn: mappedFilterNotIn,
		}

		qp := selectQueryPoint(
			req.version,
			int64(metricMeta.PreKeyFrom),
			metricMeta.PreKeyOnly,
			metricMeta.Resolution,
			isUnique,
			isStringTop,
			now.Unix(),
			shiftTimestamp(req.from.Unix(), -1, toSec(oldestShift), h.location),
			shiftTimestamp(req.to.Unix(), -1, toSec(oldestShift), h.location),
			h.utcOffset,
			req.expandToLODBoundary,
			h.location,
		)

		for _, shift := range req.shifts {
			var (
				tagsToIx   = map[tsTags]int{}        // tags => index
				ixToTags   = make([]*tsTags, 0)      // index => tags
				ixToAmount = make([]float64, 0)      // index => total "amount"
				ixToRow    = make([][]pSelectRow, 0) // index => row
			)
			shiftDelta := toSec(shift - oldestShift)
			realFromSec := shiftTimestamp(qp.fromSec, -1, shiftDelta, qp.location)
			realToSec := shiftTimestamp(qp.toSec, -1, shiftDelta, qp.location)
			pqs, err := h.pointsCache.get(ctx, queryKey, pq, pointQuery{
				fromSec:   realFromSec,
				toSec:     realToSec,
				table:     qp.table,
				hasPreKey: qp.hasPreKey,
				location:  qp.location,
			}, req.avoidCache)
			if err != nil {
				return nil, false, err
			}
			for i := range pqs {
				ix, ok := tagsToIx[pqs[i].tsTags]
				if !ok {
					ix = len(ixToTags)
					tagsToIx[pqs[i].tsTags] = ix
					ixToTags = append(ixToTags, &pqs[i].tsTags)
					ixToAmount = append(ixToAmount, 0)
					ixToRow = append(ixToRow, nil)
				}
				v := math.Abs(selectPointValue(q.what, req.maxHost, &pqs[i]))
				ixToAmount[ix] += v * v
				ixToRow[ix] = append(ixToRow[ix], pqs[i])
			}

			sortedIxs := make([]int, 0, len(ixToAmount))
			for i := range ixToAmount {
				sortedIxs = append(sortedIxs, i)
			}

			if req.numResults > 0 {
				util.PartialSortIndexByValueDesc(sortedIxs, ixToAmount, req.numResults, r, nil)
				if len(sortedIxs) > req.numResults {
					sortedIxs = sortedIxs[:req.numResults]
				}
			} else if req.numResults < 0 {
				req.numResults = -req.numResults
				util.PartialSortIndexByValueAsc(sortedIxs, ixToAmount, req.numResults, r, nil)
				if len(sortedIxs) > req.numResults {
					sortedIxs = sortedIxs[:req.numResults]
				}
			}

			for _, ix := range sortedIxs {
				tags := ixToTags[ix]
				kvs := make(map[string]SeriesMetaTag, 16)
				for j := 0; j < format.MaxTags; j++ {
					h.maybeAddQuerySeriesTagValue(kvs, metricMeta, req.version, q.by, j, tags.tag[j])
				}
				maybeAddQuerySeriesTagValueString(kvs, q.by, &tags.tagStr)

				maxHost := ""
				showMaxHost := false
				if (req.maxHost || q.what == queryFnMaxHost || q.what == queryFnMaxCountHost) && req.version == Version2 {
					showMaxHost = true
				}

				rows := ixToRow[ix]
				row := rows[0]
				value := selectPointValue(q.what, req.maxHost, &row)
				if showMaxHost && row.maxHost != 0 {
					// mapping every time is not optimal, but mapping to store in cache is also not optimal. TODO - optimize?
					label, err := h.getTagValue(row.maxHost)
					if err != nil {
						label = format.CodeTagValue(row.maxHost)
					}
					maxHost = label
				}

				meta = append(meta, QueryPointsMeta{
					TimeShift: toSec(shift),
					Tags:      kvs,
					MaxHost:   maxHost,
					Name:      req.metricWithNamespace,
					What:      q.what,
					FromSec:   realFromSec,
					ToSec:     realToSec,
				})
				data = append(data, value)
			}
		}
	}

	immutable = req.to.Before(time.Now().Add(invalidateFrom))
	resp = &GetPointResp{
		PointMeta:    meta,
		PointData:    data,
		DebugQueries: sqlQueries,
	}
	return resp, immutable, nil
}

func (h *Handler) HandleGetRender(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointRender, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), r.FormValue(paramDataFormat))
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()

	s, vars, err := h.parseHTTPRequestS(r, 12)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}

	resp, immutable, err := h.handleGetRender(
		ctx, ai,
		renderRequest{
			seriesRequest: s,
			vars:          vars,
			renderWidth:   r.FormValue(paramRenderWidth),
			renderFormat:  r.FormValue(paramDataFormat),
		})
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}

	cache, cacheStale := queryClientCacheDuration(immutable)
	respondPlot(w, resp.format, resp.data, cache, cacheStale, h.verbose, ai.user, sl)
}

func HandleGetEntity[T any](w http.ResponseWriter, r *http.Request, h *Handler, endpointName string, handle func(ai accessInfo, id int32) (T, time.Duration, error)) {
	sl := newEndpointStat(endpointName, r.Method, 0, "")
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}
	idStr := r.FormValue(ParamID)
	id, err := strconv.ParseInt(idStr, 10, 32)
	if err != nil {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, err), h.verbose, ai.user, sl)
		return
	}
	resp, cache, err := handle(ai, int32(id))
	respondJSON(w, resp, cache, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandleGetDashboard(w http.ResponseWriter, r *http.Request) {
	HandleGetEntity(w, r, h, EndpointDashboard, h.handleGetDashboard)
}

func (h *Handler) HandleGetGroup(w http.ResponseWriter, r *http.Request) {
	HandleGetEntity(w, r, h, EndpointGroup, h.handleGetGroup)
}

func (h *Handler) HandleGetNamespace(w http.ResponseWriter, r *http.Request) {
	HandleGetEntity(w, r, h, EndpointNamespace, h.handleGetNamespace)
}

func HandleGetEntityList[T any](w http.ResponseWriter, r *http.Request, h *Handler, endpointName string, handle func(ai accessInfo) (T, time.Duration, error)) {
	sl := newEndpointStat(endpointName, r.Method, 0, "")
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}
	resp, cache, err := handle(ai)
	respondJSON(w, resp, cache, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandleGetGroupsList(w http.ResponseWriter, r *http.Request) {
	HandleGetEntityList(w, r, h, EndpointGroup, h.handleGetGroupsList)
}

func (h *Handler) HandleGetDashboardList(w http.ResponseWriter, r *http.Request) {
	HandleGetEntityList(w, r, h, EndpointDashboard, h.handleGetDashboardList)
}

func (h *Handler) HandleGetNamespaceList(w http.ResponseWriter, r *http.Request) {
	HandleGetEntityList(w, r, h, EndpointNamespace, h.handleGetNamespaceList)
}

func (h *Handler) HandlePutPostDashboard(w http.ResponseWriter, r *http.Request) {
	var dashboard DashboardInfo
	handlePostEntity(h, w, r, EndpointDashboard, &dashboard, func(ctx context.Context, ai accessInfo, entity *DashboardInfo, create bool) (resp interface{}, versionToWait int64, err error) {
		response, err := h.handlePostDashboard(ctx, ai, entity.Dashboard, create, entity.Delete)
		if err != nil {
			return nil, 0, err
		}
		return response, response.Dashboard.Version, nil
	})
}

func (h *Handler) handleGetRender(ctx context.Context, ai accessInfo, req renderRequest) (*renderResponse, bool, error) {
	width, err := parseRenderWidth(req.renderWidth)
	if err != nil {
		return nil, false, err
	}

	format_, err := parseRenderFormat(req.renderFormat)
	if err != nil {
		return nil, false, err
	}

	var (
		s         = make([]*SeriesResponse, len(req.seriesRequest))
		immutable = true
		seriesNum = 0
		pointsNum = 0
	)
	for i, r := range req.seriesRequest {
		var (
			cancel func()
			data   *SeriesResponse
			err    error
			start  = time.Now()
		)
		data, cancel, err = h.handlePromqlQuery(ctx, ai, r, seriesRequestOptions{
			vars: req.vars,
			metricNameCallback: func(s string) {
				req.seriesRequest[i].metricWithNamespace = s
			},
		})
		if err != nil {
			return nil, false, err
		}
		defer cancel() // hold until plot call
		immutable = immutable && data.immutable
		if h.verbose {
			log.Printf("[debug] handled render query (%v series x %v points each) for %q in %v", len(data.Series.SeriesMeta), len(data.Series.Time), ai.user, time.Since(start))
			seriesNum += len(data.Series.SeriesMeta)
			pointsNum += len(data.Series.SeriesMeta) * len(data.Series.Time)
		}
		h.colorize(data)
		s[i] = data
	}

	ctx, cancel := context.WithTimeout(ctx, plotRenderTimeout)
	defer cancel()

	err = h.plotRenderSem.Acquire(ctx, 1)
	if err != nil {
		return nil, false, err
	}
	defer h.plotRenderSem.Release(1)

	start := time.Now()
	png, err := plot(ctx, format_, true, s, h.utcOffset, req.seriesRequest, width, h.plotTemplate)
	if err != nil {
		return nil, false, err
	}
	if h.verbose {
		log.Printf("[debug] handled render plot (%v series, %v points) for %q in %v", seriesNum, pointsNum, req.ai.user, time.Since(start))
	}

	return &renderResponse{
		format: format_,
		data:   png,
	}, immutable, nil
}

func (h *Handler) handleGetTable(ctx context.Context, ai accessInfo, debugQueries bool, req tableRequest, opt seriesRequestOptions) (resp *GetTableResp, immutable bool, err error) {
	version, err := parseVersion(req.version)
	if err != nil {
		return nil, false, err
	}

	metricMeta, err := h.getMetricMeta(ai, req.metricWithNamespace)
	if err != nil {
		return nil, false, err
	}

	err = validateQuery(metricMeta, version)
	if err != nil {
		return nil, false, err
	}

	from, to, err := parseFromToRows(req.from, req.to, req.fromRow, req.toRow)
	if err != nil {
		return nil, false, err
	}

	width, widthKind, err := parseWidth(req.width, req.widthAgg)
	if err != nil {
		return nil, false, err
	}

	queries, err := parseQueries(version, req.what, req.by, req.maxHost)
	if err != nil {
		return nil, false, err
	}
	mappedFilterIn, err := h.resolveFilter(metricMeta, version, req.filterIn)
	if err != nil {
		return nil, false, err
	}
	mappedFilterNotIn, err := h.resolveFilter(metricMeta, version, req.filterNotIn)
	if err != nil {
		return nil, false, err
	}

	isStringTop := metricMeta.StringTopDescription != ""

	isUnique := false // this parameter has meaning only for the version 1, in other cases it does nothing
	if version == Version1 {
		isUnique = queries[0].whatKind == queryFnKindUnique // we always have only one query for version 1
	}

	lods := selectQueryLODs(
		version,
		int64(metricMeta.PreKeyFrom),
		metricMeta.PreKeyOnly,
		metricMeta.Resolution,
		isUnique,
		isStringTop,
		time.Now().Unix(),
		shiftTimestamp(from.Unix(), int64(width), 0, h.location),
		shiftTimestamp(to.Unix(), int64(width), 0, h.location),
		h.utcOffset,
		width,
		widthKind,
		h.location,
	)
	desiredStepMul := int64(1)
	if widthKind == widthLODRes {
		desiredStepMul = int64(width)
	}

	if req.fromEnd {
		for i := 0; i < len(lods)/2; i++ {
			temp := lods[i]
			j := len(lods) - i - 1
			lods[i] = lods[j]
			lods[j] = temp
		}
	}

	var sqlQueries []string
	if debugQueries {
		ctx = debugQueriesContext(ctx, &sqlQueries)
	}
	what := make([]queryFn, 0, len(queries))
	for _, q := range queries {
		what = append(what, q.what)
	}
	queryRows, hasMore, err := getTableFromLODs(ctx, lods, tableReqParams{
		req:               req,
		queries:           queries,
		user:              ai.user,
		metricMeta:        metricMeta,
		isStringTop:       isStringTop,
		mappedFilterIn:    mappedFilterIn,
		mappedFilterNotIn: mappedFilterNotIn,
		rawValue:          widthKind == widthAutoRes || width == _1M,
		desiredStepMul:    desiredStepMul,
		location:          h.location,
	}, h.cache.Get, h.maybeAddQuerySeriesTagValue)
	if err != nil {
		return nil, false, err
	}
	var firstRowStr, lastRowStr string
	if len(queryRows) > 0 {
		lastRowStr, err = encodeFromRows(&queryRows[len(queryRows)-1].rowRepr)
		if err != nil {
			return nil, false, err
		}
		firstRowStr, err = encodeFromRows(&queryRows[0].rowRepr)
		if err != nil {
			return nil, false, err
		}
	}
	immutable = to.Before(time.Now().Add(invalidateFrom))
	return &GetTableResp{
		Rows:         queryRows,
		What:         what,
		FromRow:      firstRowStr,
		ToRow:        lastRowStr,
		More:         hasMore,
		DebugQueries: sqlQueries,
	}, immutable, nil
}

func (h *Handler) getRowsSlice() *[]*tsSelectRow {
	v := h.pointRowsPool.Get()
	if v == nil {
		s := make([]*tsSelectRow, 0, maxSlice)
		v = &s
	}
	return v.(*[]*tsSelectRow)
}

func (h *Handler) putRowsSlice(s *[]*tsSelectRow) {
	for i := range *s {
		(*s)[i] = nil // help GC
	}
	*s = (*s)[:0]

	if cap(*s) <= maxSlice {
		h.pointRowsPool.Put(s)
	}
}

func getDashboardMetaInfo(d *format.DashboardMeta) DashboardMetaInfo {
	data := map[string]interface{}{}
	var description string
	for k, v := range d.JSONData {
		if k == descriptionFieldName {
			description, _ = v.(string)
		} else {
			data[k] = v
		}
	}
	return DashboardMetaInfo{
		DashboardID: d.DashboardID,
		Name:        d.Name,
		Version:     d.Version,
		UpdateTime:  d.UpdateTime,
		DeletedTime: d.DeleteTime,
		Description: description,
		JSONData:    data,
	}
}

func (h *Handler) getFloatsSlice(n int) *[]float64 {
	if n > maxSlice {
		s := make([]float64, n)
		return &s // should not happen: we should never return more than maxSlice points
	}

	v := h.pointFloatsPool.Get()
	if v == nil {
		s := make([]float64, 0, maxSlice)
		v = &s
	}
	ret := v.(*[]float64)
	*ret = (*ret)[:n]

	return ret
}

func (h *Handler) putFloatsSlice(s *[]float64) {
	*s = (*s)[:0]

	if cap(*s) <= maxSlice {
		h.pointFloatsPool.Put(s)
	}
}

func accumulateSeries(s []float64) {
	acc := 0.0
	for i, v := range s {
		if !math.IsNaN(v) {
			acc += v
		}
		s[i] = acc
	}
}

func differentiateSeries(s []float64) {
	prev := math.NaN()
	for i, v := range s {
		s[i] = v - prev
		prev = v
	}
}

func (h *Handler) maybeAddQuerySeriesTagValue(m map[string]SeriesMetaTag, metricMeta *format.MetricMetaValue, version string, by []string, tagIndex int, tagValueID int32) bool {
	tagID := format.TagID(tagIndex)
	if !containsString(by, tagID) {
		return false
	}
	metaTag := SeriesMetaTag{Value: h.getRichTagValue(metricMeta, version, tagID, tagValueID)}
	if tag, ok := metricMeta.Name2Tag[tagID]; ok {
		metaTag.Comment = tag.ValueComments[metaTag.Value]
		metaTag.Raw = tag.Raw
		metaTag.RawKind = tag.RawKind
	}
	m[format.TagIDLegacy(tagIndex)] = metaTag
	return true
}

type pointsSelectCols struct {
	time      proto.ColInt64
	step      proto.ColInt64
	cnt       proto.ColFloat64
	val       []proto.ColFloat64
	tag       []proto.ColInt32
	tagIx     []int
	tagStr    proto.ColStr
	maxHostV1 proto.ColUInt8
	maxHostV2 proto.ColInt32
	shardNum  proto.ColUInt32
	res       proto.Results
}

func newPointsSelectCols(meta pointsQueryMeta, useTime bool) *pointsSelectCols {
	// NB! Keep columns selection order and names is sync with sql.go code
	c := &pointsSelectCols{
		val:   make([]proto.ColFloat64, meta.vals),
		tag:   make([]proto.ColInt32, 0, len(meta.tags)),
		tagIx: make([]int, 0, len(meta.tags)),
	}
	if useTime {
		c.res = proto.Results{
			{Name: "_time", Data: &c.time},
			{Name: "_stepSec", Data: &c.step},
		}
	}
	for _, id := range meta.tags {
		switch id {
		case format.StringTopTagID:
			c.res = append(c.res, proto.ResultColumn{Name: "key_s", Data: &c.tagStr})
		case format.ShardTagID:
			c.res = append(c.res, proto.ResultColumn{Name: "key_shard_num", Data: &c.shardNum})
		default:
			c.tag = append(c.tag, proto.ColInt32{})
			c.res = append(c.res, proto.ResultColumn{Name: "key" + id, Data: &c.tag[len(c.tag)-1]})
			c.tagIx = append(c.tagIx, format.TagIndex(id))
		}
	}
	c.res = append(c.res, proto.ResultColumn{Name: "_count", Data: &c.cnt})
	for i := 0; i < meta.vals; i++ {
		c.res = append(c.res, proto.ResultColumn{Name: "_val" + strconv.Itoa(i), Data: &c.val[i]})
	}
	if meta.maxHost {
		if meta.version == Version1 {
			c.res = append(c.res, proto.ResultColumn{Name: "_maxHost", Data: &c.maxHostV1})
		} else {
			c.res = append(c.res, proto.ResultColumn{Name: "_maxHost", Data: &c.maxHostV2})
		}
	}
	return c
}

func (c *pointsSelectCols) rowAt(i int) tsSelectRow {
	row := tsSelectRow{
		time:     c.time[i],
		stepSec:  c.step[i],
		tsValues: tsValues{countNorm: c.cnt[i]},
	}
	for j := 0; j < len(c.val); j++ {
		row.val[j] = c.val[j][i]
	}
	for j := range c.tag {
		row.tag[c.tagIx[j]] = c.tag[j][i]
	}
	if c.tagStr.Pos != nil && i < len(c.tagStr.Pos) {
		copy(row.tagStr[:], c.tagStr.Buf[c.tagStr.Pos[i].Start:c.tagStr.Pos[i].End])
	}
	if len(c.maxHostV2) != 0 {
		row.maxHost = c.maxHostV2[i]
	} else if len(c.maxHostV1) != 0 {
		row.maxHost = int32(c.maxHostV1[i])
	}
	if c.shardNum != nil {
		row.shardNum = c.shardNum[i]
	}
	return row
}

func (c *pointsSelectCols) rowAtPoint(i int) pSelectRow {
	row := pSelectRow{
		tsValues: tsValues{countNorm: c.cnt[i]},
	}
	for j := 0; j < len(c.val); j++ {
		row.val[j] = c.val[j][i]
	}
	for j := range c.tag {
		row.tag[c.tagIx[j]] = c.tag[j][i]
	}
	if c.tagStr.Pos != nil && i < len(c.tagStr.Pos) {
		copy(row.tagStr[:], c.tagStr.Buf[c.tagStr.Pos[i].Start:c.tagStr.Pos[i].End])
	}
	if len(c.maxHostV2) != 0 {
		row.maxHost = c.maxHostV2[i]
	} else if len(c.maxHostV1) != 0 {
		row.maxHost = int32(c.maxHostV1[i])
	}
	return row
}

func maybeAddQuerySeriesTagValueString(m map[string]SeriesMetaTag, by []string, tagValuePtr *stringFixed) string {
	tagValue := skeyFromFixedString(tagValuePtr)

	if containsString(by, format.StringTopTagID) {
		m[format.LegacyStringTopTagID] = SeriesMetaTag{Value: emptyToUnspecified(tagValue)}
		return tagValue
	}
	return ""
}

func skeyFromFixedString(tagValuePtr *stringFixed) string {
	tagValue := ""
	nullIx := bytes.IndexByte(tagValuePtr[:], 0)
	switch nullIx {
	case 0: // do nothing
	case -1:
		tagValue = string(tagValuePtr[:])
	default:
		tagValue = string(tagValuePtr[:nullIx])
	}
	return tagValue
}
func replaceInfNan(v *float64) {
	if math.IsNaN(*v) {
		*v = -1.111111 // Motivation - 99.9% of our graphs are >=0, -1.111111 will stand out. But we do not expect NaNs.
		return
	}
	if math.IsInf(*v, 1) {
		*v = -2.222222 // Motivation - as above, distinct value for debug
		return
	}
	if math.IsInf(*v, -1) {
		*v = -3.333333 // Motivation - as above, distinct value for debug
		return
	}
	// Motivation - we store some values as float32 anyway. Also, most code does not work well, if close to float64 limits
	if *v > math.MaxFloat32 {
		*v = math.MaxFloat32
		return
	}
	if *v < -math.MaxFloat32 {
		*v = -math.MaxFloat32
		return
	}
}

func (h *Handler) loadPoints(ctx context.Context, pq *preparedPointsQuery, lod lodInfo, ret [][]tsSelectRow, retStartIx int) (int, error) {
	query, args, err := loadPointsQuery(pq, lod, h.utcOffset)
	if err != nil {
		return 0, err
	}

	rows := 0
	cols := newPointsSelectCols(args, true)
	isFast := lod.isFast()
	isLight := pq.isLight()
	metric := pq.metricID
	table := lod.table
	kind := pq.kind
	start := time.Now()
	err = h.doSelect(ctx, util.QueryMetaInto{
		IsFast:  isFast,
		IsLight: isLight,
		User:    pq.user,
		Metric:  metric,
		Table:   table,
		Kind:    string(kind),
	}, pq.version, ch.Query{
		Body:   query,
		Result: cols.res,
		OnResult: func(_ context.Context, block proto.Block) (err error) {
			defer func() { // process crashes if we do not catch the "panic"
				if p := recover(); p != nil {
					err = fmt.Errorf("doSelect: %v", p)
				}
			}()
			for i := 0; i < block.Rows; i++ {
				if !isTimestampValid(cols.time[i], lod.stepSec, h.utcOffset, h.location) {
					log.Printf("[warning] got invalid timestamp while loading for %q, ignoring: %d is not a multiple of %v", pq.user, cols.time[i], lod.stepSec)
					continue
				}
				replaceInfNan(&cols.cnt[i])
				for j := 0; j < len(cols.val); j++ {
					replaceInfNan(&cols.val[j][i])
				}
				row := cols.rowAt(i)
				ix := retStartIx + lod.getIndexForTimestamp(row.time, 0)
				ret[ix] = append(ret[ix], row)
			}
			rows += block.Rows
			return nil
		}})
	duration := time.Since(start)
	if err != nil {
		return 0, err
	}

	if rows == maxSeriesRows {
		return rows, errTooManyRows // prevent cache being populated by incomplete data
	}
	if h.verbose {
		log.Printf("[debug] loaded %v rows from %v (%v timestamps, %v to %v step %v) for %q in %v",
			rows,
			lod.table,
			(lod.toSec-lod.fromSec)/lod.stepSec,
			time.Unix(lod.fromSec, 0),
			time.Unix(lod.toSec, 0),
			time.Duration(lod.stepSec)*time.Second,
			pq.user,
			duration,
		)
	}

	return rows, nil
}

func (h *Handler) loadPoint(ctx context.Context, pq *preparedPointsQuery, pointQuery pointQuery) ([]pSelectRow, error) {
	query, args, err := loadPointQuery(pq, pointQuery, h.utcOffset)
	if err != nil {
		return nil, err
	}
	ret := make([]pSelectRow, 0)
	rows := 0
	cols := newPointsSelectCols(args, false)
	isFast := pointQuery.isFast()
	isLight := pq.isLight()
	metric := pq.metricID
	table := pointQuery.table
	kind := pq.kind
	err = h.doSelect(ctx, util.QueryMetaInto{
		IsFast:  isFast,
		IsLight: isLight,
		User:    pq.user,
		Metric:  metric,
		Table:   table,
		Kind:    string(kind),
	}, pq.version, ch.Query{
		Body:   query,
		Result: cols.res,
		OnResult: func(_ context.Context, block proto.Block) (err error) {
			defer func() { // process crashes if we do not catch the "panic"
				if p := recover(); p != nil {
					err = fmt.Errorf("doSelect: %v", p)
				}
			}()
			for i := 0; i < block.Rows; i++ {
				//todo check
				replaceInfNan(&cols.cnt[i])
				for j := 0; j < len(cols.val); j++ {
					replaceInfNan(&cols.val[j][i])
				}
				row := cols.rowAtPoint(i)
				ret = append(ret, row)
			}
			rows += block.Rows
			return nil
		}})
	if err != nil {
		return nil, err
	}

	if rows == maxSeriesRows {
		return ret, errTooManyRows // prevent cache being populated by incomplete data
	}
	if h.verbose {
		log.Printf("[debug] loaded %v rows from %v (%v to %v) for %q in",
			rows,
			pointQuery.table,
			time.Unix(pointQuery.fromSec, 0),
			time.Unix(pointQuery.toSec, 0),
			pq.user,
		)
	}

	return ret, nil
}

func stableMulDiv(v float64, mul int64, div int64) float64 {
	// Often desiredStepMul is multiple of row.StepSec
	if mul%div == 0 {
		// so we make FP desiredStepMul by row.StepSec division first which often gives us whole number, even 1 in many cases
		return v * float64(mul/div)
	}
	// if we do multiplication first, (a * 360) might lose mantissa bits so next division by 360 will lose precision
	// hopefully 2x divisions on this code path will not slow us down too much.
	return v * float64(mul) / float64(div)
}

func selectTSValue(what queryFn, maxHost bool, raw bool, desiredStepMul int64, row *tsSelectRow) float64 {
	switch what {
	case queryFnCount, queryFnMaxCountHost, queryFnDerivativeCount:
		if raw {
			return row.countNorm
		}
		return stableMulDiv(row.countNorm, desiredStepMul, row.stepSec)
	case queryFnCountNorm, queryFnDerivativeCountNorm:
		return row.countNorm / float64(row.stepSec)
	case queryFnCumulCount:
		return row.countNorm
	case queryFnCardinality:
		if maxHost {
			if raw {
				return row.val[5]
			}
			return stableMulDiv(row.val[5], desiredStepMul, row.stepSec)
		}
		if raw {
			return row.val[0]
		}
		return stableMulDiv(row.val[0], desiredStepMul, row.stepSec)
	case queryFnCardinalityNorm:
		if maxHost {
			return row.val[5] / float64(row.stepSec)
		}
		return row.val[0] / float64(row.stepSec)
	case queryFnCumulCardinality:
		if maxHost {
			return row.val[5]
		}
		return row.val[0]
	case queryFnMin, queryFnDerivativeMin:
		return row.val[0]
	case queryFnMax, queryFnMaxHost, queryFnDerivativeMax:
		return row.val[1]
	case queryFnAvg, queryFnCumulAvg, queryFnDerivativeAvg:
		return row.val[2]
	case queryFnSum, queryFnDerivativeSum:
		if raw {
			return row.val[3]
		}
		return stableMulDiv(row.val[3], desiredStepMul, row.stepSec)
	case queryFnSumNorm, queryFnDerivativeSumNorm:
		return row.val[3] / float64(row.stepSec)
	case queryFnCumulSum:
		return row.val[3]
	case queryFnStddev:
		return row.val[4]
	case queryFnStdvar:
		return row.val[4] * row.val[4]
	case queryFnP0_1:
		return row.val[0]
	case queryFnP1:
		return row.val[1]
	case queryFnP5:
		return row.val[2]
	case queryFnP10:
		return row.val[3]
	case queryFnP25:
		return row.val[0]
	case queryFnP50:
		return row.val[1]
	case queryFnP75:
		return row.val[2]
	case queryFnP90:
		return row.val[3]
	case queryFnP95:
		return row.val[4]
	case queryFnP99:
		return row.val[5]
	case queryFnP999:
		return row.val[6]
	case queryFnUnique, queryFnDerivativeUnique:
		if raw {
			return row.val[0]
		}
		return stableMulDiv(row.val[0], desiredStepMul, row.stepSec)
	case queryFnUniqueNorm, queryFnDerivativeUniqueNorm:
		return row.val[0] / float64(row.stepSec)
	default:
		return math.NaN()
	}
}

func selectPointValue(what queryFn, maxHost bool, row *pSelectRow) float64 {
	switch what {
	case queryFnCount, queryFnMaxCountHost:
		return row.countNorm
	case queryFnMin:
		return row.val[0]
	case queryFnMax, queryFnMaxHost:
		return row.val[1]
	case queryFnAvg:
		return row.val[2]
	case queryFnSum:
		return row.val[3]
	case queryFnP0_1:
		return row.val[0]
	case queryFnP1:
		return row.val[1]
	case queryFnP5:
		return row.val[2]
	case queryFnP10:
		return row.val[3]
	case queryFnP25:
		return row.val[0]
	case queryFnP50:
		return row.val[1]
	case queryFnP75:
		return row.val[2]
	case queryFnP90:
		return row.val[3]
	case queryFnP95:
		return row.val[4]
	case queryFnP99:
		return row.val[5]
	case queryFnP999:
		return row.val[6]
	case queryFnUnique:
		return row.val[0]
	default:
		return math.NaN()
	}
}

func toSec(d time.Duration) int64 {
	return int64(d / time.Second)
}

func containsString(s []string, v string) bool {
	for _, sv := range s {
		if sv == v {
			return true
		}
	}
	return false
}

func emptyToUnspecified(s string) string {
	if s == "" {
		return format.CodeTagValue(format.TagValueIDUnspecified)
	}
	return s
}

func unspecifiedToEmpty(s string) string {
	if s == format.CodeTagValue(format.TagValueIDUnspecified) {
		return ""
	}
	return s
}

func (h *Handler) checkReadOnlyMode(w http.ResponseWriter, r *http.Request) (readOnlyMode bool) {
	if h.readOnly {
		w.WriteHeader(406)
		_, _ = w.Write([]byte("readonly mode"))
		return true
	}
	return false
}

func (h *Handler) waitVersionUpdate(ctx context.Context, version int64) error {
	ctx, cancel := context.WithTimeout(ctx, journalUpdateTimeout)
	defer cancel()
	return h.metricsStorage.Journal().WaitVersion(ctx, version)
}

func queryClientCacheDuration(immutable bool) (cache time.Duration, cacheStale time.Duration) {
	if immutable {
		return queryClientCacheImmutable, queryClientCacheStaleImmutable
	}
	return queryClientCache, queryClientCacheStale
}

func lessThan(l RowMarker, r tsSelectRow, skey string, orEq bool) bool {
	if l.Time != r.time {
		return l.Time < r.time
	}
	for i := range l.Tags {
		lv := l.Tags[i].Value
		rv := r.tag[l.Tags[i].Index]
		if lv != rv {
			return lv < rv
		}
	}
	if orEq {
		return l.SKey <= skey
	}
	return l.SKey < skey
}

func rowMarkerLessThan(l, r RowMarker) bool {
	if l.Time != r.Time {
		return l.Time < r.Time
	}
	if len(l.Tags) != len(r.Tags) {
		return len(l.Tags) < len(r.Tags)
	}
	for i := range l.Tags {
		lv := l.Tags[i].Value
		rv := r.Tags[i].Value
		if lv != rv {
			return lv < rv
		}
	}

	return l.SKey < r.SKey
}

func getQueryRespEqual(a, b *SeriesResponse) bool {
	if len(a.Series.Time) != len(b.Series.Time) {
		return false
	}
	if len(a.Series.SeriesMeta) != len(b.Series.SeriesMeta) {
		return false
	}
	if len(a.Series.SeriesData) != len(b.Series.SeriesData) {
		return false
	}
	for i := 0; i < len(a.Series.Time); i++ {
		if a.Series.Time[i] != b.Series.Time[i] {
			return false
		}
	}
	for i := 0; i < len(a.Series.SeriesData); i++ {
		var j int
		for ; j < len(b.Series.SeriesMeta); j++ {
			if reflect.DeepEqual(a.Series.SeriesMeta[i], b.Series.SeriesMeta[j]) {
				break
			}
		}
		if j == len(b.Series.SeriesMeta) {
			return false
		}
		if len(*a.Series.SeriesData[i]) != len(*b.Series.SeriesData[j]) {
			return false
		}
		for k := 0; k < len(*a.Series.SeriesData[i]); k++ {
			var (
				v1 = (*a.Series.SeriesData[i])[k]
				v2 = (*b.Series.SeriesData[j])[k]
			)
			if math.IsNaN(v1) && math.IsNaN(v2) {
				continue
			}
			if !(math.Abs(v1-v2) <= math.Max(math.Abs(v1), math.Abs(v2))/100) {
				// difference is more than a percent!
				// or one value is NaN
				return false
			}
		}
	}
	return true
}

func (h *Handler) parseHTTPRequest(r *http.Request) (seriesRequest, map[string]promql.Variable, error) {
	res, vars, err := h.parseHTTPRequestS(r, 1)
	if err != nil {
		return seriesRequest{}, nil, err
	}
	if len(res) == 0 {
		return seriesRequest{}, nil, httpErr(http.StatusBadRequest, fmt.Errorf("request is empty"))
	}
	return res[0], vars, nil
}

func (h *Handler) parseHTTPRequestS(r *http.Request, maxTabs int) (res []seriesRequest, env map[string]promql.Variable, err error) {
	defer func() {
		var dummy httpError
		if err != nil && !errors.As(err, &dummy) {
			err = httpErr(http.StatusBadRequest, err)
		}
	}()
	type seriesRequestEx struct {
		seriesRequest
		strFrom       string
		strTo         string
		strWidth      string
		strWidthAgg   string
		strNumResults string
		strType       string
	}
	var (
		dash  DashboardData
		first = func(s []string) string {
			if len(s) != 0 {
				return s[0]
			}
			return ""
		}
		tabs  = make([]seriesRequestEx, 1, maxTabs)
		tabX  = -1
		tab0  = &tabs[0]
		tabAt = func(i int) *seriesRequestEx {
			if i >= cap(tabs) {
				return nil
			}
			for j := len(tabs) - 1; j < i; j++ {
				tabs = append(tabs, seriesRequestEx{seriesRequest: seriesRequest{
					version:   Version2,
					width:     1,
					widthKind: widthLODRes,
				}})
			}
			return &tabs[i]
		}
	)
	// parse dashboard
	if id, err := strconv.Atoi(first(r.Form[paramDashboardID])); err == nil {
		var v *format.DashboardMeta
		if v = format.BuiltinDashboardByID[int32(id)]; v == nil {
			v = h.metricsStorage.GetDashboardMeta(int32(id))
		}
		if v != nil {
			// Ugly, but there is no other way because "metricsStorage" stores partially parsed dashboard!
			// TODO: either fully parse and validate dashboard JSON or store JSON string blindly.
			if bs, err := json.Marshal(v.JSONData); err == nil {
				easyjson.Unmarshal(bs, &dash)
			}
		}
	}
	var n int
	for i, v := range dash.Plots {
		tab := tabAt(i)
		if tab == nil {
			continue
		}
		if v.UseV2 {
			tab.version = Version2
		} else {
			tab.version = Version1
		}
		tab.numResults = v.NumSeries
		tab.metricWithNamespace = v.MetricName
		if v.Width > 0 {
			tab.strWidth = fmt.Sprintf("%ds", v.Width)
		}
		if v.Width > 0 {
			tab.width = v.Width
		} else {
			tab.width = 1
		}
		tab.widthKind = widthLODRes
		tab.promQL = v.PromQL
		tab.what = v.What
		tab.strType = strconv.Itoa(v.Type)
		for _, v := range v.GroupBy {
			if tid, err := parseTagID(v); err == nil {
				tab.by = append(tab.by, tid)
			}
		}
		if len(v.FilterIn) != 0 {
			tab.filterIn = make(map[string][]string)
			for k, v := range v.FilterIn {
				if tid, err := parseTagID(k); err == nil {
					tab.filterIn[tid] = v
				}
			}
		}
		if len(v.FilterNotIn) != 0 {
			tab.filterNotIn = make(map[string][]string)
			for k, v := range v.FilterNotIn {
				if tid, err := parseTagID(k); err == nil {
					tab.filterNotIn[tid] = v
				}
			}
		}
		tab.maxHost = v.MaxHost
		n++
	}
	env = make(map[string]promql.Variable)
	for _, v := range dash.Vars {
		env[v.Name] = promql.Variable{
			Value:  v.Vals,
			Group:  v.Args.Group,
			Negate: v.Args.Negate,
		}
		for _, link := range v.Link {
			if len(link) != 2 {
				continue
			}
			tabX := link[0]
			if tabX < 0 || len(tabs) <= tabX {
				continue
			}
			var (
				tagX  = link[1]
				tagID string
			)
			if tagX < 0 {
				tagID = format.StringTopTagID
			} else if 0 <= tagX && tagX < format.MaxTags {
				tagID = format.TagID(tagX)
			} else {
				continue
			}
			tab := &tabs[tabX]
			if v.Args.Group {
				tab.by = append(tab.by, tagID)
			}
			if tab.filterIn != nil {
				delete(tab.filterIn, tagID)
			}
			if tab.filterNotIn != nil {
				delete(tab.filterNotIn, tagID)
			}
			if len(v.Vals) != 0 {
				if v.Args.Negate {
					if tab.filterNotIn == nil {
						tab.filterNotIn = make(map[string][]string)
					}
					tab.filterNotIn[tagID] = v.Vals
				} else {
					if tab.filterIn == nil {
						tab.filterIn = make(map[string][]string)
					}
					tab.filterIn[tagID] = v.Vals
				}
			}
		}
	}
	if n != 0 {
		switch dash.TimeRange.To {
		case "ed": // end of day
			year, month, day := time.Now().In(h.location).Date()
			tab0.to = time.Date(year, month, day, 0, 0, 0, 0, h.location).Add(24 * time.Hour).UTC()
		case "ew": // end of week
			var (
				year, month, day = time.Now().In(h.location).Date()
				dateNow          = time.Date(year, month, day, 0, 0, 0, 0, h.location)
				offset           = time.Duration(((time.Sunday - dateNow.Weekday() + 7) % 7) + 1)
			)
			tab0.to = dateNow.Add(offset * 24 * time.Hour).UTC()
		default:
			if n, err := strconv.ParseInt(dash.TimeRange.To, 10, 64); err == nil {
				if to, err := parseUnixTimeTo(n); err == nil {
					tab0.to = to
				}
			}
		}
		if from, err := parseUnixTimeFrom(dash.TimeRange.From, tab0.to); err == nil {
			tab0.from = from
		}
		tab0.shifts, _ = parseTimeShifts(dash.TimeShifts)
	}
	// parse URL
	_ = r.ParseForm() // (*http.Request).FormValue ignores parse errors
	type (
		dashboardVar struct {
			name string
			link [][]int
		}
		dashboardVarM struct {
			val    []string
			group  string
			negate string
		}
	)
	var (
		parseTabX = func(s string) (int, error) {
			var i int
			if i, err = strconv.Atoi(s); err != nil {
				return 0, fmt.Errorf("invalid tab index %q", s)
			}
			return i, nil
		}
		vars  []dashboardVar
		varM  = make(map[string]*dashboardVarM)
		varAt = func(i int) *dashboardVar {
			for j := len(vars) - 1; j < i; j++ {
				vars = append(vars, dashboardVar{})
			}
			return &vars[i]
		}
		varByName = func(s string) (v *dashboardVarM) {
			if v = varM[s]; v == nil {
				v = &dashboardVarM{}
				varM[s] = v
			}
			return v
		}
	)
	for k, v := range r.Form {
		var i int
		if strings.HasPrefix(k, "t") {
			var dotX int
			if dotX = strings.Index(k, "."); dotX != -1 {
				var j int
				if j, err = parseTabX(k[1:dotX]); err == nil && j > 0 {
					i = j
					k = k[dotX+1:]
				}
			}
		} else if len(k) > 1 && k[0] == 'v' { // variables, not version
			var dotX int
			if dotX = strings.Index(k, "."); dotX != -1 {
				switch dotX {
				case 1: // e.g. "v.environment.g=1"
					s := strings.Split(k[dotX+1:], ".")
					switch len(s) {
					case 1:
						vv := varByName(s[0])
						vv.val = append(vv.val, v...)
					case 2:
						switch s[1] {
						case "g":
							varByName(s[0]).group = first(v)
						case "nv":
							varByName(s[0]).negate = first(v)
						}
					}
				default: // e.g. "v0.n=environment" or "v0.l=0.0-1.0"
					if varX, err := strconv.Atoi(k[1:dotX]); err == nil {
						vv := varAt(varX)
						switch k[dotX+1:] {
						case "n":
							vv.name = first(v)
						case "l":
							for _, s1 := range strings.Split(first(v), "-") {
								links := make([]int, 0, 2)
								for _, s2 := range strings.Split(s1, ".") {
									if n, err := strconv.Atoi(s2); err == nil {
										links = append(links, n)
									} else {
										break
									}
								}
								if len(links) == 2 {
									vv.link = append(vv.link, links)
								}
							}
						}
					}
				}
			}
			continue
		}
		t := tabAt(i)
		if t == nil {
			continue
		}
		switch k {
		case paramTabNumber:
			tabX, err = parseTabX(first(v))
		case ParamAvoidCache:
			t.avoidCache = true
		case ParamFromTime:
			t.strFrom = first(v)
		case ParamMetric:
			const formerBuiltin = "__builtin_" // we renamed builtin metrics, removing prefix
			name := first(v)
			if strings.HasPrefix(name, formerBuiltin) {
				name = "__" + name[len(formerBuiltin):]
			}
			t.metricWithNamespace = name
		case ParamNumResults:
			t.strNumResults = first(v)
		case ParamQueryBy:
			for _, s := range v {
				var tid string
				tid, err = parseTagID(s)
				if err != nil {
					return nil, nil, err
				}
				t.by = append(t.by, tid)
			}
		case ParamQueryFilter:
			t.filterIn, t.filterNotIn, err = parseQueryFilter(v)
		case ParamQueryVerbose:
			t.verbose = first(v) == "1"
		case ParamQueryWhat:
			t.what = v
		case ParamTimeShift:
			t.shifts, err = parseTimeShifts(v)
		case ParamToTime:
			t.strTo = first(v)
		case ParamVersion:
			s := first(v)
			switch s {
			case Version1, Version2:
				t.version = s
			default:
				return nil, nil, fmt.Errorf("invalid version: %q", s)
			}
		case ParamWidth:
			t.strWidth = first(v)
		case ParamWidthAgg:
			t.strWidthAgg = first(v)
		case paramMaxHost:
			t.maxHost = true
		case paramPromQuery:
			t.promQL = first(v)
		case paramDataFormat:
			t.format = first(v)
		case paramQueryType:
			t.strType = first(v)
		case paramExcessPoints:
			t.expandToLODBoundary = true
		}
		if err != nil {
			return nil, nil, err
		}
	}
	if len(tabs) == 0 {
		return nil, nil, nil
	}
	for _, v := range vars {
		vv := varM[v.name]
		if vv == nil {
			continue
		}
		env[v.name] = promql.Variable{
			Value:  vv.val,
			Group:  vv.group == "1",
			Negate: vv.negate == "1",
		}
		for _, link := range v.link {
			if len(link) != 2 {
				continue
			}
			tabX := link[0]
			if tabX < 0 || len(tabs) <= tabX {
				continue
			}
			var (
				tagX  = link[1]
				tagID string
			)
			if tagX < 0 {
				tagID = format.StringTopTagID
			} else if 0 <= tagX && tagX < format.MaxTags {
				tagID = format.TagID(tagX)
			} else {
				continue
			}
			tab := &tabs[tabX]
			if vv.group == "1" {
				tab.by = append(tab.by, tagID)
			}
			if tab.filterIn != nil {
				delete(tab.filterIn, tagID)
			}
			if tab.filterNotIn != nil {
				delete(tab.filterNotIn, tagID)
			}
			if len(vv.val) != 0 {
				if vv.negate == "1" {
					if tab.filterNotIn == nil {
						tab.filterNotIn = make(map[string][]string)
					}
					tab.filterNotIn[tagID] = vv.val
				} else {
					if tab.filterIn == nil {
						tab.filterIn = make(map[string][]string)
					}
					tab.filterIn[tagID] = vv.val
				}
			}
		}
	}
	// parse dependent paramemeters
	var (
		finalize = func(t *seriesRequestEx) error {
			numResultsMax := maxSeries
			if len(t.shifts) != 0 {
				numResultsMax /= len(t.shifts)
			}
			t.numResults, err = parseNumResults(t.strNumResults, defSeries, numResultsMax)
			if err != nil {
				return err
			}
			if _, ok := format.BuiltinMetricByName[t.metricWithNamespace]; ok {
				t.verbose = false
			}
			if len(t.strWidth) != 0 || len(t.strWidthAgg) != 0 {
				t.width, t.widthKind, err = parseWidth(t.strWidth, t.strWidthAgg)
				if err != nil {
					return err
				}
			}
			return nil
		}
	)
	if len(tab0.strFrom) != 0 || len(tab0.strTo) != 0 {
		tab0.from, tab0.to, err = parseFromTo(tab0.strFrom, tab0.strTo)
		if err != nil {
			return nil, nil, err
		}
	}
	err = finalize(tab0)
	if err != nil {
		return nil, nil, err
	}
	for i := range tabs[1:] {
		t := &tabs[i+1]
		t.from = tab0.from
		t.to = tab0.to
		err = finalize(t)
		if err != nil {
			return nil, nil, err
		}
	}
	// build resulting slice
	if tabX != -1 {
		if tabs[tabX].strType == "1" {
			return nil, nil, nil
		}
		return []seriesRequest{tabs[tabX].seriesRequest}, env, nil
	}
	res = make([]seriesRequest, 0, len(tabs))
	for _, t := range tabs {
		if t.strType == "1" {
			continue
		}
		if len(t.metricWithNamespace) != 0 || len(t.promQL) != 0 {
			res = append(res, t.seriesRequest)
		}
	}
	return res, env, nil
}

func (r *DashboardTimeRange) UnmarshalJSON(bs []byte) error {
	var m map[string]any
	if err := json.Unmarshal(bs, &m); err != nil {
		return err
	}
	if from, ok := m["from"].(float64); ok {
		r.From = int64(from)
	}
	switch to := m["to"].(type) {
	case float64:
		r.To = strconv.Itoa(int(to))
	case string:
		r.To = to
	}
	return nil
}

func (r *DashboardTimeShifts) UnmarshalJSON(bs []byte) error {
	var s []any
	if err := json.Unmarshal(bs, &s); err != nil {
		return err
	}
	for _, v := range s {
		if n, ok := v.(float64); ok {
			*r = append(*r, strconv.Itoa(int(n)))
		}
	}
	return nil
}

func (r *seriesRequest) validate(ai accessInfo) error {
	if r.avoidCache && !ai.isAdmin() {
		return httpErr(404, fmt.Errorf(""))
	}
	if r.width == _1M {
		for _, v := range r.shifts {
			if (v/time.Second)%_1M != 0 {
				return httpErr(http.StatusBadRequest, fmt.Errorf("time shift %v can't be used with month interval", v))
			}
		}
	}
	return nil
}

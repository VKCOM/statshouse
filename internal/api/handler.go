// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"math"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	ttemplate "text/template"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/vkcom/statshouse-go"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/mailru/easyjson"
	_ "github.com/mailru/easyjson/gen" // https://github.com/mailru/easyjson/issues/293

	"github.com/vkcom/statshouse/internal/aggregator"
	"github.com/vkcom/statshouse/internal/config"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
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
	ParamVersion       = "v"
	ParamNumResults    = "n"
	ParamMetric        = "s"
	ParamID            = "id"
	ParamEntityVersion = "ver"
	ParamNamespace     = "namespace"

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
	paramShowDisabled = "sd"
	paramPriority     = "priority"
	paramCompat       = "compat"
	paramYL, paramYH  = "yl", "yh" // Y scale range

	Version1       = "1"
	Version2       = "2"
	Version3       = "3" // new tables format with stags
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
		AdminDash                int                 `json:"admin_dash"`
	}

	Handler struct {
		HandlerOptions
		showInvisible         bool
		staticDir             http.FileSystem
		indexTemplate         *template.Template
		indexSettings         string
		ch                    map[string]*util.ClickHouse
		metricsStorage        *metajournal.MetricsStorage
		tagValueCache         *pcache.Cache
		tagValueIDCache       *pcache.Cache
		cache                 *tsCacheGroup
		pointsCache           *pointsCache
		pointFloatsPool       sync.Pool
		pointFloatsPoolSize   atomic.Int64
		cacheInvalidateTicker *time.Ticker
		cacheInvalidateStop   chan chan struct{}
		metadataLoader        *metajournal.MetricMetaLoader
		jwtHelper             *vkuth.JWTHelper
		plotRenderSem         *semaphore.Weighted
		plotTemplate          *ttemplate.Template
		rUsage                syscall.Rusage // accessed without lock by first shard addBuiltIns
		rmID                  int
		promEngine            promql.Engine
		bufferBytesAlloc      statshouse.MetricRef
		bufferBytesFree       statshouse.MetricRef
		bufferPoolBytesAlloc  statshouse.MetricRef
		bufferPoolBytesFree   statshouse.MetricRef
		bufferPoolBytesTotal  statshouse.MetricRef

		// Internal Server Errors
		errorsMu sync.RWMutex
		errors   [32]error500
		errorX   int
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
		Id      int32   `json:"id"`
		Name    string  `json:"name"`
		Weight  float64 `json:"weight"`
		Disable bool    `json:"disable"`
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
		CustomName  string              `json:"customName"`
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
		ai         accessInfo
		version    string
		numResults string
		metricName string
		tagID      string
		from       string
		to         string
		what       string
		filter     []string
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

	seriesRequest struct {
		ai               accessInfo
		version          string
		numResults       int
		metricName       string
		customMetricName string
		from             time.Time
		to               time.Time
		step             int64
		screenWidth      int64
		promQL           string
		shifts           []time.Duration
		what             []QueryFunc
		by               []string
		filterIn         map[string][]string
		filterNotIn      map[string][]string
		vars             map[string]promql.Variable
		maxHost          bool
		avoidCache       bool
		verbose          bool
		excessPoints     bool
		compat           bool // Prometheus compatibility mode
		format           string
		yl, yh           string // Y scale range

		// table query
		fromEnd bool
		fromRow RowMarker
		toRow   RowMarker
	}

	seriesRequestOptions struct {
		metricCallback func(*format.MetricMetaValue)
		rand           *rand.Rand
		timeNow        time.Time
		mode           data_model.QueryMode
		trace          bool
		strBucketLabel bool
	}

	//easyjson:json
	SeriesResponse struct {
		Series            querySeries             `json:"series"`
		SamplingFactorSrc float64                 `json:"sampling_factor_src"` // average
		SamplingFactorAgg float64                 `json:"sampling_factor_agg"` // average
		ReceiveErrors     float64                 `json:"receive_errors"`      // count/sec
		ReceiveWarnings   float64                 `json:"receive_warnings"`    // count/sec
		MappingErrors     float64                 `json:"mapping_errors"`      // count/sec
		PromQL            string                  `json:"promql"`              // equivalent PromQL query
		DebugQueries      []string                `json:"__debug_queries"`     // private, unstable: SQL queries executed
		ExcessPointLeft   bool                    `json:"excess_point_left"`
		ExcessPointRight  bool                    `json:"excess_point_right"`
		MetricMeta        *format.MetricMetaValue `json:"metric"`
		immutable         bool
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
		What         []QueryFunc     `json:"what"`
		FromRow      string          `json:"from_row"`
		ToRow        string          `json:"to_row"`
		More         bool            `json:"more"`
		DebugQueries []string        `json:"__debug_queries"` // private, unstable: SQL queries executed, can be null
	}

	renderRequest struct {
		ai            accessInfo
		seriesRequest []seriesRequest
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

	queryTableRows []queryTableRow

	QuerySeriesMeta struct {
		TimeShift int64             `json:"time_shift"`
		Tags      map[string]string `json:"tags"`
		MaxHosts  []string          `json:"max_hosts"` // max_host for now
		What      QueryFunc         `json:"what"`
	}

	QuerySeriesMetaV2 struct {
		TimeShift  int64                    `json:"time_shift"`
		Tags       map[string]SeriesMetaTag `json:"tags"`
		MaxHosts   []string                 `json:"max_hosts"` // max_host for now
		Name       string                   `json:"name"`
		Color      string                   `json:"color"`
		What       string                   `json:"what"`
		Total      int                      `json:"total"`
		MetricType string                   `json:"metric_type"`
	}

	QueryPointsMeta struct {
		TimeShift int64                    `json:"time_shift"`
		Tags      map[string]SeriesMetaTag `json:"tags"`
		MaxHost   string                   `json:"max_host"` // max_host for now
		Name      string                   `json:"name"`
		What      string                   `json:"what"`
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

	seriesResponse struct {
		*promql.TimeSeries
		metric          *format.MetricMetaValue
		promQL          string
		trace           []string
		extraPointLeft  bool
		extraPointRight bool
	}

	metadata struct {
		UserEmail string `json:"user_email"`
		UserName  string `json:"user_name"`
		UserRef   string `json:"user_ref"`
	}

	HistoryEvent struct {
		Version  int64    `json:"version"`
		Metadata metadata `json:"metadata"`
	}

	GetHistoryShortInfoResp struct {
		Events []HistoryEvent `json:"events"`
	}
)

var errTooManyRows = fmt.Errorf("can't fetch more than %v rows", maxSeriesRows)

func NewHandler(staticDir fs.FS, jsSettings JSSettings, showInvisible bool, chV1 *util.ClickHouse, chV2 *util.ClickHouse, metadataClient *tlmetadata.Client, diskCache *pcache.DiskCache, jwtHelper *vkuth.JWTHelper, opt HandlerOptions, cfg *Config) (*Handler, error) {
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
	cl := config.NewConfigListener(data_model.APIRemoteConfig, cfg)
	metricStorage := metajournal.MakeMetricsStorage(diskCacheSuffix, diskCache, nil, cl.ApplyEventCB)
	metricStorage.Journal().Start(nil, nil, metadataLoader.LoadJournal)
	h := &Handler{
		HandlerOptions: opt,
		showInvisible:  showInvisible,
		staticDir:      http.FS(staticDir),
		indexTemplate:  tmpl,
		indexSettings:  string(settings),
		metadataLoader: metadataLoader,
		ch: map[string]*util.ClickHouse{
			Version1: chV1,
			Version2: chV2,
			Version3: chV2,
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
		plotRenderSem:         semaphore.NewWeighted(maxConcurrentPlots),
		plotTemplate:          ttemplate.Must(ttemplate.New("").Parse(gnuplotTemplate)),
		bufferBytesAlloc:      statshouse.GetMetricRef(format.BuiltinMetricAPIBufferBytesAlloc, statshouse.Tags{1: srvfunc.HostnameForStatshouse(), 2: "2"}),
		bufferBytesFree:       statshouse.GetMetricRef(format.BuiltinMetricAPIBufferBytesFree, statshouse.Tags{1: srvfunc.HostnameForStatshouse(), 2: "2"}),
		bufferPoolBytesAlloc:  statshouse.GetMetricRef(format.BuiltinMetricAPIBufferBytesAlloc, statshouse.Tags{1: srvfunc.HostnameForStatshouse(), 2: "1"}),
		bufferPoolBytesFree:   statshouse.GetMetricRef(format.BuiltinMetricAPIBufferBytesFree, statshouse.Tags{1: srvfunc.HostnameForStatshouse(), 2: "1"}),
		bufferPoolBytesTotal:  statshouse.GetMetricRef(format.BuiltinMetricAPIBufferBytesTotal, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}),
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &h.rUsage)

	h.cache = newTSCacheGroup(cfg.ApproxCacheMaxSize, data_model.LODTables, h.utcOffset, h.loadPoints, cacheDefaultDropEvery)
	h.pointsCache = newPointsCache(cfg.ApproxCacheMaxSize, h.utcOffset, h.loadPoint, time.Now)
	cl.AddChangeCB(func(c config.Config) {
		cfg := c.(*Config)
		h.cache.changeMaxSize(cfg.ApproxCacheMaxSize)
	})
	go h.invalidateLoop()
	h.rmID = statshouse.StartRegularMeasurement(func(client *statshouse.Client) { // TODO - stop
		prevRUsage := h.rUsage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &h.rUsage)
		userTime := float64(h.rUsage.Utime.Nano()-prevRUsage.Utime.Nano()) / float64(time.Second)
		sysTime := float64(h.rUsage.Stime.Nano()-prevRUsage.Stime.Nano()) / float64(time.Second)

		userMetric := client.MetricRef(format.BuiltinMetricNameUsageCPU, statshouse.Tags{1: strconv.Itoa(format.TagValueIDComponentAPI), 2: strconv.Itoa(format.TagValueIDCPUUsageUser)})
		userMetric.Value(userTime)
		sysMetric := client.MetricRef(format.BuiltinMetricNameUsageCPU, statshouse.Tags{1: strconv.Itoa(format.TagValueIDComponentAPI), 2: strconv.Itoa(format.TagValueIDCPUUsageSys)})
		sysMetric.Value(sysTime)

		var vmSize, vmRSS float64
		if st, _ := srvfunc.GetMemStat(0); st != nil {
			vmSize = float64(st.Size)
			vmRSS = float64(st.Res)
		}
		client.Value(format.BuiltinMetricNameApiVmSize, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, vmSize)
		client.Value(format.BuiltinMetricNameApiVmRSS, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, vmRSS)
		client.Value(format.BuiltinMetricNameUsageMemory, statshouse.Tags{1: strconv.Itoa(format.TagValueIDComponentAPI)}, vmRSS)

		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		client.Value(format.BuiltinMetricNameApiHeapAlloc, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapAlloc))
		client.Value(format.BuiltinMetricNameApiHeapSys, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapSys))
		client.Value(format.BuiltinMetricNameApiHeapIdle, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapIdle))
		client.Value(format.BuiltinMetricNameApiHeapInuse, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapInuse))
		//-- TODO: remove when deployed
		client.Value("api_vm_size", statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, vmSize)
		client.Value("api_vm_rss", statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, vmRSS)
		client.Value("api_heap_alloc", statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapAlloc))
		client.Value("api_heap_sys", statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapSys))
		client.Value("api_heap_idle", statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapIdle))
		client.Value("api_heap_inuse", statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapInuse))
		//--

		writeActiveQuieries := func(ch *util.ClickHouse, versionTag string) {
			if ch != nil {
				fastLight := client.MetricRef(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneFastLight), 4: srvfunc.HostnameForStatshouse()})
				fastLight.Value(float64(ch.SemaphoreCountFastLight()))

				fastHeavy := client.MetricRef(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneFastHeavy), 4: srvfunc.HostnameForStatshouse()})
				fastHeavy.Value(float64(ch.SemaphoreCountFastHeavy()))

				slowLight := client.MetricRef(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneSlowLight), 4: srvfunc.HostnameForStatshouse()})
				slowLight.Value(float64(ch.SemaphoreCountSlowLight()))

				slowHeavy := client.MetricRef(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneSlowHeavy), 4: srvfunc.HostnameForStatshouse()})
				slowHeavy.Value(float64(ch.SemaphoreCountSlowHeavy()))

				slowHardware := client.MetricRef(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneFastHardware), 4: srvfunc.HostnameForStatshouse()})
				slowHardware.Value(float64(ch.SemaphoreCountFastHardware()))

				fastHardware := client.MetricRef(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneSlowHardware), 4: srvfunc.HostnameForStatshouse()})
				fastHardware.Value(float64(ch.SemaphoreCountSlowHardware()))
			}
		}
		writeActiveQuieries(chV1, "1")
		writeActiveQuieries(chV2, "2")
		if n := h.pointFloatsPoolSize.Load(); n != 0 {
			h.bufferPoolBytesTotal.Value(float64(n))
		}
	})
	h.promEngine = promql.NewEngine(h, h.location, h.utcOffset)
	return h, nil
}

func (h *Handler) Close() error {
	statshouse.StopRegularMeasurement(h.rmID)
	h.cache.shutdown()
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
				for lodLevel := range data_model.LODTables[Version2] {
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
	reportQueryKind(ctx, meta.IsFast, meta.IsLight, meta.IsHardware)
	info, err := h.ch[version].Select(ctx, meta, query)
	duration := time.Since(start)
	if h.verbose {
		log.Printf("[debug] SQL for %q done in %v, err: %v", meta.User, duration, err)
	}

	reportTiming(ctx, "ch-select", duration)
	ChSelectMetricDuration(info.Duration, meta.Metric, meta.User, meta.Table, meta.Kind, meta.IsFast, meta.IsLight, meta.IsHardware, err)
	ChSelectProfile(meta.IsFast, meta.IsLight, meta.IsHardware, info.Profile, err)

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

func (h *Handler) getMetricID(metricName string) (int32, error) {
	if metricName == format.CodeTagValue(format.TagValueIDUnspecified) {
		return format.TagValueIDUnspecified, nil
	}
	// metric ID is not considered private
	meta, err := h.getMetricMeta(accessInfo{insecureMode: true}, metricName)
	if err != nil {
		return 0, err
	}
	return meta.MetricID, nil
}

// getMetricMeta only checks view access
func (h *Handler) getMetricMeta(ai accessInfo, metricName string) (*format.MetricMetaValue, error) {
	if m, ok := format.BuiltinMetricByName[metricName]; ok {
		return m, nil
	}
	v := h.metricsStorage.GetMetaMetricByNameDelayed(metricName)
	if v == nil {
		return nil, httpErr(http.StatusNotFound, fmt.Errorf("metric %q not found", metricName))
	}
	if !ai.CanViewMetric(*v) { // We are OK with sharing this bit of information with clients
		return nil, httpErr(http.StatusForbidden, fmt.Errorf("metric %q forbidden", metricName))
	}
	return v, nil
}

func (h *HTTPRequestHandler) getMetricMeta(metricName string) (*format.MetricMetaValue, error) {
	return h.Handler.getMetricMeta(h.accessInfo, metricName)
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
func (h *Handler) getMetricIDForStat(metricName string) int32 {
	if m, ok := format.BuiltinMetricByName[metricName]; ok {
		return m.MetricID
	}
	v := h.metricsStorage.GetMetaMetricByNameDelayed(metricName)
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
	if tag.IsNamespace {
		if tagValueID != format.TagValueIDUnspecified {
			if meta := h.metricsStorage.GetNamespace(tagValueID); meta != nil {
				return meta.Name
			}
		}
		return format.CodeTagValue(tagValueID)
	}
	if tag.IsGroup {
		if tagValueID != format.TagValueIDUnspecified {
			if meta := h.metricsStorage.GetGroup(tagValueID); meta != nil {
				return meta.Name
			}
		}
		return format.CodeTagValue(tagValueID)
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
		return h.getMetricID(tagValue) // we don't consider metric ID to be private
	}
	if tag.IsNamespace {
		if tagValue == format.CodeTagValue(format.TagValueIDUnspecified) {
			return format.TagValueIDUnspecified, nil
		}
		if meta := h.metricsStorage.GetNamespaceByName(tagValue); meta != nil {
			return meta.ID, nil
		}
		return 0, httpErr(http.StatusNotFound, fmt.Errorf("namespace %q not found", tagValue))
	}
	if tag.IsGroup {
		if tagValue == format.CodeTagValue(format.TagValueIDUnspecified) {
			return format.TagValueIDUnspecified, nil
		}
		if meta := h.metricsStorage.GetGroupByName(tagValue); meta != nil {
			return meta.ID, nil
		}
		return 0, httpErr(http.StatusNotFound, fmt.Errorf("group %q not found", tagValue))
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

func (h *HTTPRequestHandler) getRichTagValueIDs(metricMeta *format.MetricMetaValue, version string, tagID string, tagValues []string) ([]int32, error) {
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
	str := r.FormValue(ParamMetric)
	ns := r.FormValue(ParamNamespace)
	return mergeMetricNamespace(ns, str)
}

func (h *HTTPRequestHandler) resolveFilter(metricMeta *format.MetricMetaValue, version string, f map[string][]string) (map[string][]interface{}, error) {
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

func (h *HTTPRequestHandler) resolveFilterV3(metricMeta *format.MetricMetaValue, f map[string][]string) (map[string][]maybeMappedTag, error) {
	m := make(map[string][]maybeMappedTag, len(f))
	for k, values := range f {
		if k == format.StringTopTagID {
			for _, val := range values {
				m[k] = append(m[k], maybeMappedTag{Value: unspecifiedToEmpty(val)})
			}
		} else {
			ids, err := h.getRichTagValueIDs(metricMeta, Version3, k, values)
			if err != nil {
				return nil, err
			}
			m[k] = []maybeMappedTag{}
			for i := range ids {
				m[k] = append(m[k], maybeMappedTag{values[i], ids[i]})
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

func HandleGetMetricsList(h *HTTPRequestHandler, r *http.Request) {
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	resp := &GetMetricsListResp{
		Metrics: []metricShortInfo{},
	}
	for _, m := range format.BuiltinMetrics {
		if !h.showInvisible && !m.Visible { // we have invisible builtin metrics
			continue
		}
		resp.Metrics = append(resp.Metrics, metricShortInfo{Name: m.Name})
	}
	for _, v := range h.metricsStorage.GetMetaMetricList(h.showInvisible) {
		if h.accessInfo.CanViewMetric(*v) {
			resp.Metrics = append(resp.Metrics, metricShortInfo{Name: v.Name})
		}
	}
	sort.Slice(resp.Metrics, func(i int, j int) bool { return resp.Metrics[i].Name < resp.Metrics[j].Name })
	respondJSON(h, resp, defaultCacheTTL, queryClientCacheStale, err)
}

func HandleGetMetric(h *HTTPRequestHandler, r *http.Request) {
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	resp, cache, err := h.handleGetMetric(r.Context(), formValueParamMetric(r), r.FormValue(ParamID), r.FormValue(ParamEntityVersion))
	respondJSON(h, resp, cache, 0, err) // we don't want clients to see stale metadata
}

func HandleGetPromConfig(h *HTTPRequestHandler, r *http.Request) {
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	if !h.accessInfo.isAdmin() {
		err = httpErr(http.StatusNotFound, fmt.Errorf("config is not found"))
		respondJSON(h, nil, 0, 0, err)
		return
	}
	s, err := aggregator.DeserializeScrapeConfig([]byte(h.metricsStorage.PromConfig().Data), h.metricsStorage)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	respondJSON(h, s, 0, 0, err)
}

func HandleGetPromConfigGenerated(h *HTTPRequestHandler, r *http.Request) {
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	if !h.accessInfo.isAdmin() {
		err = httpErr(http.StatusNotFound, fmt.Errorf("config is not found"))
		respondJSON(h, nil, 0, 0, err)
		return
	}
	s, err := aggregator.DeserializeScrapeConfig([]byte(h.metricsStorage.PromConfigGenerated().Data), nil)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	respondJSON(h, s, 0, 0, err)
}

func HandlePostMetric(h *HTTPRequestHandler, r *http.Request) {
	if h.checkReadOnlyMode(h, r) {
		return
	}
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxEntityHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	if len(res) >= maxEntityHTTPBodySize {
		respondJSON(h, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("metric body too big. Max size is %d bytes", maxEntityHTTPBodySize)))
		return
	}
	var metric MetricInfo
	if err := easyjson.Unmarshal(res, &metric); err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	m, err := h.handlePostMetric(r.Context(), h.accessInfo, formValueParamMetric(r), metric.Metric)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	err = h.waitVersionUpdate(r.Context(), m.Version)
	respondJSON(h, &MetricInfo{Metric: m}, defaultCacheTTL, 0, err)
}

func handlePostEntity[T easyjson.Unmarshaler](h *HTTPRequestHandler, r *http.Request, entity T, handleCallback func(ctx context.Context, ai accessInfo, entity T, create bool) (resp interface{}, versionToWait int64, err error)) {
	if h.checkReadOnlyMode(h, r) {
		return
	}
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxEntityHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	if len(res) >= maxEntityHTTPBodySize {
		respondJSON(h, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("entity body too big. Max size is %d bytes", maxEntityHTTPBodySize)))
		return
	}
	if err := easyjson.Unmarshal(res, entity); err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	d, version, err := handleCallback(r.Context(), h.accessInfo, entity, r.Method == http.MethodPut)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	err = h.waitVersionUpdate(r.Context(), version)
	respondJSON(h, d, defaultCacheTTL, 0, err)
}

func HandlePutPostGroup(h *HTTPRequestHandler, r *http.Request) {
	var groupInfo MetricsGroupInfo
	handlePostEntity(h, r, &groupInfo, func(ctx context.Context, ai accessInfo, entity *MetricsGroupInfo, create bool) (resp interface{}, versionToWait int64, err error) {
		response, err := h.handlePostGroup(ctx, ai, entity.Group, create)
		if err != nil {
			return nil, 0, err
		}
		return response, response.Group.Version, nil
	})
}

func HandlePostNamespace(h *HTTPRequestHandler, r *http.Request) {
	var namespaceInfo NamespaceInfo
	handlePostEntity(h, r, &namespaceInfo, func(ctx context.Context, ai accessInfo, entity *NamespaceInfo, create bool) (resp interface{}, versionToWait int64, err error) {
		response, err := h.handlePostNamespace(ctx, ai, entity.Namespace, create)
		if err != nil {
			return nil, 0, err
		}
		return response, response.Namespace.Version, nil
	})
}

func HandlePostResetFlood(h *HTTPRequestHandler, r *http.Request) {
	if h.checkReadOnlyMode(h, r) {
		return
	}
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	if !h.accessInfo.isAdmin() {
		err := httpErr(http.StatusForbidden, fmt.Errorf("admin access required"))
		respondJSON(h, nil, 0, 0, err)
		return
	}
	var limit int32
	if v := r.FormValue("limit"); v != "" {
		i, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			respondJSON(h, nil, 0, 0, httpErr(http.StatusBadRequest, err))
			return
		}
		limit = int32(i)
	}
	del, before, after, err := h.metadataLoader.ResetFlood(r.Context(), formValueParamMetric(r), limit)
	if err == nil && !del {
		err = fmt.Errorf("metric flood counter was empty (no flood)")
	}
	respondJSON(h, &struct {
		Before int32 `json:"before"`
		After  int32 `json:"after"`
	}{Before: before, After: after}, 0, 0, err)
}

func HandlePostPromConfig(h *HTTPRequestHandler, r *http.Request) {
	if h.checkReadOnlyMode(h, r) {
		return
	}
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	if !h.accessInfo.isAdmin() {
		err = httpErr(http.StatusNotFound, fmt.Errorf("config is not found"))
		respondJSON(h, nil, 0, 0, err)
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxPromConfigHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	if len(res) >= maxPromConfigHTTPBodySize {
		respondJSON(h, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("confog body too big. Max size is %d bytes", maxPromConfigHTTPBodySize)))
		return
	}
	_, err = aggregator.DeserializeScrapeConfig(res, h.metricsStorage)
	if err != nil {
		err = httpErr(http.StatusBadRequest, fmt.Errorf("invalid prometheus config syntax: %v", err))
		respondJSON(h, nil, 0, 0, err)
		return
	}
	event, err := h.metadataLoader.SaveScrapeConfig(r.Context(), h.metricsStorage.PromConfig().Version, string(res), h.accessInfo.toMetadata())
	if err != nil {
		err = fmt.Errorf("failed to save prometheus config: %w", err)
		respondJSON(h, nil, 0, 0, err)
		return
	}
	err = h.waitVersionUpdate(r.Context(), event.Version)
	respondJSON(h, struct {
		Version int64 `json:"version"`
	}{event.Version}, defaultCacheTTL, 0, err)
}

func HandlePostKnownTags(h *HTTPRequestHandler, r *http.Request) {
	if h.checkReadOnlyMode(h, r) {
		return
	}
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	if !h.accessInfo.isAdmin() {
		err = httpErr(http.StatusNotFound, fmt.Errorf("not found"))
		respondJSON(h, nil, 0, 0, err)
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxPromConfigHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	if len(res) >= maxPromConfigHTTPBodySize {
		respondJSON(h, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("config body too big. Max size is %d bytes", maxPromConfigHTTPBodySize)))
		return
	}
	_, err = aggregator.ParseKnownTags(res, h.metricsStorage)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	event, err := h.metadataLoader.SaveKnownTagsConfig(r.Context(), h.metricsStorage.KnownTags().Version, string(res))
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	err = h.waitVersionUpdate(r.Context(), event.Version)
	respondJSON(h, struct {
		Version int64 `json:"version"`
	}{event.Version}, defaultCacheTTL, 0, err)
}

func HandleGetKnownTags(h *HTTPRequestHandler, r *http.Request) {
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	if !h.accessInfo.isAdmin() {
		err = httpErr(http.StatusNotFound, fmt.Errorf("config is not found"))
		respondJSON(h, nil, 0, 0, err)
		return
	}
	var res map[string]aggregator.KnownTagsJSON
	err = json.Unmarshal([]byte(h.metricsStorage.KnownTags().Data), &res)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	respondJSON(h, res, 0, 0, err)
}

func HandleGetHistory(h *HTTPRequestHandler, r *http.Request) {
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	var id int64
	if idStr := r.FormValue(ParamID); idStr != "" {
		id, err = strconv.ParseInt(idStr, 10, 32)
		if err != nil {
			respondJSON(h, nil, 0, 0, httpErr(http.StatusBadRequest, err))
			return
		}
	} else {
		respondJSON(h, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("%s is must be set", ParamID)))
		return
	}
	hist, err := h.metadataLoader.GetShortHistory(r.Context(), id)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	resp := GetHistoryShortInfoResp{Events: make([]HistoryEvent, 0, len(hist.Events))}
	for _, h := range hist.Events {
		m := metadata{}
		_ = json.Unmarshal([]byte(h.Metadata), &m)
		resp.Events = append(resp.Events, HistoryEvent{
			Version:  h.Version,
			Metadata: m,
		})
	}
	respondJSON(h, resp, defaultCacheTTL, 0, err)
}

func (h *HTTPRequestHandler) handleGetMetric(ctx context.Context, metricName string, metricIDStr string, versionStr string) (*MetricInfo, time.Duration, error) {
	if metricIDStr != "" {
		metricID, err := strconv.ParseInt(metricIDStr, 10, 32)
		if err != nil {
			return nil, 0, fmt.Errorf("can't parse %s", metricIDStr)
		}
		if versionStr == "" {
			metricName = h.getMetricNameByID(int32(metricID))
			if metricName == "" {
				return nil, 0, fmt.Errorf("can't find metric %d", metricID)
			}
		} else {
			version, err := strconv.ParseInt(versionStr, 10, 64)
			if err != nil {
				return nil, 0, fmt.Errorf("can't parse %s", versionStr)
			}
			m, err := h.metadataLoader.GetMetric(ctx, metricID, version)
			if err != nil {
				return nil, 0, err
			}
			return &MetricInfo{
				Metric: m,
			}, defaultCacheTTL, nil
		}
	}
	v, err := h.getMetricMeta(metricName)
	if err != nil {
		return nil, 0, err
	}
	return &MetricInfo{
		Metric: *v,
	}, defaultCacheTTL, nil
}

func (h *Handler) handleGetDashboard(ctx context.Context, ai accessInfo, id int32, version int64) (*DashboardInfo, time.Duration, error) {
	if id < 0 {
		if dash, ok := format.BuiltinDashboardByID[id]; ok {
			return &DashboardInfo{Dashboard: getDashboardMetaInfo(dash)}, defaultCacheTTL, nil
		} else {
			return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("dashboard %d not found", id))
		}
	}
	var dash *format.DashboardMeta
	if version == 0 {
		dash = h.metricsStorage.GetDashboardMeta(id)
		if dash == nil {
			return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("dashboard %d not found", id))
		}
	} else {
		dashI, err := h.metadataLoader.GetDashboard(ctx, int64(id), version)
		if err != nil {
			return nil, 0, err
		}
		dash = &dashI
	}
	return &DashboardInfo{Dashboard: getDashboardMetaInfo(dash)}, defaultCacheTTL, nil
}

func (h *Handler) handleGetDashboardList(ai accessInfo, showInvisible bool) (*GetDashboardListResp, time.Duration, error) {
	dashs := h.metricsStorage.GetDashboardList(showInvisible)
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
	if ai.service {
		return &DashboardInfo{}, httpErr(http.StatusForbidden, fmt.Errorf("services mustn't create or modify dashboards")) // TODO unify according to SH access policy
	}
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
	}, create, delete, ai.toMetadata())
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

func (h *Handler) handleGetGroup(_ accessInfo, id int32) (*MetricsGroupInfo, time.Duration, error) {
	group, ok := h.metricsStorage.GetGroupWithMetricsList(id)
	if !ok {
		return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("group %d not found", id))
	}
	return &MetricsGroupInfo{Group: *group.Group, Metrics: group.Metrics}, defaultCacheTTL, nil
}

func (h *Handler) handleGetGroupsList(ai accessInfo, showInvisible bool) (*GetGroupListResp, time.Duration, error) {
	groups := h.metricsStorage.GetGroupsList(showInvisible)
	resp := &GetGroupListResp{}
	for _, group := range groups {
		resp.Groups = append(resp.Groups, groupShortInfo{
			Id:      group.ID,
			Name:    group.Name,
			Weight:  group.Weight,
			Disable: group.Disable,
		})
	}
	return resp, defaultCacheTTL, nil
}

func (h *Handler) handleGetNamespace(_ accessInfo, id int32) (*NamespaceInfo, time.Duration, error) {
	namespace := h.metricsStorage.GetNamespace(id)
	if namespace == nil {
		return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("namespace %d not found", id))
	}
	return &NamespaceInfo{Namespace: *namespace}, defaultCacheTTL, nil
}

func (h *Handler) handleGetNamespaceList(ai accessInfo, showInvisible bool) (*GetNamespaceListResp, time.Duration, error) {
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
	var err error
	if namespace.ID >= 0 {
		namespace, err = h.metadataLoader.SaveNamespace(ctx, namespace, create, ai.toMetadata())
	} else {
		n := h.metricsStorage.GetNamespace(namespace.ID)
		if n == nil {
			return &NamespaceInfo{}, httpErr(http.StatusNotFound, fmt.Errorf("namespace %d not found", namespace.ID))
		}
		create := n == format.BuiltInNamespaceDefault[namespace.ID]
		namespace, err = h.metadataLoader.SaveBuiltinNamespace(ctx, namespace, create)
	}

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
	var err error
	if group.ID >= 0 {
		group, err = h.metadataLoader.SaveMetricsGroup(ctx, group, create, ai.toMetadata())
	} else {
		group, err = h.metadataLoader.SaveBuiltInGroup(ctx, group)
	}
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
	err = h.waitVersionUpdate(ctx, group.Version)
	if err != nil {
		return &MetricsGroupInfo{}, err
	}
	info, _ := h.metricsStorage.GetGroupWithMetricsList(group.ID)
	return &MetricsGroupInfo{Group: group, Metrics: info.Metrics}, nil
}

// TODO - remove metric name from request
func (h *Handler) handlePostMetric(ctx context.Context, ai accessInfo, _ string, metric format.MetricMetaValue) (format.MetricMetaValue, error) {
	create := metric.MetricID == 0
	var resp format.MetricMetaValue
	var err error
	if metric.PreKeyOnly && (metric.PreKeyFrom == 0 || metric.PreKeyTagID == "") {
		return format.MetricMetaValue{}, httpErr(http.StatusBadRequest, fmt.Errorf("use prekey_only with non empty prekey_tag_id"))
	}
	if create {
		if !ai.CanEditMetric(true, metric, metric) {
			return format.MetricMetaValue{}, httpErr(http.StatusForbidden, fmt.Errorf("can't create metric %q", metric.Name))
		}
		resp, err = h.metadataLoader.SaveMetric(ctx, metric, ai.toMetadata())
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
		if !ai.CanEditMetric(false, *old, metric) {
			return format.MetricMetaValue{}, httpErr(http.StatusForbidden, fmt.Errorf("can't edit metric %q", old.Name))
		}
		if diffContainsRawTagChanges(*old, metric) {
			if isAdmin := ai.isAdmin() || ai.insecureMode; !isAdmin {
				return format.MetricMetaValue{}, httpErr(http.StatusForbidden,
					fmt.Errorf("raw tags can only be edited by administrators, please contact the support group"))
			}
		}
		resp, err = h.metadataLoader.SaveMetric(ctx, metric, ai.toMetadata())
		if err != nil {
			err = fmt.Errorf("error saving metric in sqllite: %w", err)
			log.Println(err.Error())
			return format.MetricMetaValue{}, fmt.Errorf("can't edit metric: %w", err)
		}
	}
	return resp, nil
}

func diffContainsRawTagChanges(old, new format.MetricMetaValue) bool {
	for i := 0; i < len(old.Tags) && i < len(new.Tags); i++ {
		if old.Tags[i].Raw != new.Tags[i].Raw {
			return true // edit
		}
	}
	for i := len(new.Tags); i < len(old.Tags); i++ {
		if old.Tags[i].Raw {
			return true // removal
		}
	}
	return false
}

func HandleGetMetricTagValues(h *HTTPRequestHandler, r *http.Request) {
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()

	_ = r.ParseForm() // (*http.Request).FormValue ignores parse errors, too
	resp, immutable, err := h.handleGetMetricTagValues(
		ctx,
		getMetricTagValuesReq{
			ai:         h.accessInfo,
			version:    r.FormValue(ParamVersion),
			numResults: r.FormValue(ParamNumResults),
			metricName: formValueParamMetric(r),
			tagID:      r.FormValue(ParamTagID),
			from:       r.FormValue(ParamFromTime),
			to:         r.FormValue(ParamToTime),
			what:       r.FormValue(ParamQueryWhat),
			filter:     r.Form[ParamQueryFilter],
		})

	cache, cacheStale := queryClientCacheDuration(immutable)
	respondJSON(h, resp, cache, cacheStale, err)
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
	if meta.stag {
		c.res = append(c.res, proto.ResultColumn{Name: "_string_value", Data: &c.val})
	} else {
		c.res = append(c.res, proto.ResultColumn{Name: "_value", Data: &c.valID})
	}
	c.res = append(c.res, proto.ResultColumn{Name: "_count", Data: &c.cnt})
	return c
}

func newTagValuesSelectColsV3(meta tagValuesQueryMeta) *tagValuesSelectCols {
	c := &tagValuesSelectCols{meta: meta}
	c.res = append(c.res, proto.ResultColumn{Name: "_mapped", Data: &c.valID})
	c.res = append(c.res, proto.ResultColumn{Name: "_unmapped", Data: &c.val})
	c.res = append(c.res, proto.ResultColumn{Name: "_count", Data: &c.cnt})
	return c
}

func (c *tagValuesSelectCols) rowAt(i int) selectRow {
	row := selectRow{cnt: c.cnt[i]}
	if c.meta.mixed {
		pos := c.val.Pos[i]
		row.val = string(c.val.Buf[pos.Start:pos.End])
		row.valID = c.valID[i]
		return row
	}
	if c.meta.stag {
		pos := c.val.Pos[i]
		row.val = string(c.val.Buf[pos.Start:pos.End])
	} else {
		row.valID = c.valID[i]
	}
	return row
}

func (h *HTTPRequestHandler) handleGetMetricTagValues(ctx context.Context, req getMetricTagValuesReq) (resp *GetMetricTagValuesResp, immutable bool, err error) {
	version, err := parseVersion(req.version)
	if err != nil {
		return nil, false, err
	}

	var numResults int
	if req.numResults == "" || req.numResults == "0" {
		numResults = defTagValues
	} else if numResults, err = parseNumResults(req.numResults, maxTagValues); err != nil {
		return nil, false, err
	}

	metricMeta, err := h.getMetricMeta(req.metricName)
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
	var filterInV3 map[string][]maybeMappedTag
	var filterNotInV3 map[string][]maybeMappedTag
	if version == Version3 {
		filterInV3, err = h.resolveFilterV3(metricMeta, filterIn)
		if err != nil {
			return nil, false, err
		}
		filterNotInV3, err = h.resolveFilterV3(metricMeta, filterNotIn)
		if err != nil {
			return nil, false, err
		}
	}

	lods, err := data_model.GetLODs(data_model.GetTimescaleArgs{
		Version:     req.version,
		Start:       from.Unix(),
		End:         to.Unix(),
		ScreenWidth: 100, // really dumb
		TimeNow:     time.Now().Unix(),
		Metric:      metricMeta,
		Location:    h.location,
		UTCOffset:   h.utcOffset,
	})
	if err != nil {
		return nil, false, err
	}

	pq := &preparedTagValuesQuery{
		version:       version,
		metricID:      metricMeta.MetricID,
		preKeyTagID:   metricMeta.PreKeyTagID,
		tagID:         tagID,
		numResults:    numResults,
		filterIn:      mappedFilterIn,
		filterNotIn:   mappedFilterNotIn,
		filterInV3:    filterInV3,
		filterNotInV3: filterNotInV3,
	}

	tagInfo := map[selectRow]float64{}
	if version == Version1 && tagID == format.EnvTagID {
		tagInfo[selectRow{valID: format.TagValueIDProductionLegacy}] = 100 // we only support production tables for v1
	} else {
		for _, lod := range lods {
			query, meta, err := tagValuesQuery(pq, lod) // we set limit to numResult+1
			if err != nil {
				return nil, false, err
			}
			var cols *tagValuesSelectCols
			switch pq.version {
			case Version3:
				cols = newTagValuesSelectColsV3(meta)
			case Version1, Version2:
				cols = newTagValuesSelectCols(meta)
			}
			isFast := lod.FromSec+fastQueryTimeInterval >= lod.ToSec
			err = h.doSelect(ctx, util.QueryMetaInto{
				IsFast:  isFast,
				IsLight: true,
				User:    req.ai.user,
				Metric:  metricMeta.MetricID,
				Table:   lod.Table,
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
		if len(v) > 0 {
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

func HandleGetTable(h *HTTPRequestHandler, r *http.Request) {
	var err error
	var req seriesRequest
	if req, err = parseHTTPRequest(r, h.location, h.metricsStorage.GetDashboardMeta); err == nil {
		if err = h.parseAccessToken(r); err == nil {
			req.ai = h.accessInfo
			err = req.validate()
		}
	}
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()

	if req.numResults <= 0 || maxTableRowsPage < req.numResults {
		req.numResults = maxTableRowsPage
	}
	respTable, immutable, err := h.handleGetTable(ctx, true, req)
	if h.verbose && err == nil {
		log.Printf("[debug] handled query (%v rows) for %q in %v", len(respTable.Rows), h.endpointStat.user, time.Since(h.endpointStat.timestamp))
	}

	cache, cacheStale := queryClientCacheDuration(immutable)
	respondJSON(h, respTable, cache, cacheStale, err)
}

func HandleSeriesQuery(h *HTTPRequestHandler, r *http.Request) {
	var err error
	var req seriesRequest
	if req, err = parseHTTPRequest(r, h.location, h.metricsStorage.GetDashboardMeta); err == nil {
		if err = h.parseAccessToken(r); err == nil {
			req.ai = h.accessInfo
			err = req.validate()
		}
	}
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	s, cancel, err := h.handleSeriesRequestS(withEndpointStat(r.Context(), &h.endpointStat), req, &h.endpointStat, make([]seriesResponse, 2))
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	defer cancel()
	switch {
	case r.FormValue(paramDataFormat) == dataFormatCSV:
		exportCSV(h, h.buildSeriesResponse(s...), req.metricName)
	default:
		res := h.buildSeriesResponse(s...)
		cache, cacheStale := queryClientCacheDuration(res.immutable)
		respondJSON(h, res, cache, cacheStale, nil)
	}
}

func HandleFrontendStat(h *HTTPRequestHandler, r *http.Request) {
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	if h.accessInfo.service {
		// statistics from bots isn't welcome
		respondJSON(h, nil, 0, 0, httpErr(404, fmt.Errorf("")))
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	var batch tlstatshouse.AddMetricsBatchBytes
	err = batch.UnmarshalJSON(body)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	for _, v := range batch.Metrics {
		if string(v.Name) != format.BuiltinMetricNameIDUIErrors { // metric whitelist
			respondJSON(h, nil, 0, 0, fmt.Errorf("metric is not in whitelist"))
			return
		}
		tags := make(statshouse.NamedTags, 0, len(v.Tags))
		for _, v := range v.Tags {
			tags = append(tags, [2]string{string(v.Key), string(v.Value)})
		}
		metric := statshouse.MetricNamedRef(string(v.Name), tags)
		switch {
		case v.IsSetUnique():
			metric.Uniques(v.Unique)
		case v.IsSetValue():
			metric.Values(v.Value)
		default:
			metric.Count(v.Counter)
		}
	}
	respondJSON(h, nil, 0, 0, nil)
}

func (h *Handler) queryBadges(ctx context.Context, req seriesRequest, meta *format.MetricMetaValue) (seriesResponse, func(), error) {
	var res seriesResponse
	ctx = debugQueriesContext(ctx, &res.trace)
	ctx = promql.TraceContext(ctx, &res.trace)
	req.ai.skipBadgesValidation = true
	v, cleanup, err := h.promEngine.Exec(
		withAccessInfo(ctx, &req.ai),
		promql.Query{
			Start: req.from.Unix(),
			End:   req.to.Unix(),
			Step:  req.step,
			Expr:  fmt.Sprintf(`%s{@what="countraw,avg",@by="1,2",2=" 0",2=" %d"}`, format.BuiltinMetricNameBadges, meta.MetricID),
			Options: promql.Options{
				Version:          req.version,
				ExplicitGrouping: true,
				QuerySequential:  h.querySequential,
				ScreenWidth:      req.screenWidth,
			},
		})
	if err != nil {
		return seriesResponse{}, nil, err
	}
	res.TimeSeries, _ = v.(*promql.TimeSeries)
	return res, cleanup, nil
}

func HandlePointQuery(h *HTTPRequestHandler, r *http.Request) {
	var err error
	var req seriesRequest
	if req, err = parseHTTPRequest(r, h.location, h.metricsStorage.GetDashboardMeta); err == nil {
		if err = h.parseAccessToken(r); err == nil {
			req.ai = h.accessInfo
			err = req.validate()
		}
	}
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	s, cancel, err := h.handleSeriesRequest(
		withEndpointStat(r.Context(), &h.endpointStat), req,
		seriesRequestOptions{mode: data_model.PointQuery, trace: true})
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	defer cancel()
	switch {
	case r.FormValue(paramDataFormat) == dataFormatCSV:
		respondJSON(h, h.buildPointResponse(s), 0, 0, httpErr(http.StatusBadRequest, nil))
	default:
		immutable := req.to.Before(time.Now().Add(invalidateFrom))
		cache, cacheStale := queryClientCacheDuration(immutable)
		respondJSON(h, h.buildPointResponse(s), cache, cacheStale, err)
	}
}

func HandleGetRender(h *HTTPRequestHandler, r *http.Request) {
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()

	s, err := parseHTTPRequestS(r, 12, h.location, h.metricsStorage.GetDashboardMeta)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}

	resp, immutable, err := h.handleGetRender(
		ctx, h.accessInfo,
		renderRequest{
			seriesRequest: s,
			renderWidth:   r.FormValue(paramRenderWidth),
			renderFormat:  r.FormValue(paramDataFormat),
		}, &h.endpointStat)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}

	cache, cacheStale := queryClientCacheDuration(immutable)
	respondPlot(h, resp.format, resp.data, cache, cacheStale, h.accessInfo.user)
}

func HandleGetEntity[T any](h *HTTPRequestHandler, r *http.Request, handle func(ctx context.Context, ai accessInfo, id int32, version int64) (T, time.Duration, error)) {
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	idStr := r.FormValue(ParamID)
	id, err := strconv.ParseInt(idStr, 10, 32)
	if err != nil {
		respondJSON(h, nil, 0, 0, httpErr(http.StatusBadRequest, err))
		return
	}
	verStr := r.FormValue(ParamEntityVersion)
	var ver int64
	if verStr != "" {
		ver, err = strconv.ParseInt(verStr, 10, 64)
		if err != nil {
			respondJSON(h, nil, 0, 0, httpErr(http.StatusBadRequest, err))
			return
		}
	}
	resp, cache, err := handle(r.Context(), h.accessInfo, int32(id), ver)
	respondJSON(h, resp, cache, 0, err)
}

func HandleGetDashboard(h *HTTPRequestHandler, r *http.Request) {
	HandleGetEntity(h, r, h.handleGetDashboard)
}

func HandleGetGroup(h *HTTPRequestHandler, r *http.Request) {
	HandleGetEntity(h, r, func(ctx context.Context, ai accessInfo, id int32, _ int64) (*MetricsGroupInfo, time.Duration, error) {
		return h.handleGetGroup(ai, id)
	})
}

func HandleGetNamespace(h *HTTPRequestHandler, r *http.Request) {
	HandleGetEntity(h, r, func(_ context.Context, ai accessInfo, id int32, _ int64) (*NamespaceInfo, time.Duration, error) {
		return h.handleGetNamespace(ai, id)
	})
}

func HandleGetEntityList[T any](h *HTTPRequestHandler, r *http.Request, handle func(ai accessInfo, showInvisible bool) (T, time.Duration, error)) {
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	sd := r.URL.Query().Has(paramShowDisabled)
	resp, cache, err := handle(h.accessInfo, sd)
	respondJSON(h, resp, cache, 0, err)
}

func HandleGetGroupsList(h *HTTPRequestHandler, r *http.Request) {
	HandleGetEntityList(h, r, h.handleGetGroupsList)
}

func HandleGetDashboardList(h *HTTPRequestHandler, r *http.Request) {
	HandleGetEntityList(h, r, h.handleGetDashboardList)
}

func HandleGetNamespaceList(h *HTTPRequestHandler, r *http.Request) {
	HandleGetEntityList(h, r, h.handleGetNamespaceList)
}

func HandlePutPostDashboard(h *HTTPRequestHandler, r *http.Request) {
	var dashboard DashboardInfo
	handlePostEntity(h, r, &dashboard, func(ctx context.Context, ai accessInfo, entity *DashboardInfo, create bool) (resp interface{}, versionToWait int64, err error) {
		response, err := h.handlePostDashboard(ctx, ai, entity.Dashboard, create, entity.Delete)
		if err != nil {
			return nil, 0, err
		}
		return response, response.Dashboard.Version, nil
	})
}

func (h *HTTPRequestHandler) handleGetRender(ctx context.Context, ai accessInfo, req renderRequest, es *endpointStat) (*renderResponse, bool, error) {
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
		r.ai = ai
		if r.numResults == 0 || r.numResults > 15 {
			// Limit number of plots on the preview because
			// there is no point in drawing a lot on a small canvas
			// (it takes time and doesn't look good)
			r.numResults = 15
		}
		start := time.Now()
		v, cancel, err := h.handleSeriesRequest(withEndpointStat(ctx, es), r, seriesRequestOptions{
			metricCallback: func(meta *format.MetricMetaValue) {
				req.seriesRequest[i].metricName = meta.Name
			},
			strBucketLabel: true,
		})
		if err != nil {
			return nil, false, err
		}
		defer cancel() // hold until plot call
		res := h.buildSeriesResponse(v)
		immutable = immutable && res.immutable
		if h.verbose {
			log.Printf("[debug] handled render query (%v series x %v points each) for %q in %v", len(res.Series.SeriesMeta), len(res.Series.Time), ai.user, time.Since(start))
			seriesNum += len(res.Series.SeriesMeta)
			pointsNum += len(res.Series.SeriesMeta) * len(res.Series.Time)
		}
		h.colorize(res)
		s[i] = res
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
	es.timings.Report("plot", time.Since(start))
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

func (h *HTTPRequestHandler) handleGetTable(ctx context.Context, debugQueries bool, req seriesRequest) (resp *GetTableResp, immutable bool, err error) {
	metricMeta, err := h.getMetricMeta(req.metricName)
	if err != nil {
		return nil, false, err
	}
	err = validateQuery(metricMeta, req.version)
	if err != nil {
		return nil, false, err
	}
	mappedFilterIn, err := h.resolveFilter(metricMeta, req.version, req.filterIn)
	if err != nil {
		return nil, false, err
	}
	mappedFilterNotIn, err := h.resolveFilter(metricMeta, req.version, req.filterNotIn)
	if err != nil {
		return nil, false, err
	}
	lods, err := data_model.GetLODs(data_model.GetTimescaleArgs{
		Version:     req.version,
		Start:       req.from.Unix(),
		End:         req.to.Unix(),
		Step:        req.step,
		ScreenWidth: req.screenWidth,
		TimeNow:     time.Now().Unix(),
		Metric:      metricMeta,
		Location:    h.location,
		UTCOffset:   h.utcOffset,
	})
	if err != nil {
		return nil, false, err
	}
	desiredStepMul := int64(1)
	if req.step != 0 {
		desiredStepMul = int64(req.step)
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
	queryRows, hasMore, err := getTableFromLODs(ctx, lods, tableReqParams{
		req:               req,
		user:              h.accessInfo.user,
		metricMeta:        metricMeta,
		isStringTop:       metricMeta.StringTopDescription != "",
		mappedFilterIn:    mappedFilterIn,
		mappedFilterNotIn: mappedFilterNotIn,
		rawValue:          req.screenWidth == 0 || req.step == _1M,
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
	immutable = req.to.Before(time.Now().Add(invalidateFrom))
	return &GetTableResp{
		Rows:         queryRows,
		What:         req.what,
		FromRow:      firstRowStr,
		ToRow:        lastRowStr,
		More:         hasMore,
		DebugQueries: sqlQueries,
	}, immutable, nil
}

func (h *Handler) handleSeriesRequestS(ctx context.Context, req seriesRequest, es *endpointStat, s []seriesResponse) ([]seriesResponse, func(), error) {
	var err error
	var cancelT func()
	var freeBadges func()
	var freeRes func()
	ctx, cancelT = context.WithTimeout(ctx, h.querySelectTimeout)
	cancel := func() {
		cancelT()
		if freeBadges != nil {
			freeBadges()
		}
		if freeRes != nil {
			freeRes()
		}
	}
	if req.verbose && len(s) > 1 {
		var g *errgroup.Group
		g, ctx = errgroup.WithContext(ctx)
		g.Go(func() (err error) {
			s[0], freeRes, err = h.handleSeriesRequest(withEndpointStat(ctx, es), req, seriesRequestOptions{
				trace: true,
				metricCallback: func(meta *format.MetricMetaValue) {
					req.metricName = meta.Name
					if meta.MetricID != format.BuiltinMetricIDBadges {
						g.Go(func() (err error) {
							s[1], freeBadges, err = h.queryBadges(ctx, req, meta)
							return err
						})
					}
				},
			})
			return err
		})
		err = g.Wait()
	} else {
		s[0], freeRes, err = h.handleSeriesRequest(withEndpointStat(ctx, es), req, seriesRequestOptions{
			trace: true,
		})
	}
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return s, cancel, nil
}

func (h *Handler) handleSeriesRequest(ctx context.Context, req seriesRequest, opt seriesRequestOptions) (seriesResponse, func(), error) {
	err := req.validate()
	if err != nil {
		return seriesResponse{}, nil, err
	}
	var limit int
	var promqlGenerated bool
	if len(req.promQL) == 0 {
		req.promQL, err = h.getPromQuery(req)
		if err != nil {
			return seriesResponse{}, nil, httpErr(http.StatusBadRequest, err)
		}
		promqlGenerated = true
	} else {
		limit = req.numResults
	}
	if opt.timeNow.IsZero() {
		opt.timeNow = time.Now()
	}
	var offsets = make([]int64, 0, len(req.shifts))
	for _, v := range req.shifts {
		offsets = append(offsets, -toSec(v))
	}
	var res seriesResponse
	if opt.trace {
		ctx = debugQueriesContext(ctx, &res.trace)
		ctx = promql.TraceContext(ctx, &res.trace)
	}
	v, cleanup, err := h.promEngine.Exec(
		withAccessInfo(ctx, &req.ai),
		promql.Query{
			Start: req.from.Unix(),
			End:   req.to.Unix(),
			Step:  req.step,
			Expr:  req.promQL,
			Options: promql.Options{
				Version:          req.version,
				Mode:             opt.mode,
				AvoidCache:       req.avoidCache,
				TimeNow:          opt.timeNow.Unix(),
				Extend:           req.excessPoints,
				ExplicitGrouping: true,
				RawBucketLabel:   !opt.strBucketLabel,
				QuerySequential:  h.querySequential,
				TagWhat:          promqlGenerated,
				ScreenWidth:      req.screenWidth,
				MaxHost:          req.maxHost,
				Offsets:          offsets,
				Limit:            limit,
				Rand:             opt.rand,
				ExprQueriesSingleMetricCallback: func(metric *format.MetricMetaValue) {
					res.metric = metric
					if opt.metricCallback != nil {
						opt.metricCallback(metric)
					}
				},
				Vars:   req.vars,
				Compat: req.compat,
			},
		})
	if err != nil {
		return seriesResponse{}, nil, err
	}
	if res.TimeSeries, _ = v.(*promql.TimeSeries); res.TimeSeries == nil {
		cleanup()
		return seriesResponse{}, nil, fmt.Errorf("string literals are not supported")
	}
	if promqlGenerated {
		res.promQL = req.promQL
	}
	if len(res.Time) != 0 {
		res.extraPointLeft = res.Time[0] < req.from.Unix()
		res.extraPointRight = req.to.Unix() <= res.Time[len(res.Time)-1]
	}
	// clamp values because JSON doesn't support "Inf" values,
	// extra large values usually don't make sense.
	// TODO: don't lose values, pass large values (including "Inf" as is)
	for _, d := range res.Series.Data {
		for i, v := range *d.Values {
			if v < -math.MaxFloat32 {
				(*d.Values)[i] = -math.MaxFloat32
			} else if v > math.MaxFloat32 {
				(*d.Values)[i] = math.MaxFloat32
			}
		}
	}
	return res, cleanup, nil
}

func (h *Handler) buildSeriesResponse(s ...seriesResponse) *SeriesResponse {
	s0 := s[0]
	res := &SeriesResponse{
		Series: querySeries{
			Time:       s0.TimeSeries.Time,
			SeriesData: make([]*[]float64, 0, len(s0.Series.Data)),
			SeriesMeta: make([]QuerySeriesMetaV2, 0, len(s0.Series.Data)),
		},
		PromQL:           s0.promQL,
		MetricMeta:       s0.metric,
		DebugQueries:     s0.trace,
		ExcessPointLeft:  s0.extraPointLeft,
		ExcessPointRight: s0.extraPointRight,
	}
	for i, d := range s0.Series.Data {
		meta := QuerySeriesMetaV2{
			MaxHosts:   d.GetSMaxHosts(h),
			Total:      s0.Series.Meta.Total,
			MetricType: s0.Series.Meta.Units,
		}
		meta.What, meta.TimeShift, meta.Tags = s0.queryFuncShiftAndTagsAt(i)
		if s0.metric != nil {
			meta.Name = s0.metric.Name
		}
		if meta.Total == 0 {
			meta.Total = len(s0.Series.Data)
		}
		res.Series.SeriesMeta = append(res.Series.SeriesMeta, meta)
		res.Series.SeriesData = append(res.Series.SeriesData, d.Values)
	}
	if res.Series.SeriesData == nil {
		// frontend expects not "null" value
		res.Series.SeriesData = make([]*[]float64, 0)
	}
	// Add badges
	if s0.metric != nil && len(s) > 1 && s[1].TimeSeries != nil && len(s[1].Time) > 0 {
		s1 := s[1]
		for _, d := range s1.Series.Data {
			if t, ok := d.Tags.ID2Tag["2"]; !ok || t.SValue != s0.metric.Name {
				continue
			}
			if t, ok := d.Tags.ID2Tag["1"]; ok {
				badgeType := t.Value
				if t, ok = d.Tags.ID2Tag[promql.LabelWhat]; ok {
					what := data_model.DigestWhat(t.Value)
					switch {
					case what == data_model.DigestAvg && badgeType == format.TagValueIDBadgeAgentSamplingFactor:
						res.SamplingFactorSrc = sumSeries(d.Values, 1) / float64(len(s1.Time))
					case what == data_model.DigestAvg && badgeType == format.TagValueIDBadgeAggSamplingFactor:
						res.SamplingFactorAgg = sumSeries(d.Values, 1) / float64(len(s1.Time))
					case what == data_model.DigestCountRaw && badgeType == format.TagValueIDBadgeIngestionErrors:
						res.ReceiveErrors = sumSeries(d.Values, 0)
					case what == data_model.DigestCountRaw && badgeType == format.TagValueIDBadgeIngestionWarnings:
						res.ReceiveWarnings = sumSeries(d.Values, 0)
					case what == data_model.DigestCountRaw && badgeType == format.TagValueIDBadgeAggMappingErrors:
						res.MappingErrors = sumSeries(d.Values, 0)
					}
				}
			}
			// TODO - show badge if some heuristics on # of contributors is triggered
			// if format.IsValueCodeZero(metric) && meta.What.String() == ParamQueryFnCountNorm && badgeType == format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeContributors)) {
			//	sumContributors := sumSeries(respIngestion.Series.SeriesData[i], 0)
			//	fmt.Printf("contributors sum %f\n", sumContributors)
			// }
		}
		res.DebugQueries = append(res.DebugQueries, "") // line break
		res.DebugQueries = append(res.DebugQueries, s1.trace...)
	}
	h.colorize(res)
	return res
}

func (h *HTTPRequestHandler) buildPointResponse(s seriesResponse) *GetPointResp {
	res := &GetPointResp{
		PointMeta:    make([]QueryPointsMeta, 0),
		PointData:    make([]float64, 0),
		DebugQueries: s.trace,
	}
	for i, d := range s.Series.Data {
		meta := QueryPointsMeta{
			FromSec: s.Time[0],
			ToSec:   s.Time[1],
		}
		meta.What, meta.TimeShift, meta.Tags = s.queryFuncShiftAndTagsAt(i)
		if s.metric != nil {
			meta.Name = s.metric.Name
		}
		if maxHost := d.GetSMaxHosts(h); len(maxHost) != 0 {
			meta.MaxHost = maxHost[0]
		}
		res.PointMeta = append(res.PointMeta, meta)
		res.PointData = append(res.PointData, (*d.Values)[0])
	}
	return res
}

func (s seriesResponse) queryFuncShiftAndTagsAt(i int) (string, int64, map[string]SeriesMetaTag) {
	d := s.Series.Data[i]
	tags := make(map[string]SeriesMetaTag, len(d.Tags.ID2Tag))
	timsShift := -d.Offset
	for id, tag := range d.Tags.ID2Tag {
		if len(tag.SValue) == 0 || tag.ID == labels.MetricName || tag.ID == promql.LabelWhat {
			continue
		}
		if tag.ID == promql.LabelOffset {
			timsShift = -(d.Offset + int64(tag.Value))
			continue
		}
		k := id
		v := SeriesMetaTag{Value: tag.SValue}
		if tag.Index != 0 {
			var name string
			index := tag.Index - promql.SeriesTagIndexOffset
			if index == format.StringTopTagIndex {
				k = format.LegacyStringTopTagID
				name = format.StringTopTagID
			} else {
				k = format.TagIDLegacy(index)
				name = format.TagID(index)
			}
			if s.Series.Meta.Metric != nil {
				if meta, ok := s.Series.Meta.Metric.Name2Tag[name]; ok {
					v.Comment = meta.ValueComments[v.Value]
					v.Raw = meta.Raw
					v.RawKind = meta.RawKind
				}
			}
		}
		tags[k] = v

	}
	queryFunc := d.What.QueryF
	if queryFunc == "" {
		queryFunc = promql.DigestWhatString(d.What.Digest)
	}
	return queryFunc, timsShift, tags
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
	sizeInBytes := n * 8
	if n > data_model.MaxSlice {
		s := make([]float64, n)
		h.bufferBytesAlloc.Value(float64(sizeInBytes))
		h.pointFloatsPoolSize.Add(int64(sizeInBytes))
		return &s // should not happen: we should never return more than maxSlice points
	}

	v := h.pointFloatsPool.Get()
	if v == nil {
		s := make([]float64, 0, data_model.MaxSlice)
		v = &s
	}
	ret := v.(*[]float64)
	*ret = (*ret)[:n]
	h.bufferPoolBytesAlloc.Value(float64(sizeInBytes))
	h.pointFloatsPoolSize.Add(int64(sizeInBytes))
	return ret
}

func (h *Handler) putFloatsSlice(s *[]float64) {
	sizeInBytes := len(*s) * 8
	*s = (*s)[:0]

	if cap(*s) <= data_model.MaxSlice {
		h.pointFloatsPool.Put(s)
		h.bufferPoolBytesFree.Value(float64(sizeInBytes))
	} else {
		h.bufferBytesFree.Value(float64(sizeInBytes))
	}
	h.pointFloatsPoolSize.Sub(int64(sizeInBytes))
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
	time         proto.ColInt64
	step         proto.ColInt64
	cnt          proto.ColFloat64
	val          []proto.ColFloat64
	tag          []proto.ColInt32
	stag         []proto.ColStr
	tagIx        []int
	tagStr       proto.ColStr
	minMaxHostV1 [2]proto.ColUInt8 // "min" at [0], "max" at [1]
	minMaxHostV2 [2]proto.ColInt32 // "min" at [0], "max" at [1]
	minMaxHostV3 [2]proto.ColStr   // "min" at [0], "max" at [1]
	shardNum     proto.ColUInt32
	res          proto.Results
}

func newPointsSelectCols(meta pointsQueryMeta, useTime bool, version string) *pointsSelectCols {
	if version == Version3 {
		return newPointsSelectColsV3(meta, useTime)
	}
	return newPointsSelectColsV2(meta, useTime)
}

func newPointsSelectColsV3(meta pointsQueryMeta, useTime bool) *pointsSelectCols {
	// NB! Keep columns selection order and names is sync with sql.go code
	c := &pointsSelectCols{
		val:   make([]proto.ColFloat64, meta.vals),
		tag:   make([]proto.ColInt32, 0, len(meta.tags)),
		stag:  make([]proto.ColStr, 0, len(meta.tags)),
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
			c.res = append(c.res, proto.ResultColumn{Name: "tag" + id, Data: &c.tag[len(c.tag)-1]})
			c.stag = append(c.stag, proto.ColStr{})
			c.res = append(c.res, proto.ResultColumn{Name: "stag" + id, Data: &c.stag[len(c.tag)-1]})
			c.tagIx = append(c.tagIx, format.TagIndex(id))
		}
	}
	c.res = append(c.res, proto.ResultColumn{Name: "_count", Data: &c.cnt})
	for i := 0; i < meta.vals; i++ {
		c.res = append(c.res, proto.ResultColumn{Name: "_val" + strconv.Itoa(i), Data: &c.val[i]})
	}
	if meta.minMaxHost {
		c.res = append(c.res, proto.ResultColumn{Name: "_minHost", Data: &c.minMaxHostV3[0]})
		c.res = append(c.res, proto.ResultColumn{Name: "_maxHost", Data: &c.minMaxHostV3[1]})
	}
	return c
}

func newPointsSelectColsV2(meta pointsQueryMeta, useTime bool) *pointsSelectCols {
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
	if meta.minMaxHost {
		switch meta.version {
		case Version1:
			c.res = append(c.res, proto.ResultColumn{Name: "_minHost", Data: &c.minMaxHostV1[0]})
			c.res = append(c.res, proto.ResultColumn{Name: "_maxHost", Data: &c.minMaxHostV1[1]})
		case Version2:
			c.res = append(c.res, proto.ResultColumn{Name: "_minHost", Data: &c.minMaxHostV2[0]})
			c.res = append(c.res, proto.ResultColumn{Name: "_maxHost", Data: &c.minMaxHostV2[1]})
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
	for j := range c.stag {
		st := c.stag[j]
		if st.Buf == nil || len(st.Pos) < i || len(st.Buf) < st.Pos[i].End {
			continue
		}
		row.stag[c.tagIx[j]] = string(st.Buf[st.Pos[i].Start:st.Pos[i].End])
	}
	if c.tagStr.Pos != nil && i < len(c.tagStr.Pos) {
		copy(row.tagStr[:], c.tagStr.Buf[c.tagStr.Pos[i].Start:c.tagStr.Pos[i].End])
	}
	if len(c.minMaxHostV2[0]) != 0 {
		row.host[0] = c.minMaxHostV2[0][i]
	}
	if len(c.minMaxHostV2[1]) != 0 {
		row.host[1] = c.minMaxHostV2[1][i]
	}
	if c.minMaxHostV3[0].Rows() != 0 {
		mm3 := c.minMaxHostV3[0]
		if i < len(mm3.Pos) && mm3.Pos[i].End-mm3.Pos[i].Start == 5 && mm3.Pos[i].Start < len(mm3.Buf) && mm3.Buf[mm3.Pos[i].Start] == 0 { // first byte is 0 for mapped hostTagId
			row.host[0] = int32(binary.LittleEndian.Uint32(mm3.Buf[mm3.Pos[i].Start+1 : mm3.Pos[i].End]))
		}
	}
	if c.minMaxHostV3[1].Rows() != 0 {
		mm3 := c.minMaxHostV3[1]
		if i < len(mm3.Pos) && mm3.Pos[i].End-mm3.Pos[i].Start == 5 && mm3.Pos[i].Start < len(mm3.Buf) && mm3.Buf[mm3.Pos[i].Start] == 0 { // first byte is 0 for mapped hostTagId
			row.host[1] = int32(binary.LittleEndian.Uint32(mm3.Buf[mm3.Pos[i].Start+1 : mm3.Pos[i].End]))
		}
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
	if len(c.minMaxHostV2[0]) != 0 {
		row.host[0] = c.minMaxHostV2[0][i]
	}
	if len(c.minMaxHostV2[1]) != 0 {
		row.host[1] = c.minMaxHostV2[1][i]
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

func (h *Handler) loadPoints(ctx context.Context, pq *preparedPointsQuery, lod data_model.LOD, ret [][]tsSelectRow, retStartIx int) (int, error) {
	query, args, err := loadPointsQuery(pq, lod, h.utcOffset)
	if err != nil {
		return 0, err
	}

	rows := 0
	cols := newPointsSelectCols(args, true, pq.version)
	isFast := lod.IsFast()
	isLight := pq.isLight()
	IsHardware := pq.IsHardware()
	metric := pq.metricID
	table := lod.Table
	kind := pq.kind
	var metricName string
	if m, ok := format.BuiltinMetrics[pq.metricID]; ok {
		metricName = m.Name
	} else if m := h.metricsStorage.GetMetaMetric(pq.metricID); m != nil {
		metricName = m.Name
	}
	start := time.Now()
	mappings := make(map[string]int32)
	err = h.doSelect(ctx, util.QueryMetaInto{
		IsFast:     isFast,
		IsLight:    isLight,
		IsHardware: IsHardware,
		User:       pq.user,
		Metric:     metric,
		Table:      table,
		Kind:       kind.String(),
	}, pq.version, ch.Query{
		Body:   query,
		Result: cols.res,
		OnResult: func(_ context.Context, block proto.Block) error {
			for i := 0; i < block.Rows; i++ {
				replaceInfNan(&cols.cnt[i])
				for j := 0; j < len(cols.val); j++ {
					replaceInfNan(&cols.val[j][i])
				}
				row := cols.rowAt(i)
				if metricName != "" { // should be always true
					for k := range row.stag {
						// check if tag value is mapped
						if v, ok := mappings[row.stag[k]]; ok && v != 0 {
							row.tag[k] = v
							row.stag[k] = ""
						}
						// call GetTagMapping only first time for each tag value, since it's expensive
						v, _, _, _ := h.metadataLoader.GetTagMapping(ctx, row.stag[k], metricName, false)
						mappings[row.stag[k]] = v
						if v != 0 {
							row.tag[k] = v
							row.stag[k] = ""
						}
					}
				}
				ix, err := lod.IndexOf(row.time)
				if err != nil {
					return err
				}
				ix += retStartIx
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
			lod.Table,
			(lod.ToSec-lod.FromSec)/lod.StepSec,
			time.Unix(lod.FromSec, 0),
			time.Unix(lod.ToSec, 0),
			time.Duration(lod.StepSec)*time.Second,
			pq.user,
			duration,
		)
	}

	return rows, nil
}

func (h *Handler) loadPoint(ctx context.Context, pq *preparedPointsQuery, lod data_model.LOD) ([]pSelectRow, error) {
	query, args, err := loadPointQuery(pq, lod, h.utcOffset)
	if err != nil {
		return nil, err
	}
	ret := make([]pSelectRow, 0)
	rows := 0
	cols := newPointsSelectColsV2(args, false) // loadPoint doesn't yet have v3 query
	isFast := lod.IsFast()
	isLight := pq.isLight()
	isHardware := pq.IsHardware()
	metric := pq.metricID
	table := lod.Table
	kind := pq.kind
	err = h.doSelect(ctx, util.QueryMetaInto{
		IsFast:     isFast,
		IsLight:    isLight,
		IsHardware: isHardware,
		User:       pq.user,
		Metric:     metric,
		Table:      table,
		Kind:       kind.String(),
	}, pq.version, ch.Query{
		Body:   query,
		Result: cols.res,
		OnResult: func(_ context.Context, block proto.Block) error {
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
			lod.Table,
			time.Unix(lod.FromSec, 0),
			time.Unix(lod.ToSec, 0),
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

func selectTSValue(what data_model.DigestWhat, maxHost bool, desiredStepMul int64, row *tsSelectRow) float64 {
	switch what {
	case data_model.DigestCount:
		return stableMulDiv(row.countNorm, desiredStepMul, row.stepSec)
	case data_model.DigestCountRaw:
		return row.countNorm
	case data_model.DigestCountSec:
		return row.countNorm / float64(row.stepSec)
	case data_model.DigestCardinality:
		if maxHost {
			return stableMulDiv(row.val[5], desiredStepMul, row.stepSec)
		}
		return stableMulDiv(row.val[0], desiredStepMul, row.stepSec)
	case data_model.DigestCardinalityRaw:
		if maxHost {
			return row.val[5]
		}
		return row.val[0]
	case data_model.DigestCardinalitySec:
		if maxHost {
			return row.val[5] / float64(row.stepSec)
		}
		return row.val[0] / float64(row.stepSec)
	case data_model.DigestMin:
		return row.val[0]
	case data_model.DigestMax:
		return row.val[1]
	case data_model.DigestAvg:
		return row.val[2]
	case data_model.DigestSum:
		return stableMulDiv(row.val[3], desiredStepMul, row.stepSec)
	case data_model.DigestSumRaw:
		return row.val[3]
	case data_model.DigestSumSec:
		return row.val[3] / float64(row.stepSec)
	case data_model.DigestStdDev:
		return row.val[4]
	case data_model.DigestStdVar:
		return row.val[4] * row.val[4]
	case data_model.DigestP0_1:
		return row.val[0]
	case data_model.DigestP1:
		return row.val[1]
	case data_model.DigestP5:
		return row.val[2]
	case data_model.DigestP10:
		return row.val[3]
	case data_model.DigestP25:
		return row.val[0]
	case data_model.DigestP50:
		return row.val[1]
	case data_model.DigestP75:
		return row.val[2]
	case data_model.DigestP90:
		return row.val[3]
	case data_model.DigestP95:
		return row.val[4]
	case data_model.DigestP99:
		return row.val[5]
	case data_model.DigestP999:
		return row.val[6]
	case data_model.DigestUnique:
		return stableMulDiv(row.val[0], desiredStepMul, row.stepSec)
	case data_model.DigestUniqueRaw:
		return row.val[0]
	case data_model.DigestUniqueSec:
		return row.val[0] / float64(row.stepSec)
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

func (h *Handler) checkReadOnlyMode(w http.ResponseWriter, _ *http.Request) (readOnlyMode bool) {
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

func lessThan(l RowMarker, r tsSelectRow, skey string, orEq bool, fromEnd bool) bool {
	if fromEnd {
		if l.Time != r.time {
			return l.Time > r.time
		}
		for i := range l.Tags {
			lv := l.Tags[i].Value
			rv := r.tag[l.Tags[i].Index]
			if lv != rv {
				return lv > rv
			}
		}
		if orEq {
			return l.SKey >= skey
		}
		return l.SKey > skey
	} else {
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
}

func (s queryTableRows) Less(i, j int) bool {
	l, r := s[i].rowRepr, s[j].rowRepr
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

func (s queryTableRows) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s queryTableRows) Len() int {
	return len(s)
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

func (r *seriesRequest) validate() error {
	if r.avoidCache && !r.ai.isAdmin() {
		return httpErr(404, fmt.Errorf(""))
	}
	if r.step == _1M {
		for _, v := range r.shifts {
			if (v/time.Second)%_1M != 0 {
				return httpErr(http.StatusBadRequest, fmt.Errorf("time shift %v can't be used with month interval", v))
			}
		}
	}
	return nil
}

func mergeMetricNamespace(namespace string, metric string) string {
	if strings.Contains(namespace, format.NamespaceSeparator) {
		return metric
	}
	return format.NamespaceName(namespace, metric)
}

func (h *HTTPRequestHandler) parseAccessToken(r *http.Request) (err error) {
	h.accessInfo, err = parseAccessToken(h.jwtHelper, vkuth.GetAccessToken(r), h.protectedMetricPrefixes, h.LocalMode, h.insecureMode)
	h.endpointStat.setAccessInfo(h.accessInfo)
	return err
}

func HandleProf(w *HTTPRequestHandler, r *http.Request) {
	if pprofAccessAllowed(w, r) {
		pprof.Index(w, r)
	}
}

func HandleProfCmdline(w *HTTPRequestHandler, r *http.Request) {
	if pprofAccessAllowed(w, r) {
		pprof.Cmdline(w, r)
	}
}

func HandleProfProfile(w *HTTPRequestHandler, r *http.Request) {
	if pprofAccessAllowed(w, r) {
		pprof.Profile(w, r)
	}
}

func HandleProfSymbol(w *HTTPRequestHandler, r *http.Request) {
	if pprofAccessAllowed(w, r) {
		pprof.Symbol(w, r)
	}
}

func HandleProfTrace(w *HTTPRequestHandler, r *http.Request) {
	if pprofAccessAllowed(w, r) {
		pprof.Trace(w, r)
	}
}

func pprofAccessAllowed(w *HTTPRequestHandler, r *http.Request) bool {
	if err := w.parseAccessToken(r); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return false
	}
	if ok := w.accessInfo.insecureMode || w.accessInfo.bitAdmin; !ok {
		w.WriteHeader(http.StatusForbidden)
		return false
	}
	return true
}

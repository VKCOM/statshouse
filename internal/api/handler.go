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
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	ttemplate "text/template"
	"time"

	"github.com/mailru/easyjson"
	_ "github.com/mailru/easyjson/gen" // https://github.com/mailru/easyjson/issues/293
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/util"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
	"github.com/vkcom/statshouse/internal/vkgo/statlogs"
	"github.com/vkcom/statshouse/internal/vkgo/vkuth"
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
	paramRenderWidth  = "rw"
	paramDataFormat   = "df"
	paramTabNumber    = "tn"

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
	maxTimeShifts = 10
	maxFunctions  = 10

	cacheInvalidateCheckInterval = 1 * time.Second
	cacheInvalidateCheckTimeout  = 5 * time.Second
	cacheInvalidateMaxRows       = 100_000
	cacheDefaultDropEvery        = 90 * time.Second

	queryClientCache      = 1 * time.Second
	queryClientCacheStale = 9 * time.Second // ~ v2 lag
	querySelectTimeout    = 60 * time.Second
	// TODO: querySelectTimeout must be longer than the longest normal query.
	FastQueryTimeInterval = (86400 + 3600) * 2

	maxMetricHTTPBodySize     = 64 << 10
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
		Delete  bool                `json:"delete_mark"`
	}

	//easyjson:json
	PromConfigInfo struct {
		Config  string `json:"config"`
		Version int64  `json:"version"`
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

	getQueryReq struct {
		ai                      accessInfo
		version                 string
		numResults              string
		allowNegativeNumResults bool
		metricWithNamespace     string
		from                    string
		to                      string
		width                   string
		widthAgg                string
		timeShifts              []string
		what                    []string
		by                      []string
		filterIn                map[string][]string
		filterNotIn             map[string][]string
	}

	//easyjson:json
	GetQueryResp struct {
		Series                   querySeries             `json:"series"`
		ReceiveErrorsLegacy      float64                 `json:"receive_errors_legacy"`       // sum of average, legacy
		SamplingFactorSrc        float64                 `json:"sampling_factor_src"`         // average
		SamplingFactorAgg        float64                 `json:"sampling_factor_agg"`         // average
		MappingFloodEventsLegacy float64                 `json:"mapping_flood_events_legacy"` // sum of average, legacy
		ReceiveErrors            float64                 `json:"receive_errors"`              // count/sec
		MappingErrors            float64                 `json:"mapping_error"`               // count/sec
		DebugQueries             []string                `json:"__debug_queries"`             // private, unstable: SQL queries executed
		MetricMeta               *format.MetricMetaValue `json:"-"`
		syncPoolBuffers          []*[]float64            // buffers to be returned to sync.Pool after response is serialized
	}

	getRenderReq struct {
		ai           accessInfo
		getQueryReq  []getQueryReq
		renderWidth  string
		renderFormat string
	}

	getRenderResp struct {
		format string
		data   []byte
	}

	querySeries struct {
		Time       []int64             `json:"time"`        // N
		SeriesMeta []QuerySeriesMetaV2 `json:"series_meta"` // M
		SeriesData []*[]float64        `json:"series_data"` // MxN
	}

	QuerySeriesMeta struct {
		TimeShift int64             `json:"time_shift"`
		Tags      map[string]string `json:"tags"`
		MaxHosts  []string          `json:"max_hosts"` // max_host for now
		What      queryFn           `json:"what"`
	}

	QuerySeriesMetaV2 struct {
		TimeShift int64                    `json:"time_shift"`
		Tags      map[string]SeriesMetaTag `json:"tags"`
		MaxHosts  []string                 `json:"max_hosts"` // max_host for now
		What      queryFn                  `json:"what"`
	}

	SeriesMetaTag struct {
		Value   string `json:"value"`
		Comment string `json:"comment,omitempty"`
		Raw     bool   `json:"raw,omitempty"`
		RawKind string `json:"raw_kind,omitempty"`
	}

	cacheInvalidateLogRow struct {
		T  int64 `ch:"time"` // time of insert
		At int64 `ch:"key1"` // seconds inserted (changed), which should be invalidated
	}
)

func NewHandler(verbose bool, staticDir fs.FS, jsSettings JSSettings, protectedPrefixes []string, showInvisible bool, utcOffsetSec int64, approxCacheMaxSize int, chV1 *util.ClickHouse, chV2 *util.ClickHouse, metadataClient *tlmetadata.Client, diskCache *pcache.DiskCache, jwtHelper *vkuth.JWTHelper, location *time.Location, localMode, readOnly, insecureMode bool) (*Handler, error) {
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
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &h.rUsage)

	h.cache = newTSCacheGroup(approxCacheMaxSize, lodTables, h.utcOffset, h.loadPoints, cacheDefaultDropEvery)
	go h.invalidateLoop()

	h.rmID = statlogs.StartRegularMeasurement(func(registry *statlogs.Registry) { // TODO - stop
		prevRUsage := h.rUsage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &h.rUsage)
		userTime := float64(h.rUsage.Utime.Nano()-prevRUsage.Utime.Nano()) / float64(time.Second)
		sysTime := float64(h.rUsage.Stime.Nano()-prevRUsage.Stime.Nano()) / float64(time.Second)

		userMetric := registry.AccessMetricRaw(format.BuiltinMetricNameUsageCPU, statlogs.RawTags{Tag1: strconv.Itoa(format.TagValueIDComponentAPI), Tag2: strconv.Itoa(format.TagValueIDCPUUsageUser)})
		userMetric.Value(userTime)
		sysMetric := registry.AccessMetricRaw(format.BuiltinMetricNameUsageCPU, statlogs.RawTags{Tag1: strconv.Itoa(format.TagValueIDComponentAPI), Tag2: strconv.Itoa(format.TagValueIDCPUUsageSys)})
		sysMetric.Value(sysTime)

		var rss float64
		if st, _ := srvfunc.GetMemStat(0); st != nil {
			rss = float64(st.Res)
		}
		memMetric := registry.AccessMetricRaw(format.BuiltinMetricNameUsageMemory, statlogs.RawTags{Tag1: strconv.Itoa(format.TagValueIDComponentAPI)})
		memMetric.Value(rss)

		writeActiveQuieries := func(ch *util.ClickHouse, versionTag string) {
			if ch != nil {
				slowMetric := registry.AccessMetricRaw(format.BuiltinMetricNameAPIActiveQueries, statlogs.RawTags{Tag2: versionTag, Tag3: strconv.Itoa(format.TagValueIDAPILaneSlow), Tag4: srvfunc.HostnameForStatshouse()})
				slowMetric.Value(float64(ch.SemaphoreCountSlow()))
				fastMetric := registry.AccessMetricRaw(format.BuiltinMetricNameAPIActiveQueries, statlogs.RawTags{Tag2: versionTag, Tag3: strconv.Itoa(format.TagValueIDAPILaneFast), Tag4: srvfunc.HostnameForStatshouse()})
				fastMetric.Value(float64(ch.SemaphoreCountFast()))
			}
		}
		writeActiveQuieries(chV1, "1")
		writeActiveQuieries(chV2, "2")
	})

	return h, nil
}

func (h *Handler) Close() error {
	statlogs.StopRegularMeasurement(h.rmID)
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

	var rows []cacheInvalidateLogRow
	err := h.doSelect(true, ctx, "cache-update", Version2, &rows, fmt.Sprintf(`
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
		todo    = map[int64][]int64{}
		newSeen = map[cacheInvalidateLogRow]struct{}{}
	)
	for _, r := range rows {
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

	for lodLevel, times := range todo {
		h.cache.Invalidate(lodLevel, times)
	}

	return from, newSeen
}

func (h *Handler) doSelect(isFast bool, ctx context.Context, user string, version string, dest interface{}, query string, args ...interface{}) error {
	if version == Version1 && h.ch[version] == nil {
		return fmt.Errorf("legacy ClickHouse database is disabled")
	}

	debugQuery, err := util.BindQuery(query, args...)
	if err != nil {
		debugQuery = fmt.Sprintf("failed to format SQL query: %v", err)
	}
	saveDebugQuery(ctx, debugQuery)

	start := time.Now()
	err = h.ch[version].Select(isFast, ctx, dest, query, args...)
	if h.verbose {
		log.Printf("[debug] SQL for %q done in %v, err: %v", user, time.Since(start), err)
	}

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
	if !ai.canViewMetric(metricWithNamespace) { // We are OK with sharing this bit of information with clients
		return nil, httpErr(http.StatusForbidden, fmt.Errorf("metric %q forbidden", metricWithNamespace))
	}
	if m, ok := format.BuiltinMetricByName[metricWithNamespace]; ok {
		return m, nil
	}
	v := h.metricsStorage.GetMetaMetricByName(metricWithNamespace)
	if v == nil {
		return nil, httpErr(http.StatusNotFound, fmt.Errorf("metric %q not found", metricWithNamespace))
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
		og, err := getOpenGraphInfo(r, origPath)
		if err != nil {
			log.Printf("[error] failed to generate opengraph tags for index.html: %v", err)
		}
		data := struct {
			OpenGraph *openGraphInfo
			Settings  string
		}{og, h.indexSettings}
		err = h.indexTemplate.Execute(w, data)
		if err != nil {
			log.Printf("[error] failed to write index.html: %v", err)
		}
	} else {
		http.FileServer(h.staticDir).ServeHTTP(w, r)
	}
}

func (h *Handler) parseAccessToken(w http.ResponseWriter, r *http.Request, es *endpointStat) (accessInfo, bool) {
	ai, err := parseAccessToken(h.jwtHelper, vkuth.GetAccessToken(r), h.protectedPrefixes, h.localMode, h.insecureMode)
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
	ai, ok := h.parseAccessToken(w, r, nil)
	if !ok {
		return
	}
	resp, cache, err := h.handleGetMetricsList(ai)
	respondJSON(w, resp, cache, queryClientCacheStale, err, h.verbose, ai.user, nil)
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
	sl := newEndpointStat(EndpointMetric, h.getMetricIDForStat(r.FormValue(ParamMetric)), "")
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}
	resp, cache, err := h.handleGetMetric(ai, formValueParamMetric(r), r.FormValue(ParamID))
	respondJSON(w, resp, cache, 0, err, h.verbose, ai.user, sl) // we don't want clients to see stale metadata
}

func (h *Handler) HandleGetPromConfig(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointPrometheus, 0, "")
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}
	resp, cache, err := h.handleGetPromConfig(ai)
	respondJSON(w, resp, cache, 0, err, h.verbose, ai.user, sl) // we don't want clients to see stale metadata
}

func (h *Handler) HandlePostMetric(w http.ResponseWriter, r *http.Request) {
	if h.checkReadOnlyMode(w, r) {
		return
	}
	ai, ok := h.parseAccessToken(w, r, nil)
	if !ok {
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxMetricHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	if len(res) >= maxMetricHTTPBodySize {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("metric body too big. Max size is %d bytes", maxMetricHTTPBodySize)), h.verbose, ai.user, nil)
		return
	}
	var metric MetricInfo
	if err := easyjson.Unmarshal(res, &metric); err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	m, err := h.handlePostMetric(r.Context(), ai, formValueParamMetric(r), metric.Metric)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	err = h.waitVersionUpdate(r.Context(), m.Version)
	respondJSON(w, &MetricInfo{Metric: m}, defaultCacheTTL, 0, err, h.verbose, ai.user, nil)
}

func (h *Handler) HandlePutPostGroup(w http.ResponseWriter, r *http.Request) {
	if h.checkReadOnlyMode(w, r) {
		return
	}
	ai, ok := h.parseAccessToken(w, r, nil)
	if !ok {
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxMetricHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	if len(res) >= maxMetricHTTPBodySize {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("group body too big. Max size is %d bytes", maxMetricHTTPBodySize)), h.verbose, ai.user, nil)
		return
	}
	var groupInfo MetricsGroupInfo
	if err := easyjson.Unmarshal(res, &groupInfo); err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	d, err := h.handlePostGroup(r.Context(), ai, groupInfo.Group, r.Method == http.MethodPut, groupInfo.Delete)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	err = h.waitVersionUpdate(r.Context(), d.Group.Version)
	respondJSON(w, d, defaultCacheTTL, 0, err, h.verbose, ai.user, nil)
}

func (h *Handler) HandlePostResetFlood(w http.ResponseWriter, r *http.Request) {
	if h.checkReadOnlyMode(w, r) {
		return
	}
	ai, ok := h.parseAccessToken(w, r, nil)
	if !ok {
		return
	}
	if !ai.isAdmin() {
		err := httpErr(http.StatusForbidden, fmt.Errorf("admin access required"))
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	del, err := h.metadataLoader.ResetFlood(context.Background(), formValueParamMetric(r))
	if err == nil && !del {
		err = fmt.Errorf("metric flood counter was empty (no flood)")
	}
	respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
}

func (h *Handler) HandlePostPromConfig(w http.ResponseWriter, r *http.Request) {
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
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	if len(res) >= maxPromConfigHTTPBodySize {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("confog body too big. Max size is %d bytes", maxPromConfigHTTPBodySize)), h.verbose, ai.user, nil)
		return
	}
	var promConfigInfo PromConfigInfo
	if err := easyjson.Unmarshal(res, &promConfigInfo); err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	event, err := h.handlePostPromConfig(r.Context(), ai, promConfigInfo.Config, promConfigInfo.Version)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	err = h.waitVersionUpdate(r.Context(), event.Version)
	respondJSON(w, &PromConfigInfo{Config: event.Data, Version: event.Version}, defaultCacheTTL, 0, err, h.verbose, ai.user, nil)
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

func (h *Handler) handleGetPromConfig(ai accessInfo) (*PromConfigInfo, time.Duration, error) {
	if !ai.isAdmin() {
		return &PromConfigInfo{}, 0, httpErr(http.StatusNotFound, fmt.Errorf("config is not found"))
	}
	config := h.metricsStorage.PromConfig()
	return &PromConfigInfo{
		Config:  config.Data,
		Version: config.Version,
	}, defaultCacheTTL, nil
}

func (h *Handler) handlePostPromConfig(ctx context.Context, ai accessInfo, configStr string, version int64) (tlmetadata.Event, error) {
	if !ai.isAdmin() {
		return tlmetadata.Event{}, httpErr(http.StatusNotFound, fmt.Errorf("config is not found"))
	}
	event, err := h.metadataLoader.SavePromConfig(ctx, version, configStr)
	if err != nil {
		return tlmetadata.Event{}, fmt.Errorf("failed to save prometheus config: %w", err)
	}
	return event, nil
}

func (h *Handler) handleGetDashboard(ai accessInfo, id int32) (*DashboardInfo, time.Duration, error) {
	dash := h.metricsStorage.GetDashboardMeta(id)
	if dash == nil {
		return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("dashboard %d not found", id))
	}
	return &DashboardInfo{Dashboard: getDashboardMetaInfo(dash)}, defaultCacheTTL, nil
}

func (h *Handler) handleGetDashboardList(ai accessInfo) (*GetDashboardListResp, time.Duration, error) {
	dashs := h.metricsStorage.GetDashboardList()
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

func (h *Handler) handlePostGroup(ctx context.Context, ai accessInfo, group format.MetricsGroup, create, delete bool) (*MetricsGroupInfo, error) {
	if !ai.isAdmin() {
		return nil, httpErr(http.StatusNotFound, fmt.Errorf("group %s not found", group.Name))
	}
	if !create {
		if h.metricsStorage.GetGroup(group.ID) != nil {
			return &MetricsGroupInfo{}, httpErr(http.StatusNotFound, fmt.Errorf("group %d not found", group.ID))
		}
	}
	group, err := h.metadataLoader.SaveMetricsGroup(ctx, group, create, delete)
	if err != nil {
		s := "edit"
		if create {
			s = "create"
		}
		return &MetricsGroupInfo{}, fmt.Errorf("can't %s group: %w", s, err)
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
	if create {
		if !ai.canEditMetric(metric.Name, metric, metric) {
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
		v := h.metricsStorage.GetMetaMetric(metric.MetricID)
		if v == nil {
			return format.MetricMetaValue{}, httpErr(http.StatusNotFound, fmt.Errorf("metric %q not found (id %d)", metric.Name, metric.MetricID))
		}
		if v.Name != metric.Name && !ai.canChangeMetricByName(v.Name, metric.Name) {
			return format.MetricMetaValue{}, httpErr(http.StatusForbidden, fmt.Errorf("one of names %s, %s isn't acceptable", metric.Name, v.Name))
		}
		if !ai.canEditMetric(v.Name, *v, metric) {
			return format.MetricMetaValue{}, httpErr(http.StatusForbidden, fmt.Errorf("can't edit metric %q", v.Name))
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
	sl := newEndpointStat(EndpointMetricTagValues, h.getMetricIDForStat(r.FormValue(ParamMetric)), "")
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), querySelectTimeout)
	defer cancel()

	_ = r.ParseForm() // (*http.Request).FormValue ignores parse errors, too
	resp, err := h.handleGetMetricTagValues(
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

	respondJSON(w, resp, queryClientCache, queryClientCacheStale, err, h.verbose, ai.user, sl)
}

func (h *Handler) handleGetMetricTagValues(ctx context.Context, req getMetricTagValuesReq) (*GetMetricTagValuesResp, error) {
	version, err := parseVersion(req.version)
	if err != nil {
		return nil, err
	}

	numResults, err := parseNumResults(req.numResults, defTagValues, maxTagValues, false)
	if err != nil {
		return nil, err
	}

	metricMeta, err := h.getMetricMeta(req.ai, req.metricWithNamespace)
	if err != nil {
		return nil, err
	}

	err = validateQuery(metricMeta, version)
	if err != nil {
		return nil, err
	}

	tagID, err := parseTagID(req.tagID)
	if err != nil {
		return nil, err
	}

	from, to, err := parseFromTo(req.from, req.to)
	if err != nil {
		return nil, err
	}

	_, kind, err := parseQueryWhat(req.what)
	if err != nil {
		return nil, err
	}

	filterIn, filterNotIn, err := parseQueryFilter(req.filter)
	if err != nil {
		return nil, err
	}
	mappedFilterIn, err := h.resolveFilter(metricMeta, version, filterIn)
	if err != nil {
		return nil, err
	}
	mappedFilterNotIn, err := h.resolveFilter(metricMeta, version, filterNotIn)
	if err != nil {
		return nil, err
	}

	type selectRow struct {
		TagValueID int32   `ch:"_value"`
		TagValue   string  `ch:"_string_value"`
		Count      float64 `ch:"_count"`
	}

	lods := selectTagValueLODs(
		version,
		int64(metricMeta.PreKeyFrom),
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
		tagInfo[selectRow{TagValueID: format.TagValueIDProductionLegacy}] = 100 // we only support production tables for v1
	} else {
		for _, lod := range lods {
			query, args, err := tagValuesQuery(pq, lod) // we set limit to numResult+1
			if err != nil {
				return nil, err
			}

			var data []selectRow
			isFast := lod.fromSec+FastQueryTimeInterval >= lod.toSec
			err = h.doSelect(isFast, ctx, req.ai.user, version, &data, query, args...)
			if err != nil {
				return nil, err
			}
			for _, d := range data {
				tagInfo[selectRow{TagValueID: d.TagValueID, TagValue: d.TagValue}] += d.Count
			}
		}
	}

	data := make([]selectRow, 0, len(tagInfo))
	for k, count := range tagInfo {
		data = append(data, selectRow{TagValueID: k.TagValueID, TagValue: k.TagValue, Count: count})
	}
	sort.Slice(data, func(i int, j int) bool { return data[i].Count > data[j].Count })

	ret := &GetMetricTagValuesResp{
		TagValues: []MetricTagValueInfo{},
	}
	if len(data) > numResults {
		data = data[:numResults]
		ret.TagValuesMore = true
	}
	for _, d := range data {
		v := d.TagValue
		if pq.stringTag() {
			v = emptyToUnspecified(v)
		} else {
			v = h.getRichTagValue(metricMeta, version, tagID, d.TagValueID)
		}
		ret.TagValues = append(ret.TagValues, MetricTagValueInfo{
			Value: v,
			Count: d.Count,
		})
	}

	return ret, nil
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

func (h *Handler) HandleGetQuery(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointQuery, h.getMetricIDForStat(r.FormValue(ParamMetric)), r.FormValue(paramDataFormat))
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), querySelectTimeout)
	defer cancel()

	_ = r.ParseForm() // (*http.Request).FormValue ignores parse errors, too
	metricWithNamespace := formValueParamMetric(r)
	queryVerbose := false
	if _, ok := format.BuiltinMetricByName[metricWithNamespace]; !ok && r.FormValue(ParamQueryVerbose) == "1" {
		queryVerbose = true
	}

	filterIn, filterNotIn, err := parseQueryFilter(r.Form[ParamQueryFilter])
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}

	getQuery := func(ctx context.Context) (*GetQueryResp, error) {
		resp, err := h.handleGetQuery(
			ctx,
			true,
			getQueryReq{
				ai:                  ai,
				version:             r.FormValue(ParamVersion),
				numResults:          r.FormValue(ParamNumResults),
				metricWithNamespace: metricWithNamespace,
				from:                r.FormValue(ParamFromTime),
				to:                  r.FormValue(ParamToTime),
				width:               r.FormValue(ParamWidth),
				widthAgg:            r.FormValue(ParamWidthAgg),
				timeShifts:          r.Form[ParamTimeShift],
				what:                r.Form[ParamQueryWhat],
				by:                  r.Form[ParamQueryBy],
				filterIn:            filterIn,
				filterNotIn:         filterNotIn,
			})
		if h.verbose && err == nil {
			log.Printf("[debug] handled query (%v series x %v points each) for %q in %v", len(resp.Series.SeriesMeta), len(resp.Series.Time), ai.user, time.Since(sl.startTime))
		}
		return resp, err
	}

	getQueryBuiltin := func(ctx context.Context) (*GetQueryResp, error) {
		return h.handleGetQuery(
			ctx,
			false,
			getQueryReq{
				ai:                  ai.withBadgesRequest(),
				version:             Version2,
				numResults:          "20",
				metricWithNamespace: format.BuiltinMetricNameBadges,
				from:                r.FormValue(ParamFromTime),
				to:                  r.FormValue(ParamToTime),
				width:               r.FormValue(ParamWidth),
				widthAgg:            r.FormValue(ParamWidthAgg), // TODO - resolution of badge metric (currently 5s)?
				what:                []string{ParamQueryFnCountNorm, ParamQueryFnAvg},
				by:                  []string{"key1", "key2"},
				filterIn:            map[string][]string{"key2": {metricWithNamespace, format.AddRawValuePrefix("0")}},
			})
	}

	var (
		resp          *GetQueryResp
		respIngestion *GetQueryResp
	)
	if !queryVerbose {
		resp, err = getQuery(ctx)
	} else {
		var g *errgroup.Group
		g, ctx = errgroup.WithContext(ctx)
		g.Go(func() error {
			var err error
			resp, err = getQuery(ctx)
			return err
		})
		g.Go(func() error {
			var err error
			respIngestion, err = getQueryBuiltin(ctx)
			return err
		})
		err = g.Wait()
	}
	defer h.freeQueryResp(resp)
	defer h.freeQueryResp(respIngestion)

	if queryVerbose && err == nil && len(respIngestion.Series.Time) > 0 {
		for i, meta := range respIngestion.Series.SeriesMeta {
			badgeType := meta.Tags["key1"].Value
			metric := meta.Tags["key2"].Value
			if metric == metricWithNamespace {
				switch {
				case meta.What.String() == ParamQueryFnAvg && badgeType == format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeAgentSamplingFactor)):
					resp.SamplingFactorSrc = sumSeries(respIngestion.Series.SeriesData[i], 1) / float64(len(respIngestion.Series.Time))
				case meta.What.String() == ParamQueryFnAvg && badgeType == format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeAggSamplingFactor)):
					resp.SamplingFactorAgg = sumSeries(respIngestion.Series.SeriesData[i], 1) / float64(len(respIngestion.Series.Time))
				case meta.What.String() == ParamQueryFnAvg && badgeType == format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeIngestionErrorsOld)):
					resp.ReceiveErrorsLegacy = sumSeries(respIngestion.Series.SeriesData[i], 0)
				case meta.What.String() == ParamQueryFnAvg && badgeType == format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeAggMappingErrorsOld)):
					resp.MappingFloodEventsLegacy = sumSeries(respIngestion.Series.SeriesData[i], 0)
				case meta.What.String() == ParamQueryFnCountNorm && badgeType == format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeIngestionErrors)):
					resp.ReceiveErrors = sumSeries(respIngestion.Series.SeriesData[i], 0)
				case meta.What.String() == ParamQueryFnCountNorm && badgeType == format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeAggMappingErrors)):
					resp.MappingErrors = sumSeries(respIngestion.Series.SeriesData[i], 0)
				}
			}
			// TODO - show badge if some heuristics on # of contributors is triggered
			// if format.IsValueCodeZero(metric) && meta.What.String() == ParamQueryFnCountNorm && badgeType == format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeContributors)) {
			//	sumContributors := sumSeries(respIngestion.Series.SeriesData[i], 0)
			//	fmt.Printf("contributors sum %f\n", sumContributors)
			// }
		}
	}

	switch {
	case err == nil && r.FormValue(paramDataFormat) == dataFormatCSV:
		exportCSV(w, resp, metricWithNamespace, sl)
	default:
		respondJSON(w, resp, queryClientCache, queryClientCacheStale, err, h.verbose, ai.user, sl)
	}
}

func (h *Handler) freeQueryResp(resp *GetQueryResp) {
	if resp != nil {
		for i := range resp.Series.SeriesData {
			resp.Series.SeriesData[i] = nil
		}
		resp.Series.SeriesData = nil

		for i, ss := range resp.syncPoolBuffers {
			h.putFloatsSlice(ss)
			resp.syncPoolBuffers[i] = nil
		}
		resp.syncPoolBuffers = nil
	}
}

// don't forget to defer a call to h.freeQueryResp()
func (h *Handler) handleGetQuery(ctx context.Context, debugQueries bool, req getQueryReq) (resp *GetQueryResp, err error) {
	version, err := parseVersion(req.version)
	if err != nil {
		return nil, err
	}

	metricMeta, err := h.getMetricMeta(req.ai, req.metricWithNamespace)
	if err != nil {
		return nil, err
	}

	err = validateQuery(metricMeta, version)
	if err != nil {
		return nil, err
	}

	from, to, err := parseFromTo(req.from, req.to)
	if err != nil {
		return nil, err
	}

	width, widthKind, err := parseWidth(req.width, req.widthAgg)
	if err != nil {
		return nil, err
	}

	shifts, err := parseTimeShifts(req.timeShifts, width)
	if err != nil {
		return nil, err
	}

	numResultsPerShift, err := parseNumResults(
		req.numResults,
		defSeries,
		maxSeries/len(shifts),
		req.allowNegativeNumResults,
	)
	if err != nil {
		return nil, err
	}

	queries, err := parseQueries(version, req.what, req.by)
	if err != nil {
		return nil, err
	}

	mappedFilterIn, err := h.resolveFilter(metricMeta, version, req.filterIn)
	if err != nil {
		return nil, err
	}
	mappedFilterNotIn, err := h.resolveFilter(metricMeta, version, req.filterNotIn)
	if err != nil {
		return nil, err
	}

	oldestShift := shifts[0]
	isStringTop := metricMeta.StringTopDescription != ""

	isUnique := false // this parameter has meaning only for the version 1, in other cases it does nothing
	if version == Version1 {
		isUnique = queries[0].whatKind == queryFnKindUnique // we always have only one query for version 1
	}

	lods := selectQueryLODs(
		version,
		int64(metricMeta.PreKeyFrom),
		metricMeta.Resolution,
		isUnique,
		isStringTop,
		time.Now().Unix(),
		shiftTimestamp(from.Unix(), int64(width), toSec(oldestShift), h.location),
		shiftTimestamp(to.Unix(), int64(width), toSec(oldestShift), h.location),
		h.utcOffset,
		width,
		widthKind,
		h.location,
	)

	if len(lods) > 0 {
		// left shift leftmost LOD by one step to facilitate calculation of derivative (if any) in the leftmost requested point
		// NB! don't forget to exclude this extra point on the left on successful return
		lods[0].fromSec -= lods[0].stepSec

		// ensure that we can right-shift the oldest LOD to cover other shifts
		if width != _1M {
			step := lods[0].stepSec
			for _, shift := range shifts[1:] {
				shiftDelta := toSec(shift - oldestShift)
				if shiftDelta%step != 0 {
					return nil, httpErr(http.StatusBadRequest, fmt.Errorf("invalid time shift sequence %v (shift %v not divisible by %v)", shifts, shift, time.Duration(step)*time.Second))
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
	)
	defer func() {
		// return buffers to sync.Pool if error occurred (`freeQueryResp` won't be called)
		if err != nil {
			for _, s := range syncPoolBuffers {
				h.putFloatsSlice(s)
			}
		}
	}()

	var sqlQueries []string
	if debugQueries {
		ctx = debugQueriesContext(ctx, &sqlQueries)
	}

	for _, q := range queries {
		qs := normalizedQueryString(req.metricWithNamespace, q.whatKind, req.by, req.filterIn, req.filterNotIn)
		pq := &preparedPointsQuery{
			user:        req.ai.user,
			version:     version,
			metricID:    metricMeta.MetricID,
			preKeyTagID: metricMeta.PreKeyTagID,
			isStringTop: isStringTop,
			kind:        q.whatKind,
			by:          q.by,
			filterIn:    mappedFilterIn,
			filterNotIn: mappedFilterNotIn,
		}

		desiredStepMul := int64(1)
		if widthKind == widthLODRes {
			desiredStepMul = int64(width)
		} else if len(lods) > 0 {
			desiredStepMul = lods[len(lods)-1].stepSec
		}

		for _, shift := range shifts {
			type selectRowsPtr *[]*tsSelectRow
			var ( // initialized to suppress Goland's invalid "may be nil" warnings
				tagsToIx      = map[tsTags]int{}           // tags => index
				ixToTags      = make([]*tsTags, 0)         // index => tags
				ixToLodToRows = make([][]selectRowsPtr, 0) // index => ("lod index" => all rows, ordered by time)
				ixToAmount    = make([]float64, 0)         // index => total "amount"
			)

			shiftDelta := toSec(shift - oldestShift)
			for lodIx, lod := range lods {
				m, err := h.cache.Get(ctx, version, qs, pq, lodInfo{
					fromSec:   shiftTimestamp(lod.fromSec, lod.stepSec, shiftDelta, lod.location),
					toSec:     shiftTimestamp(lod.toSec, lod.stepSec, shiftDelta, lod.location),
					stepSec:   lod.stepSec,
					table:     lod.table,
					hasPreKey: lod.hasPreKey,
					location:  h.location,
				})
				if err != nil {
					return nil, err
				}

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
						v := math.Abs(selectTSValue(q.what, lod.stepSec, desiredStepMul, &rows[i]))
						ixToAmount[ix] += v * v * float64(lod.stepSec)
					}
				}
			}

			sortedIxs := make([]int, 0, len(ixToAmount))
			for i := range ixToAmount {
				sortedIxs = append(sortedIxs, i)
			}

			if numResultsPerShift > 0 {
				partialSortIndexByValueDesc(sortedIxs, ixToAmount, numResultsPerShift)
				if len(sortedIxs) > numResultsPerShift {
					sortedIxs = sortedIxs[:numResultsPerShift]
				}
			}

			for _, i := range sortedIxs {
				tags := ixToTags[i]
				kvs := make(map[string]SeriesMetaTag, 16)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(0), tags.Tag0)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(1), tags.Tag1)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(2), tags.Tag2)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(3), tags.Tag3)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(4), tags.Tag4)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(5), tags.Tag5)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(6), tags.Tag6)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(7), tags.Tag7)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(8), tags.Tag8)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(9), tags.Tag9)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(10), tags.Tag10)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(11), tags.Tag11)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(12), tags.Tag12)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(13), tags.Tag13)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(14), tags.Tag14)
				h.maybeAddQuerySeriesTagValue(kvs, metricMeta, version, q.by, format.TagID(15), tags.Tag15)
				h.maybeAddQuerySeriesTagValueString(kvs, q.by, format.StringTopTagID, &tags.STag)

				ts := h.getFloatsSlice(len(allTimes))
				syncPoolBuffers = append(syncPoolBuffers, ts)

				var maxHosts []string
				if (q.what == queryFnMaxHost || q.what == queryFnMaxCountHost) && version == Version2 {
					maxHosts = make([]string, len(*ts))
				}
				for i := range *ts {
					(*ts)[i] = math.NaN() // will become JSON null
				}
				base := 0
				for lodIx, rows := range ixToLodToRows[i] {
					if rows != nil {
						lod := lods[lodIx]
						for _, row := range *rows {
							lodTimeIx := lod.getIndexForTimestamp(row.Time, shiftDelta)
							(*ts)[base+lodTimeIx] = selectTSValue(q.what, lod.stepSec, desiredStepMul, row)
							if maxHosts != nil {
								// mapping every time is not optimal, but mapping to store in cache is also not optimal. TODO - optimize?
								tagValueID := int32(row.Val5)
								label, err := h.getTagValue(tagValueID)
								if err != nil {
									label = format.CodeTagValue(tagValueID)
								}
								maxHosts[base+lodTimeIx] = label
							}
						}
					}
					base += len(lodTimes[lodIx])
				}
				switch q.what {
				case queryFnCumulCount, queryFnCumulAvg, queryFnCumulSum, queryFnCumulCardinality:
					// starts from 1 to exclude extra point on the left
					accumulateSeries((*ts)[1:])
				case queryFnDerivativeCount, queryFnDerivativeCountNorm, queryFnDerivativeAvg,
					queryFnDerivativeSum, queryFnDerivativeSumNorm, queryFnDerivativeMin,
					queryFnDerivativeMax, queryFnDerivativeUnique, queryFnDerivativeUniqueNorm:
					// Extra point on the left was needed for this case
					differentiateSeries(*ts)
				}

				// exclude extra point on the left from final slice
				s := (*ts)[1:]
				if maxHosts != nil {
					maxHosts = maxHosts[1:]
				}

				meta = append(meta, QuerySeriesMetaV2{TimeShift: toSec(shift), Tags: kvs, MaxHosts: maxHosts, What: q.what})
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
	return &GetQueryResp{
		Series: querySeries{
			Time:       allTimes,
			SeriesMeta: meta,
			SeriesData: data,
		},
		DebugQueries:    sqlQueries,
		MetricMeta:      metricMeta,
		syncPoolBuffers: syncPoolBuffers,
	}, nil
}

func (h *Handler) HandleGetRender(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointRender, h.getMetricIDForStat(r.FormValue(ParamMetric)), r.FormValue(paramDataFormat))
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), querySelectTimeout)
	defer cancel()

	_ = r.ParseForm() // (*http.Request).FormValue ignores parse errors, too
	var (
		from  = r.FormValue(ParamFromTime)
		to    = r.FormValue(ParamToTime)
		s     []getQueryReq
		tabTo = 12 // maximum number of tabs to render
	)
	tabFrom, err := strconv.Atoi(r.FormValue(paramTabNumber))
	if err != nil {
		tabFrom = 0 // tab isn't specified, assume first
	}
	if tabFrom != -1 {
		tabTo = tabFrom + 1 // render single tab
	} else {
		tabFrom = 0 // render all tabs, tabTo remains intact
	}
	for i := tabFrom; i < tabTo; i++ {
		var p string
		if i == 0 {
			p = ""
		} else {
			p = fmt.Sprintf("t%d.", i)
		}

		paramMetric := p + ParamMetric
		metricWithNamespace := r.FormValue(paramMetric)
		if metricWithNamespace == "" {
			break
		}

		paramQueryFilter := p + ParamQueryFilter
		filterIn, filterNotIn, err := parseQueryFilter(r.Form[paramQueryFilter])
		if err != nil {
			respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
			return
		}

		var (
			paramVersion    = p + ParamVersion
			paramNumResults = p + ParamNumResults
			paramQueryBy    = p + ParamQueryBy
			paramTimeShift  = p + ParamTimeShift
			paramWidthAgg   = p + ParamWidthAgg
			paramWidth      = p + ParamWidth
			paramQueryWhat  = p + ParamQueryWhat
		)
		s = append(s, getQueryReq{
			ai:                  ai,
			version:             r.FormValue(paramVersion),
			numResults:          r.FormValue(paramNumResults),
			metricWithNamespace: metricWithNamespace,
			from:                from,
			to:                  to,
			width:               r.FormValue(paramWidth),
			widthAgg:            r.FormValue(paramWidthAgg),
			timeShifts:          r.Form[paramTimeShift],
			what:                r.Form[paramQueryWhat],
			by:                  r.Form[paramQueryBy],
			filterIn:            filterIn,
			filterNotIn:         filterNotIn,
		})
	}

	resp, err := h.handleGetRender(
		ctx,
		getRenderReq{
			ai:           ai,
			getQueryReq:  s,
			renderWidth:  r.FormValue(paramRenderWidth),
			renderFormat: r.FormValue(paramDataFormat),
		})
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}

	respondPlot(w, resp.format, resp.data, queryClientCache, queryClientCacheStale, h.verbose, ai.user, sl)
}

func (h *Handler) HandleGetDashboard(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointDashboard, h.getMetricIDForStat(r.FormValue(ParamID)), "")
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
	resp, cache, err := h.handleGetDashboard(ai, int32(id))
	respondJSON(w, resp, cache, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandleGetGroup(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointGroup, 0, "")
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
	resp, cache, err := h.handleGetGroup(ai, int32(id))
	respondJSON(w, resp, cache, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandleGetGroupsList(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointGroup, 0, "")
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}
	resp, cache, err := h.handleGetGroupsList(ai)
	respondJSON(w, resp, cache, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandleGetDashboardList(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStat(EndpointDashboard, 0, "")
	ai, ok := h.parseAccessToken(w, r, sl)
	if !ok {
		return
	}
	resp, cache, err := h.handleGetDashboardList(ai)
	respondJSON(w, resp, cache, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandlePutPostDashboard(w http.ResponseWriter, r *http.Request) {
	if h.checkReadOnlyMode(w, r) {
		return
	}
	ai, ok := h.parseAccessToken(w, r, nil)
	if !ok {
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxMetricHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	if len(res) >= maxMetricHTTPBodySize {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("metric body too big. Max size is %d bytes", maxMetricHTTPBodySize)), h.verbose, ai.user, nil)
		return
	}
	var dashboard DashboardInfo
	if err := easyjson.Unmarshal(res, &dashboard); err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	d, err := h.handlePostDashboard(r.Context(), ai, dashboard.Dashboard, r.Method == http.MethodPut, dashboard.Delete)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	err = h.waitVersionUpdate(r.Context(), d.Dashboard.Version)
	respondJSON(w, d, defaultCacheTTL, 0, err, h.verbose, ai.user, nil)
}

func (h *Handler) handleGetRender(ctx context.Context, req getRenderReq) (*getRenderResp, error) {
	width, err := parseRenderWidth(req.renderWidth)
	if err != nil {
		return nil, err
	}

	format_, err := parseRenderFormat(req.renderFormat)
	if err != nil {
		return nil, err
	}

	var (
		s         = make([]*GetQueryResp, len(req.getQueryReq))
		seriesNum = 0
		pointsNum = 0
	)
	for i, r := range req.getQueryReq {
		start := time.Now()
		data, err := h.handleGetQuery(ctx, false, r)
		defer h.freeQueryResp(data) // yes, hold until plot call
		if err != nil {
			return nil, err
		}
		if h.verbose {
			log.Printf("[debug] handled render query (%v series x %v points each) for %q in %v", len(data.Series.SeriesMeta), len(data.Series.Time), r.ai.user, time.Since(start))
			seriesNum += len(data.Series.SeriesMeta)
			pointsNum += len(data.Series.SeriesMeta) * len(data.Series.Time)
		}
		s[i] = data
	}

	ctx, cancel := context.WithTimeout(ctx, plotRenderTimeout)
	defer cancel()

	err = h.plotRenderSem.Acquire(ctx, 1)
	if err != nil {
		return nil, err
	}
	defer h.plotRenderSem.Release(1)

	start := time.Now()
	png, err := plot(ctx, format_, true, s, h.utcOffset, req.getQueryReq, width, h.plotTemplate)
	if err != nil {
		return nil, err
	}
	if h.verbose {
		log.Printf("[debug] handled render plot (%v series, %v points) for %q in %v", seriesNum, pointsNum, req.ai.user, time.Since(start))
	}

	return &getRenderResp{
		format: format_,
		data:   png,
	}, nil
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

func (h *Handler) maybeAddQuerySeriesTagValue(m map[string]SeriesMetaTag, metricMeta *format.MetricMetaValue, version string, by []string, tagID string, id int32) {
	if containsString(by, tagID) {
		metaTag := SeriesMetaTag{Value: h.getRichTagValue(metricMeta, version, tagID, id)}
		if tag, ok := metricMeta.Name2Tag[tagID]; ok {
			metaTag.Comment = tag.ValueComments[metaTag.Value]
			metaTag.Raw = tag.Raw
			metaTag.RawKind = tag.RawKind
		}
		m[tagID] = metaTag
	}
}

func (h *Handler) maybeAddQuerySeriesTagValueString(m map[string]SeriesMetaTag, by []string, tagName string, tagValuePtr *stringFixed) {
	tagValue := ""
	nullIx := bytes.IndexByte(tagValuePtr[:], 0)
	switch nullIx {
	case 0: // do nothing
	case -1:
		tagValue = string(tagValuePtr[:])
	default:
		tagValue = string(tagValuePtr[:nullIx])
	}

	if containsString(by, tagName) {
		m[tagName] = SeriesMetaTag{Value: emptyToUnspecified(tagValue)}
	}
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

func (h *Handler) loadPoints(ctx context.Context, pq *preparedPointsQuery, lod lodInfo, ret [][]tsSelectRow, retStartIx int) error {
	query, args, err := loadPointsQuery(pq, lod, h.utcOffset)
	if err != nil {
		return err
	}

	var data []tsSelectRow
	start := time.Now()
	isFast := lod.fromSec+FastQueryTimeInterval >= lod.toSec
	err = h.doSelect(isFast, ctx, pq.user, pq.version, &data, query, args...)
	if err != nil {
		return err
	}
	if len(data) == maxSeriesRows {
		return fmt.Errorf("can't fetch more than %v rows", maxSeriesRows) // prevent cache being populated by incomplete data
	}
	if h.verbose {
		log.Printf("[debug] loaded %v rows from %v (%v timestamps, %v to %v step %v) for %q in %v",
			len(data),
			lod.table,
			(lod.toSec-lod.fromSec)/lod.stepSec,
			time.Unix(lod.fromSec, 0),
			time.Unix(lod.toSec, 0),
			time.Duration(lod.stepSec)*time.Second,
			pq.user,
			time.Since(start),
		)
	}

	for _, row := range data {
		if !isTimestampValid(row.Time, lod.stepSec, h.utcOffset, h.location) {
			log.Printf("[warning] got invalid timestamp while loading for %q, ignoring: %d is not a multiple of %v", pq.user, row.Time, lod.stepSec)
			continue
		}

		replaceInfNan(&row.CountNorm)
		replaceInfNan(&row.Val0)
		replaceInfNan(&row.Val1)
		replaceInfNan(&row.Val2)
		replaceInfNan(&row.Val3)
		replaceInfNan(&row.Val4)
		replaceInfNan(&row.Val5)
		replaceInfNan(&row.Val6)

		ix := retStartIx + lod.getIndexForTimestamp(row.Time, 0)
		ret[ix] = append(ret[ix], row)
	}

	return nil
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

func selectTSValue(what queryFn, stepMul int64, desiredStepMul int64, row *tsSelectRow) float64 {
	if stepMul == _1M {
		desiredStepMul = row.StepSec
	}
	switch what {
	case queryFnCount, queryFnMaxCountHost, queryFnDerivativeCount:
		return stableMulDiv(row.CountNorm, desiredStepMul, row.StepSec)
	case queryFnCountNorm, queryFnDerivativeCountNorm:
		return row.CountNorm / float64(row.StepSec)
	case queryFnCumulCount:
		return row.CountNorm
	case queryFnCardinality:
		return stableMulDiv(row.Val0, desiredStepMul, row.StepSec)
	case queryFnCardinalityNorm:
		return row.Val0 / float64(row.StepSec)
	case queryFnCumulCardinality:
		return row.Val0
	case queryFnMin, queryFnDerivativeMin:
		return row.Val0
	case queryFnMax, queryFnMaxHost, queryFnDerivativeMax:
		return row.Val1
	case queryFnAvg, queryFnCumulAvg, queryFnDerivativeAvg:
		return row.Val2
	case queryFnSum, queryFnDerivativeSum:
		return stableMulDiv(row.Val3, desiredStepMul, row.StepSec)
	case queryFnSumNorm, queryFnDerivativeSumNorm:
		return row.Val3 / float64(row.StepSec)
	case queryFnCumulSum:
		return row.Val3
	case queryFnStddev:
		return row.Val4
	case queryFnP25:
		return row.Val0
	case queryFnP50:
		return row.Val1
	case queryFnP75:
		return row.Val2
	case queryFnP90:
		return row.Val3
	case queryFnP95:
		return row.Val4
	case queryFnP99:
		return row.Val5
	case queryFnP999:
		return row.Val6
	case queryFnUnique, queryFnDerivativeUnique:
		return stableMulDiv(row.Val0, desiredStepMul, row.StepSec)
	case queryFnUniqueNorm, queryFnDerivativeUniqueNorm:
		return row.Val0 / float64(row.StepSec)
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

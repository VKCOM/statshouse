// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/VKCOM/statshouse-go"
	"github.com/gorilla/mux"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/promql"
	"github.com/VKCOM/statshouse/internal/promql/parser"
)

var errQueryOutOfRange = fmt.Errorf("exceeded maximum resolution of %d points per timeseries", data_model.MaxSlice)

func HandleInstantQuery(r *httpRequestHandler) {
	// parse query
	q := promql.Query{
		Expr: r.FormValue("query"),
		Options: promql.Options{
			Version:       r.version,
			Version3Start: r.Version3Start.Load(),
			Mode:          data_model.InstantQuery,
			Compat:        true,
			TimeNow:       time.Now().Unix(),
			Namespace:     r.Header.Get("X-StatsHouse-Namespace"),
		},
	}
	w := r.Response()
	if t := r.FormValue("time"); t == "" {
		q.Start = q.Options.TimeNow
	} else {
		var err error
		q.Start, err = parseTime(t)
		if err != nil {
			promRespondError(w, promErrorBadData, fmt.Errorf("invalid parameter time: %w", err))
			return
		}
	}
	q.End = q.Start + 1 // handler expects half open interval [start, end)
	// execute query
	ctx, cancel := context.WithTimeout(r.Context(), r.querySelectTimeout)
	defer cancel()
	res, dispose, err := r.promEngine.Exec(ctx, r, q)
	if err != nil {
		promRespondError(w, promErrorExec, err)
		return
	}
	defer dispose()
	promRespond(w, promResponseData{ResultType: res.Type(), Result: res})
}

func HandleRangeQuery(r *httpRequestHandler) {
	// parse query
	q := promql.Query{
		Expr: r.FormValue("query"),
		Options: promql.Options{
			Version:       r.version,
			Version3Start: r.Version3Start.Load(),
			Compat:        true,
			Namespace:     r.Header.Get("X-StatsHouse-Namespace"),
		},
	}
	var err error
	q.Start, err = parseTime(r.FormValue("start"))
	w := r.Response()
	if err != nil {
		promRespondError(w, promErrorBadData, fmt.Errorf("invalid parameter start: %w", err))
		return
	}
	q.End, err = parseTime(r.FormValue("end"))
	if err != nil {
		promRespondError(w, promErrorBadData, fmt.Errorf("invalid parameter end: %w", err))
		return
	}
	q.End++ // handler expects half open interval [start, end)
	q.Step, err = parseDuration(r.FormValue("step"))
	if err != nil {
		promRespondError(w, promErrorBadData, fmt.Errorf("invalid parameter step: %w", err))
		return
	}
	// execute query
	ctx, cancel := context.WithTimeout(r.Context(), r.querySelectTimeout)
	defer cancel()
	res, dispose, err := r.promEngine.Exec(ctx, r, q)
	if err != nil {
		promRespondError(w, promErrorExec, err)
		return
	}
	defer dispose()
	promRespond(w, promResponseData{ResultType: res.Type(), Result: res})
}

func HandlePromSeriesQuery(r *httpRequestHandler) {
	_ = r.ParseForm()
	match := r.Form["match[]"]
	w := r.Response()
	if len(match) == 0 {
		promRespondError(w, promErrorBadData, fmt.Errorf("no match[] parameter provided"))
		return
	}
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		promRespondError(w, promErrorBadData, fmt.Errorf("invalid parameter start: %w", err))
		return
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		promRespondError(w, promErrorBadData, fmt.Errorf("invalid parameter end: %w", err))
		return
	}
	if start < end-300 {
		start = end - 300
	}
	end++ // handler expects half open interval [start, end)
	var res []labels.Labels
	for _, expr := range match {
		func() {
			val, cancel, err := r.promEngine.Exec(
				r.Context(), r,
				promql.Query{
					Start: start,
					End:   end,
					Expr:  expr,
					Options: promql.Options{
						Version:          r.version,
						Version3Start:    r.Version3Start.Load(),
						NewShardingStart: r.NewShardingStart.Load(),
						Limit:            1000,
						Mode:             data_model.TagsQuery,
						Namespace:        r.Header.Get("X-StatsHouse-Namespace"),
					},
				})
			if err != nil {
				return
			}
			defer cancel()
			if ts, _ := val.(*promql.TimeSeries); ts != nil {
				for _, series := range ts.Series.Data {
					var tags labels.Labels
					for _, v := range series.Tags.ID2Tag {
						tags = append(tags, labels.Label{Name: v.GetName(), Value: v.SValue})
					}
					res = append(res, tags)
				}
			}
		}()
	}
	promRespond(w, res)
}

func HandlePromLabelsQuery(r *httpRequestHandler) {
	namespace := r.Header.Get("X-StatsHouse-Namespace")
	var prefix string
	switch namespace {
	case "", "__default":
		// no prefix
	default:
		prefix = namespace + format.NamespaceSeparator
	}
	_ = r.ParseForm()
	var res []string
	for _, expr := range r.Form["match[]"] {
		if ast, err := parser.ParseExpr(expr); err == nil {
			for _, s := range parser.ExtractSelectors(ast) {
				for _, sel := range s {
					if sel.Name == "__name__" {
						metricName := prefix + sel.Value
						if meta := r.metricsStorage.GetMetaMetricByName(metricName); meta != nil {
							res = meta.AppendTagNames(res)
						}
					}
				}
			}
		}
	}
	promRespond(r.Response(), res)
}

func HandlePromLabelValuesQuery(r *httpRequestHandler) {
	namespace := r.Header.Get("X-StatsHouse-Namespace")
	var prefix string
	switch namespace {
	case "", "__default":
		// no prefix
	default:
		prefix = namespace + format.NamespaceSeparator
	}
	var res []string
	tagName := mux.Vars(r.Request)["name"]
	w := r.Response()
	if tagName == "__name__" {
		for _, meta := range r.metricsStorage.GetMetaMetricList(r.showInvisible) {
			trimmed := strings.TrimPrefix(meta.Name, prefix)
			if (prefix == "" || meta.Name != trimmed) && r.accessInfo.CanViewMetric(*meta) {
				res = append(res, trimmed)
			}
		}
	} else {
		if tagName != format.StringTopTagID {
			// StatsHouse tags have numeric names (indices) but Grafana forbids them,
			// allow "_" prefix to workaround
			tagName = strings.TrimPrefix(tagName, "_")
		}
		if tagName != "" {
			_ = r.ParseForm()
			start, err := parseTime(r.FormValue("start"))
			if err != nil {
				promRespondError(w, promErrorBadData, fmt.Errorf("invalid parameter start: %w", err))
				return
			}
			end, err := parseTime(r.FormValue("end"))
			if err != nil {
				promRespondError(w, promErrorBadData, fmt.Errorf("invalid parameter end: %w", err))
				return
			}
			if start < end-300 {
				start = end - 300
			}
			end++ // handler expects half open interval [start, end)
			for _, expr := range r.Form["match[]"] {
				func() {
					val, cancel, err := r.promEngine.Exec(
						r.Context(), r,
						promql.Query{
							Start: start,
							End:   end,
							Expr:  expr,
							Options: promql.Options{
								Version:          r.version,
								Version3Start:    r.Version3Start.Load(),
								NewShardingStart: r.NewShardingStart.Load(),
								Limit:            1000,
								Mode:             data_model.TagsQuery,
								GroupBy:          []string{tagName},
								Namespace:        r.Header.Get("X-StatsHouse-Namespace"),
							},
						})
					if err != nil {
						return
					}
					defer cancel()
					if ts, _ := val.(*promql.TimeSeries); ts != nil {
						for _, series := range ts.Series.Data {
							if v, _ := series.Tags.Get(tagName); v != nil && v.SValue != "" {
								res = append(res, v.SValue)
							}
						}
					}
				}()
			}
		}
	}
	promRespond(w, res)
}

// region Request

func parseTime(s string) (int64, error) {
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(math.Round(v)), nil
	}
	if v, err := time.Parse(time.RFC3339, s); err == nil {
		return v.Unix(), nil
	}
	return 0, fmt.Errorf("cannot parse %qs to a valid timestamp", s)
}

func parseDuration(s string) (int64, error) {
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		v = math.Round(v)
		if v <= 0 {
			v = 1
		}
		return int64(v), nil
	}
	if v, err := model.ParseDuration(s); err == nil {
		return int64(math.Round(float64(v) / float64(time.Second))), nil
	}
	return 0, fmt.Errorf("cannot parse %qs to a valid duration", s)
}

// endregion Request

// region Response

const (
	promStatusSuccess promStatus = "success"
	promStatusError   promStatus = "error"
)

type promStatus string
type promErrorType string

const (
	promErrorTimeout  promErrorType = "timeout"
	promErrorCanceled promErrorType = "canceled"
	promErrorExec     promErrorType = "execution"
	promErrorBadData  promErrorType = "bad_data"
	promErrorInternal promErrorType = "internal"
	promErrorNotFound promErrorType = "not_found"
)

type promResponse struct {
	Status    promStatus    `json:"status"`
	Data      interface{}   `json:"data,omitempty"`
	ErrorType promErrorType `json:"errorType,omitempty"`
	Error     string        `json:"error,omitempty"`
	Warnings  []string      `json:"warnings,omitempty"`
}

type promResponseData struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     interface{}      `json:"result"`
}

func promRespond(w http.ResponseWriter, data interface{}) {
	statusMessage := promStatusSuccess
	b, err := json.Marshal(&promResponse{
		Data:   data,
		Status: statusMessage,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		log.Printf("[error] error writing prometheus API response (%v bytes written): %v", n, err)
	}
}

func promRespondError(w http.ResponseWriter, typ promErrorType, err error) {
	b, err := json.Marshal(&promResponse{
		Status:    promStatusError,
		ErrorType: typ,
		Error:     err.Error(),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var code int
	switch typ {
	case promErrorBadData:
		code = http.StatusBadRequest
	case promErrorExec:
		code = http.StatusUnprocessableEntity
	case promErrorCanceled, promErrorTimeout:
		code = http.StatusServiceUnavailable
	case promErrorInternal:
		code = http.StatusInternalServerError
	case promErrorNotFound:
		code = http.StatusNotFound
	default:
		code = http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if n, err := w.Write(b); err != nil {
		log.Printf("[error] error writing prometheus API response (%v bytes written): %v", n, err)
	}
}

// endregion

func (h *requestHandler) MatchMetrics(f *data_model.QueryFilter) error {
	h.metricsStorage.MatchMetrics(f)
	for _, metric := range f.MatchingMetrics {
		if !h.accessInfo.CanViewMetric(*metric) {
			return httpErr(http.StatusForbidden, fmt.Errorf("metric %q forbidden", metric.Name))
		}
	}
	return nil
}

func (h *requestHandler) GetHostName(hostID int32) string {
	v, err := h.getTagValue(hostID)
	if err != nil {
		return format.CodeTagValue(hostID)
	}
	return v
}

func (h *requestHandler) GetHostName64(hostID int64) string {
	if hostID < math.MinInt32 || math.MaxInt32 < hostID {
		return format.CodeTagValue64(hostID)
	}
	return h.GetHostName(int32(hostID))
}

func (h *requestHandler) GetTagValue(qry promql.TagValueQuery) string {
	var tagID string
	if len(qry.TagID) == 0 {
		tagID = format.TagID(qry.TagIndex)
	} else {
		tagID = qry.TagID
	}
	return h.getRichTagValue(qry.Metric, qry.Version, tagID, qry.TagValueID)
}

func (h *requestHandler) GetTagValueID(qry promql.TagValueIDQuery) (int64, error) {
	res, err := h.getRichTagValueID(&qry.Tag, qry.Version, qry.TagValue)
	if err != nil {
		var httpErr httpError
		if errors.As(err, &httpErr) && httpErr.code == http.StatusNotFound {
			err = promql.ErrNotFound
		}
	}
	return res, err
}

func (ev *requestHandler) GetTagFilter(metric *format.MetricMetaValue, tagIndex int, tagValue string) (data_model.TagValue, error) {
	if tagValue == "" {
		return data_model.NewTagValue("", 0), nil
	}
	if format.HasRawValuePrefix(tagValue) {
		v, err := format.ParseCodeTagValue(tagValue)
		if err != nil {
			return data_model.TagValue{}, err
		}
		if v != 0 {
			return data_model.NewTagValueM(v), nil
		} else {
			return data_model.NewTagValue("", 0), nil
		}
	}
	var t format.MetricMetaTag
	if 0 <= tagIndex && tagIndex < len(metric.Tags) {
		t = metric.Tags[tagIndex]
		if t.Raw {
			// histogram bucket label
			if t.Name == labels.BucketLabel {
				if v, err := strconv.ParseFloat(tagValue, 32); err == nil {
					return data_model.NewTagValueM(int64(statshouse.LexEncode(float32(v)))), nil
				}
			}
			// mapping from raw value comments
			var s string
			for k, v := range t.ValueComments {
				if v == tagValue {
					if s != "" {
						return data_model.TagValue{}, fmt.Errorf("ambiguous comment to value mapping")
					}
					s = k
				}
			}
			if s != "" {
				v, err := format.ParseCodeTagValue(s)
				if err != nil {
					return data_model.TagValue{}, err
				}
				return data_model.NewTagValueM(v), nil
			}
		}
	}
	v, err := ev.GetTagValueID(promql.TagValueIDQuery{
		Version:  ev.version,
		Tag:      t,
		TagValue: tagValue,
	})
	switch err {
	case nil:
		return data_model.NewTagValue(tagValue, v), nil
	case promql.ErrNotFound:
		return data_model.NewTagValue(tagValue, format.TagValueIDDoesNotExist), nil
	default:
		return data_model.TagValue{}, err
	}
}

func (h *requestHandler) QuerySeries(ctx context.Context, qry *promql.SeriesQuery) (promql.Series, func(), error) {
	if !h.accessInfo.CanViewMetricName(qry.Metric.Name) {
		return promql.Series{}, func() {}, httpErr(http.StatusForbidden, fmt.Errorf("metric %q forbidden", qry.Metric.Name))
	}
	if qry.Options.Mode == data_model.PointQuery {
		for _, what := range qry.Whats {
			switch what.Digest {
			case promql.DigestCount, promql.DigestMin, promql.DigestMax, promql.DigestAvg,
				promql.DigestSum, promql.DigestP25, promql.DigestP50, promql.DigestP75,
				promql.DigestP90, promql.DigestP95, promql.DigestP99, promql.DigestP999,
				promql.DigestUnique:
				// pass
			default:
				return promql.Series{}, func() {}, fmt.Errorf("function %s is not supported", what.Digest.String())
			}
		}
	}
	var step int64
	if qry.Range != 0 {
		step = qry.Range
	} else {
		step = qry.Timescale.Step
	}
	res := promql.Series{Meta: promql.SeriesMeta{Metric: qry.Metric}}
	if len(qry.Whats) == 1 {
		switch qry.Whats[0].Digest {
		case promql.DigestCount, promql.DigestCountSec, promql.DigestCountRaw,
			promql.DigestStdVar, promql.DigestCardinality, promql.DigestCardinalitySec,
			promql.DigestCardinalityRaw, promql.DigestUnique, promql.DigestUniqueSec:
			// measure units does not apply to counters
		default:
			res.Meta.Units = qry.Metric.MetricType
		}
	}
	var lods []data_model.LOD
	if qry.Options.Mode == data_model.PointQuery {
		lod0 := qry.Timescale.LODs[0]
		start := qry.Timescale.Time[0]
		metric := qry.Metric
		lods = []data_model.LOD{{
			FromSec:     qry.Timescale.Time[0] - qry.Offset,
			ToSec:       qry.Timescale.Time[1] - qry.Offset,
			StepSec:     lod0.Step,
			Version:     qry.Options.Version,
			Metric:      qry.Metric,
			NewSharding: h.newSharding(qry.Metric, start),
			HasPreKey:   metric.PreKeyOnly || (metric.PreKeyFrom != 0 && int64(metric.PreKeyFrom) <= start),
			PreKeyOnly:  metric.PreKeyOnly,
			Location:    h.location,
		}}
	} else {
		lods = qry.Timescale.GetLODs(qry.Metric, qry.Offset)
	}
	type tagValue struct {
		tsValues
		x  int // series index
		tx int // time index
	}
	tagX := make(map[tsTags]tagValue, len(qry.GroupBy))
	var buffers []*[]float64
	cleanup := func() {
		for _, s := range buffers {
			h.putFloatsSlice(s)
		}
	}
	var succeeded bool
	defer func() {
		if !succeeded {
			cleanup()
		}
	}()
	for _, what := range h.getHandlerWhat(qry.Whats) {
		var tx int // time index
		for _, lod := range lods {
			pq := queryBuilder{
				version:          h.version,
				user:             h.accessInfo.user,
				metric:           qry.Metric,
				what:             what.qry,
				by:               qry.GroupBy,
				filterIn:         qry.FilterIn,
				filterNotIn:      qry.FilterNotIn,
				strcmpOff:        h.Version3StrcmpOff.Load(),
				minMaxHost:       qry.MinMaxHost,
				utcOffset:        h.utcOffset,
				point:            qry.Options.Mode == data_model.PointQuery,
				play:             qry.Options.Play,
				newShardingStart: h.NewShardingStart.Load(),
			}
			switch qry.Options.Mode {
			case data_model.PointQuery:
				data, err := h.pointsCache.get(ctx, h, &pq, lod, qry.Options.AvoidCache)
				if err != nil {
					return promql.Series{}, nil, err
				}
				for i := 0; i < len(data); i++ {
					tag, ok := tagX[data[i].tsTags]
					if !ok {
						tag.x = len(res.Data)
						tagX[data[i].tsTags] = tag
						for _, fn := range what.sel {
							v := h.Alloc(len(qry.Timescale.Time))
							buffers = append(buffers, v)
							for y := range *v {
								(*v)[y] = promql.NilValue
							}
							res.Data = append(res.Data, promql.SeriesData{
								Values: v,
								What:   fn,
							})
						}
					}
					// select "point" value
					for y := 0; y < len(what.sel); y++ {
						(*res.Data[tag.x+y].Values)[tx] = data[i].value(what.sel[y].Digest, what.qry[y].Argument, 1, 1)
					}

				}
				tx++
			case data_model.RangeQuery, data_model.InstantQuery:
				data, err := cacheGet(ctx, h, &pq, lod, qry.Options.AvoidCache)
				if err != nil {
					return promql.Series{}, nil, err
				}
				for i := 0; i < len(data); i++ {
					if len(data[i]) == 0 {
						continue
					}
					k := tx + i
					for j := 0; j < len(data[i]); j++ {
						tagV, ok := tagX[data[i][j].tsTags]
						if !ok {
							x := len(res.Data)
							if x > maxSeriesRows {
								return promql.Series{}, nil, errTooManyRows
							}
							tagX[data[i][j].tsTags] = tagValue{
								tsValues: data[i][j].tsValues,
								x:        x,
								tx:       k,
							}
							for k := 0; k < len(what.sel); k++ {
								v := h.Alloc(len(qry.Timescale.Time))
								buffers = append(buffers, v)
								for y := range *v {
									(*v)[y] = promql.NilValue
								}
								var h [2][]chutil.ArgMinMaxStringFloat32
								for z, qryHost := range qry.MinMaxHost {
									if qryHost {
										h[z] = make([]chutil.ArgMinMaxStringFloat32, len(qry.Timescale.Time))
									}
								}
								res.Data = append(res.Data, promql.SeriesData{
									Values:     v,
									MinMaxHost: h,
									What:       what.sel[k],
								})
							}
						} else {
							if k == 0 || tagV.tx != k {
								tagV.tx = k
								tagV.tsValues = data[i][j].tsValues
							} else {
								tagV.tsValues.merge(data[i][j].tsValues)
							}
							tagX[data[i][j].tsTags] = tagV
						}
					}
					for _, tagV := range tagX {
						if tagV.tx == k {
							what.copyRowValuesAt(res.Data, tagV.x, k, &tagV.tsValues, step, lod.StepSec)
						}
					}
				}
				tx += len(data)
			case data_model.TagsQuery:
				data, err := cacheGet(ctx, h, &pq, lod, qry.Options.AvoidCache)
				if err != nil {
					return promql.Series{}, nil, err
				}
				for i := 0; i < len(data); i++ {
					for j := 0; j < len(data[i]); j++ {
						if _, ok := tagX[data[i][j].tsTags]; !ok {
							tagX[data[i][j].tsTags] = tagValue{x: len(tagX)}
						}
					}
				}
				tx += len(data)
			default:
				return promql.Series{}, func() {}, fmt.Errorf("query mode %v is not supported", qry.Options.Mode)
			}
		}
		if qry.Options.Mode == data_model.TagsQuery {
			res.Data = make([]promql.SeriesData, len(tagX))
		}
		tagWhat := len(qry.Whats) > 1 || qry.Options.TagWhat
		for i := 0; i < len(what.sel); i++ {
			what := what.sel[i]
			for v, tag := range tagX {
				for _, x := range qry.GroupBy {
					switch x {
					case format.StringTopTagIndex, format.StringTopTagIndexV3:
						res.AddTagAt(i+tag.x, &promql.SeriesTag{
							Metric: qry.Metric,
							Index:  format.StringTopTagIndex + promql.SeriesTagIndexOffset,
							ID:     format.StringTopTagID,
							Name:   qry.Metric.StringTopName,
							Value:  v.tag[format.StringTopTagIndexV3],
							SValue: v.stag[format.StringTopTagIndexV3],
						})
					case format.ShardTagIndex:
						res.AddTagAt(i+tag.x, &promql.SeriesTag{
							Metric: qry.Metric,
							ID:     promql.LabelShard,
							Value:  int64(v.shardNum),
						})
					default:
						if 0 <= x && x < len(v.tag) && x < len(qry.Metric.Tags) {
							st := &promql.SeriesTag{
								Metric: qry.Metric,
								Index:  x + promql.SeriesTagIndexOffset,
								ID:     format.TagID(x),
								Name:   qry.Metric.Tags[x].Name,
								Value:  v.tag[x],
							}
							if qry.Options.Version == Version3 && x < len(v.tag) && v.stag[x] != "" {
								st.SetSValue(v.stag[x])
							}
							res.AddTagAt(i+tag.x, st)
						}
					}
				}
				if tagWhat {
					res.AddTagAt(i+tag.x, &promql.SeriesTag{
						ID:    promql.LabelWhat,
						Value: int64(what.Digest),
					})
				}
			}
			if qry.Options.Mode == data_model.TagsQuery {
				break
			}
		}
		tagX = make(map[tsTags]tagValue, len(tagX))
	}
	res.Meta.Total = len(res.Data)
	h.reportQueryMemUsage(len(res.Data), len(qry.Timescale.Time))
	succeeded = true // prevents deffered "cleanup"
	return res, cleanup, nil
}

func (h *requestHandler) QueryTagValueIDs(ctx context.Context, qry promql.TagValuesQuery) ([]int64, error) {
	var (
		pq = &queryBuilder{
			version:          h.version,
			metric:           qry.Metric,
			tag:              qry.Tag,
			numResults:       math.MaxInt - 1,
			strcmpOff:        h.Version3StrcmpOff.Load(),
			utcOffset:        h.utcOffset,
			newShardingStart: h.NewShardingStart.Load(),
		}
		tags = make(map[int64]bool)
	)
	for _, lod := range qry.Timescale.GetLODs(qry.Metric, qry.Offset) {
		query := pq.buildTagValueIDsQuery(lod)
		isFast := lod.FromSec+fastQueryTimeInterval >= lod.ToSec
		newSharding := h.newSharding(pq.metric, lod.FromSec)
		err := h.doSelect(ctx, chutil.QueryMetaInto{
			IsFast:         isFast,
			IsLight:        true,
			User:           h.accessInfo.user,
			Metric:         qry.Metric,
			Table:          lod.Table(newSharding),
			NewSharding:    newSharding,
			DisableCHAddrs: h.disabledCHAddrs(),
		}, Version2, ch.Query{
			Body:   query.body,
			Result: query.res,
			OnResult: func(_ context.Context, b proto.Block) error {
				for i := 0; i < b.Rows; i++ {
					tags[query.rowAt(i).valID] = true
				}
				return nil
			}})
		if err != nil {
			return nil, err
		}
	}
	res := make([]int64, 0, len(tags))
	for v := range tags {
		res = append(res, v)
	}
	return res, nil
}

func (h *requestHandler) Tracef(format string, a ...any) {
	if h.debug {
		h.traceMu.Lock()
		defer h.traceMu.Unlock()
		h.trace = append(h.trace, fmt.Sprintf(format, a...))
	}
}

type handlerWhat struct {
	sel []promql.SelectorWhat // what was specified in the selector is not necessarily equal to
	qry tsWhat                // what we will request
}

func (h *requestHandler) getHandlerWhat(whats []promql.SelectorWhat) []handlerWhat {
	if len(whats) == 0 {
		return nil
	}
	sort.Slice(whats, func(i, j int) bool {
		return whats[i].Digest < whats[j].Digest
	})
	res := make([]handlerWhat, 0, 1)
	res = append(res, handlerWhat{
		sel: []promql.SelectorWhat{whats[0]},
		qry: tsWhat{whats[0].Digest.Selector()},
	})
	tail := &res[0]
	for i := 1; i < len(whats); {
		for n := 1; i < len(whats) && n < len(tail.qry); i++ {
			if v := whats[i].Digest.Selector(); v != tail.qry[n-1] {
				tail.qry[n] = v
				n++
			}
			tail.sel = append(tail.sel, whats[i])
		}
		if i < len(whats) {
			res = append(res, handlerWhat{
				sel: []promql.SelectorWhat{whats[i]},
				qry: tsWhat{whats[i].Digest.Selector()},
			})
			tail = &res[len(res)-1]
			i++
		}
	}
	return res
}

func (w *handlerWhat) copyRowValuesAt(data []promql.SeriesData, x, y int, row *tsValues, queryStep, rowStep int64) {
	if queryStep == 0 {
		queryStep = rowStep
	}
	for i := 0; i < len(w.sel); i++ {
		(*data[x].Values)[y] = row.value(w.sel[i].Digest, w.qry[i].Argument, queryStep, rowStep)
		if minHost := data[x].MinMaxHost[0]; minHost != nil {
			if row.minHost.Arg != 0 {
				minHost[y] = row.minHost.AsArgMinMaxStringFloat32()
			} else {
				minHost[y] = row.minHostStr.ArgMinMaxStringFloat32
			}
		}
		if maxHost := data[x].MinMaxHost[1]; maxHost != nil {
			if row.maxHost.Arg != 0 {
				maxHost[y] = row.maxHost.AsArgMinMaxStringFloat32()
			} else {
				maxHost[y] = row.maxHostStr.ArgMinMaxStringFloat32
			}
		}
		x++
	}
}

func (w *handlerWhat) appendRowValues(s []Float64, row *tsSelectRow, queryStep int64, lod *data_model.LOD) []Float64 {
	if queryStep == 0 {
		queryStep = lod.StepSec
	}
	for i := 0; i < len(w.sel); i++ {
		s = append(s, Float64(row.value(w.sel[i].Digest, w.qry[i].Argument, queryStep, lod.StepSec)))
	}
	return s
}

func (row *tsValues) value(what promql.DigestWhat, arg float64, queryStep, lodStep int64) float64 {
	var val float64
	switch what {
	case promql.DigestCount:
		val = stableMulDiv(row.count, queryStep, lodStep)
	case promql.DigestCountSec:
		val = row.count / float64(lodStep)
	case promql.DigestCountRaw:
		val = row.count
	case promql.DigestSum:
		val = stableMulDiv(row.sum, queryStep, lodStep)
	case promql.DigestSumSec:
		val = row.sum / float64(lodStep)
	case promql.DigestSumRaw:
		val = row.sum
	case promql.DigestAvg:
		val = row.sum / row.count
	case promql.DigestMin:
		val = row.min
	case promql.DigestMax:
		val = row.max
	case promql.DigestP0_1,
		promql.DigestP1,
		promql.DigestP5,
		promql.DigestP10,
		promql.DigestP25,
		promql.DigestP50,
		promql.DigestP75,
		promql.DigestP90,
		promql.DigestP95,
		promql.DigestP99,
		promql.DigestP999:
		val = row.percentile.Quantile(arg)
	case promql.DigestStdDev:
		// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance, "Naïve algorithm", poor numeric stability
		if row.count < 2 {
			val = 0
		} else {
			val = math.Sqrt(math.Max((row.sumsquare-math.Pow(row.sum, 2)/row.count)/(row.count-1), 0))
		}
	case promql.DigestStdVar:
		if row.count < 2 {
			val = 0
		} else {
			val = math.Max((row.sumsquare-math.Pow(row.sum, 2)/row.count)/(row.count-1), 0)
		}
	case promql.DigestCardinality:
		val = stableMulDiv(row.cardinality, queryStep, lodStep)
	case promql.DigestCardinalitySec:
		val = row.cardinality / float64(lodStep)
	case promql.DigestCardinalityRaw:
		val = row.cardinality
	case promql.DigestUnique:
		val = stableMulDiv(float64(row.unique.Size(false)), queryStep, lodStep)
	case promql.DigestUniqueSec:
		val = float64(row.unique.Size(false)) / float64(lodStep)
	case promql.DigestUniqueRaw:
		val = float64(row.unique.Size(false))
	}
	replaceInfNan(&val)
	return val
}

func (h *Handler) Alloc(n int) *[]float64 {
	if n > data_model.MaxSlice {
		panic(httpErr(http.StatusBadRequest, errQueryOutOfRange))
	}
	return h.getFloatsSlice(n)
}

func (h *Handler) Free(s *[]float64) {
	h.putFloatsSlice(s)
}

func (h *requestHandler) getPromQuery(req seriesRequest) (string, error) {
	if len(req.promQL) != 0 {
		return req.promQL, nil
	}
	// query function
	const (
		nat int = iota // native
		cum            // cumulative
		der            // derivative
	)
	var whats [3][]promql.SelectorWhat
	for _, v := range req.what {
		switch v.QueryF {
		case format.ParamQueryFnCumulCount,
			format.ParamQueryFnCumulCardinality,
			format.ParamQueryFnCumulAvg,
			format.ParamQueryFnCumulSum:
			whats[cum] = append(whats[cum], v)
		case format.ParamQueryFnDerivativeCount,
			format.ParamQueryFnDerivativeCountNorm,
			format.ParamQueryFnDerivativeSum,
			format.ParamQueryFnDerivativeSumNorm,
			format.ParamQueryFnDerivativeAvg,
			format.ParamQueryFnDerivativeMin,
			format.ParamQueryFnDerivativeMax,
			format.ParamQueryFnDerivativeUnique,
			format.ParamQueryFnDerivativeUniqueNorm:
			whats[der] = append(whats[der], v)
		default:
			whats[nat] = append(whats[nat], v)
		}
	}
	// filtering and grouping
	var filterGroupBy []string
	var m [1]*format.MetricMetaValue
	f := data_model.QueryFilter{MetricMatcher: &labels.Matcher{Type: labels.MatchEqual, Value: req.metricName}}
	err := h.MatchMetrics(&f)
	if err != nil {
		return "", err
	}
	copy(m[:], f.MatchingMetrics)
	if len(req.by) != 0 {
		by, err := promqlGetBy(req.by, m[0])
		if err != nil {
			return "", err
		}
		filterGroupBy = append(filterGroupBy, fmt.Sprintf("@by=%q", by))
	}
	for t, in := range req.filterIn {
		for _, v := range in {
			tid, err := format.APICompatNormalizeTagID(t)
			if err != nil {
				return "", err
			}
			tid = promqlTagName(tid, m[0])
			filterGroupBy = append(filterGroupBy, fmt.Sprintf("%s=%q", tid, promqlGetFilterValue(tid, v, m[0])))
		}
	}
	for t, out := range req.filterNotIn {
		for _, v := range out {
			tid, err := format.APICompatNormalizeTagID(t)
			if err != nil {
				return "", err
			}
			tid = promqlTagName(tid, m[0])
			filterGroupBy = append(filterGroupBy, fmt.Sprintf("%s!=%q", tid, promqlGetFilterValue(tid, v, m[0])))
		}
	}
	// generate resulting string
	q := make([]string, 0, 3)
	for i, qws := range whats {
		if len(qws) == 0 {
			continue
		}
		var sb strings.Builder
		for j, qw := range qws {
			if j > 0 {
				sb.WriteByte(',')
			}
			w := qw.Digest.String()
			sb.WriteString(w)
			if qw.QueryF != w {
				sb.WriteByte(':')
				sb.WriteString(qw.QueryF)
			}
		}
		if req.maxHost {
			sb.WriteByte(',')
			sb.WriteString(promql.MaxHost)
		}
		expr := fmt.Sprintf("@what=%q", sb.String())
		expr = strings.Join(append([]string{expr}, filterGroupBy...), ",")
		expr = fmt.Sprintf("{@name=%q,%s}", req.metricName, expr)
		switch i {
		case cum:
			expr = fmt.Sprintf("prefix_sum(%s)", expr)
		case der:
			expr = fmt.Sprintf("idelta(%s)", expr)
		}
		q = append(q, expr)
	}
	res := strings.Join(q, " or ")
	var groupBy string
	if len(req.what) > 1 {
		groupBy = fmt.Sprintf(" by(%s) ", promql.LabelWhat)
	}
	if req.numResults < 0 {
		res = fmt.Sprintf("bottomk%s(%d,%s)", groupBy, -req.numResults, res)
		if len(groupBy) != 0 {
			res = fmt.Sprintf("sort(%s)", res)
		}
	} else if 0 <= req.numResults && req.numResults < math.MaxInt {
		numResults := req.numResults
		if numResults == 0 {
			numResults = defSeries
		}
		res = fmt.Sprintf("topk%s(%d,%s)", groupBy, numResults, res)
		if len(groupBy) != 0 {
			res = fmt.Sprintf("sort_desc(%s)", res)
		}
	} else {
		res = fmt.Sprintf("sort_desc%s(%s)", groupBy, res)
	}
	return res, nil
}

func promqlGetBy(by []string, m *format.MetricMetaValue) (string, error) {
	var (
		tags = make([]int, format.MaxTags)
		skey bool
	)
	for _, v := range by {
		tid, err := format.APICompatNormalizeTagID(v)
		if err != nil {
			return "", err
		}
		if tid == format.StringTopTagID {
			skey = true
			continue
		}
		if i := format.TagIndex(tid); 0 <= i && i < format.MaxTags {
			tags[i]++
		}
	}
	by = by[:0]
	for i, v := range tags {
		if v > 0 {
			by = append(by, promqlTagNameAt(i, m))
		}
	}
	if skey {
		by = append(by, promqlTagNameSTop(m))
	}
	return strings.Join(by, ","), nil
}

func promqlGetFilterValue(tagID string, s string, m *format.MetricMetaValue) string {
	if tagID == format.StringTopTagID && s == format.TagValueCodeZero {
		return ""
	}
	if m != nil {
		if t := m.Name2Tag(tagID); t.Raw && t.BuiltinKind == 0 && len(t.ValueComments) != 0 {
			if v := t.ValueComments[s]; v != "" {
				return v
			}
		}
	}
	return s
}

func promqlTagName(tagID string, m *format.MetricMetaValue) string {
	if m != nil {
		if t := m.Name2Tag(tagID); t.Name != "" {
			return t.Name
		}
	}
	return tagID
}

func promqlTagNameAt(tagX int, m *format.MetricMetaValue) string {
	if m != nil && 0 <= tagX && tagX < len(m.Tags) && m.Tags[tagX].Name != "" {
		return m.Tags[tagX].Name
	}
	return strconv.Itoa(tagX)
}

func promqlTagNameSTop(m *format.MetricMetaValue) string {
	if m == nil || m.StringTopName == "" {
		return format.StringTopTagID
	}
	return m.StringTopName
}

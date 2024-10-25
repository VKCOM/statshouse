// Copyright 2022 V Kontakte LLC
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
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/gorilla/mux"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql"
	"github.com/vkcom/statshouse/internal/promql/parser"
	"github.com/vkcom/statshouse/internal/util"
)

var errQueryOutOfRange = fmt.Errorf("exceeded maximum resolution of %d points per timeseries", data_model.MaxSlice)
var errAccessViolation = fmt.Errorf("metric access violation")

func HandleInstantQuery(h *HTTPRequestHandler, r *http.Request) {
	// parse access token
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	// parse query
	q := promql.Query{
		Expr: r.FormValue("query"),
		Options: promql.Options{
			Mode:      data_model.InstantQuery,
			Compat:    true,
			TimeNow:   time.Now().Unix(),
			Namespace: r.Header.Get("X-StatsHouse-Namespace"),
		},
	}
	if t := r.FormValue("time"); t == "" {
		q.Start = q.Options.TimeNow
	} else {
		var err error
		q.Start, err = parseTime(t)
		if err != nil {
			promRespondError(h, promErrorBadData, fmt.Errorf("invalid parameter time: %w", err))
			return
		}
	}
	q.End = q.Start + 1 // handler expects half open interval [start, end)
	// execute query
	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()
	res, dispose, err := h.promEngine.Exec(withAccessInfo(ctx, &h.accessInfo), q)
	if err != nil {
		promRespondError(h, promErrorExec, err)
		return
	}
	defer dispose()
	promRespond(h, promResponseData{ResultType: res.Type(), Result: res})
}

func HandleRangeQuery(h *HTTPRequestHandler, r *http.Request) {
	// parse access token
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	// parse query
	q := promql.Query{
		Expr: r.FormValue("query"),
		Options: promql.Options{
			Compat:    true,
			Namespace: r.Header.Get("X-StatsHouse-Namespace"),
		},
	}
	q.Start, err = parseTime(r.FormValue("start"))
	if err != nil {
		promRespondError(h, promErrorBadData, fmt.Errorf("invalid parameter start: %w", err))
		return
	}
	q.End, err = parseTime(r.FormValue("end"))
	if err != nil {
		promRespondError(h, promErrorBadData, fmt.Errorf("invalid parameter end: %w", err))
		return
	}
	q.End++ // handler expects half open interval [start, end)
	q.Step, err = parseDuration(r.FormValue("step"))
	if err != nil {
		promRespondError(h, promErrorBadData, fmt.Errorf("invalid parameter step: %w", err))
		return
	}
	// execute query
	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()
	res, dispose, err := h.promEngine.Exec(withAccessInfo(ctx, &h.accessInfo), q)
	if err != nil {
		promRespondError(h, promErrorExec, err)
		return
	}
	defer dispose()
	promRespond(h, promResponseData{ResultType: res.Type(), Result: res})
}

func HandlePromSeriesQuery(h *HTTPRequestHandler, r *http.Request) {
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	_ = r.ParseForm()
	match := r.Form["match[]"]
	if len(match) == 0 {
		promRespondError(h, promErrorBadData, fmt.Errorf("no match[] parameter provided"))
		return
	}
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		promRespondError(h, promErrorBadData, fmt.Errorf("invalid parameter start: %w", err))
		return
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		promRespondError(h, promErrorBadData, fmt.Errorf("invalid parameter end: %w", err))
		return
	}
	if start < end-300 {
		start = end - 300
	}
	end++ // handler expects half open interval [start, end)
	var res []labels.Labels
	for _, expr := range match {
		func() {
			val, cancel, err := h.promEngine.Exec(
				withAccessInfo(r.Context(), &h.accessInfo),
				promql.Query{
					Start: start,
					End:   end,
					Expr:  expr,
					Options: promql.Options{
						Limit:     1000,
						Mode:      data_model.TagsQuery,
						Namespace: r.Header.Get("X-StatsHouse-Namespace"),
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
	promRespond(h, res)
}

func HandlePromLabelValuesQuery(h *HTTPRequestHandler, r *http.Request) {
	err := h.parseAccessToken(r)
	if err != nil {
		respondJSON(h, nil, 0, 0, err)
		return
	}
	namespace := r.Header.Get("X-StatsHouse-Namespace")
	var prefix string
	switch namespace {
	case "", "__default":
		// no prefix
	default:
		prefix = namespace + format.NamespaceSeparator
	}
	var res []string
	tagName := mux.Vars(r)["name"]
	if tagName == "__name__" {
		for _, meta := range h.metricsStorage.GetMetaMetricList(h.showInvisible) {
			trimmed := strings.TrimPrefix(meta.Name, prefix)
			if meta.Name != trimmed && h.accessInfo.CanViewMetric(*meta) {
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
				promRespondError(h, promErrorBadData, fmt.Errorf("invalid parameter start: %w", err))
				return
			}
			end, err := parseTime(r.FormValue("end"))
			if err != nil {
				promRespondError(h, promErrorBadData, fmt.Errorf("invalid parameter end: %w", err))
				return
			}
			if start < end-300 {
				start = end - 300
			}
			end++ // handler expects half open interval [start, end)
			for _, expr := range r.Form["match[]"] {
				func() {
					val, cancel, err := h.promEngine.Exec(
						withAccessInfo(r.Context(), &h.accessInfo),
						promql.Query{
							Start: start,
							End:   end,
							Expr:  expr,
							Options: promql.Options{
								Limit:     1000,
								Mode:      data_model.TagsQuery,
								GroupBy:   []string{tagName},
								Namespace: r.Header.Get("X-StatsHouse-Namespace"),
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
	promRespond(h, res)
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

func (h *Handler) MatchMetrics(ctx context.Context, matcher *labels.Matcher, namespace string) ([]*format.MetricMetaValue, error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		return nil, errAccessViolation
	}
	res := h.metricsStorage.MatchMetrics(matcher, namespace, h.showInvisible, nil)
	for _, metric := range res {
		if !ai.CanViewMetric(*metric) {
			return nil, httpErr(http.StatusForbidden, fmt.Errorf("metric %q forbidden", metric.Name))
		}
	}
	return res, nil
}

func (h *Handler) GetHostName(hostID int32) string {
	v, err := h.getTagValue(hostID)
	if err != nil {
		return format.CodeTagValue(hostID)
	}
	return v
}

func (h *Handler) GetTagValue(qry promql.TagValueQuery) string {
	var tagID string
	if len(qry.TagID) == 0 {
		tagID = format.TagID(qry.TagIndex)
	} else {
		tagID = qry.TagID
	}
	return h.getRichTagValue(qry.Metric, data_model.VersionOrDefault(qry.Version), tagID, qry.TagValueID)
}

func (h *Handler) GetTagValueID(qry promql.TagValueIDQuery) (int32, error) {
	res, err := h.getRichTagValueID(&qry.Metric.Tags[qry.TagIndex], qry.Version, qry.TagValue)
	if err != nil {
		var httpErr httpError
		if errors.As(err, &httpErr) && httpErr.code == http.StatusNotFound {
			err = promql.ErrNotFound
		}
	}
	return res, err
}

func (h *Handler) QuerySeries(ctx context.Context, qry *promql.SeriesQuery) (promql.Series, func(), error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		return promql.Series{}, nil, errAccessViolation
	}
	if !ai.CanViewMetricName(qry.Metric.Name) {
		return promql.Series{}, func() {}, httpErr(http.StatusForbidden, fmt.Errorf("metric %q forbidden", qry.Metric.Name))
	}
	if qry.Options.Mode == data_model.PointQuery {
		for _, what := range qry.Whats {
			switch what.Digest {
			case data_model.DigestCount, data_model.DigestMin, data_model.DigestMax, data_model.DigestAvg,
				data_model.DigestSum, data_model.DigestP25, data_model.DigestP50, data_model.DigestP75,
				data_model.DigestP90, data_model.DigestP95, data_model.DigestP99, data_model.DigestP999,
				data_model.DigestUnique:
				// pass
			default:
				return promql.Series{}, func() {}, fmt.Errorf("function %s is not supported", promql.DigestWhatString(what.Digest))
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
		case data_model.DigestCount, data_model.DigestCountSec, data_model.DigestCountRaw,
			data_model.DigestStdVar, data_model.DigestCardinality, data_model.DigestCardinalitySec,
			data_model.DigestCardinalityRaw, data_model.DigestUnique, data_model.DigestUniqueSec:
			// measure units does not apply to counters
		default:
			res.Meta.Units = qry.Metric.MetricType
		}
	}
	version := data_model.VersionOrDefault(qry.Options.Version)
	var lods []data_model.LOD
	if qry.Options.Mode == data_model.PointQuery {
		lod0 := qry.Timescale.LODs[0]
		start := qry.Timescale.Time[0]
		metric := qry.Metric
		lods = []data_model.LOD{{
			FromSec:    qry.Timescale.Time[0] - qry.Offset,
			ToSec:      qry.Timescale.Time[1] - qry.Offset,
			StepSec:    lod0.Step,
			Table:      data_model.LODTables[version][lod0.Step],
			HasPreKey:  metric.PreKeyOnly || (metric.PreKeyFrom != 0 && int64(metric.PreKeyFrom) <= start),
			PreKeyOnly: metric.PreKeyOnly,
			Location:   h.location,
		}}
	} else {
		lods = qry.Timescale.GetLODs(qry.Metric, qry.Offset)
	}
	tagX := make(map[tsTags]int, len(qry.GroupBy))
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
	for _, args := range getHandlerArgs(qry, ai, step) {
		var tx int // time index
		for _, lod := range lods {
			switch qry.Options.Mode {
			case data_model.PointQuery:
				data, err := h.pointsCache.get(ctx, args.qs, &args.pq, lod, qry.Options.AvoidCache)
				if err != nil {
					return promql.Series{}, nil, err
				}
				for i := 0; i < len(data); i++ {
					x, ok := tagX[data[i].tsTags]
					if !ok {
						x = len(res.Data)
						tagX[data[i].tsTags] = x
						for _, fn := range args.what {
							v := h.Alloc(len(qry.Timescale.Time))
							buffers = append(buffers, v)
							for y := range *v {
								(*v)[y] = promql.NilValue
							}
							res.Data = append(res.Data, promql.SeriesData{
								Values: v,
								What:   fn.sel,
							})
						}
					}
					// select "point" value
					for y, what := range args.what {
						var v float64
						row := &data[i]
						switch what.qry {
						case data_model.DigestCount, data_model.DigestCountRaw, data_model.DigestCountSec:
							v = row.countNorm
						case data_model.DigestMin,
							data_model.DigestP0_1, data_model.DigestP25,
							data_model.DigestUnique, data_model.DigestUniqueRaw, data_model.DigestUniqueSec,
							data_model.DigestCardinality, data_model.DigestCardinalityRaw, data_model.DigestCardinalitySec:
							v = row.val[0]
						case data_model.DigestMax, data_model.DigestP1, data_model.DigestP50:
							v = row.val[1]
						case data_model.DigestAvg, data_model.DigestP5, data_model.DigestP75:
							v = row.val[2]
						case data_model.DigestSum, data_model.DigestSumRaw, data_model.DigestSumSec, data_model.DigestP10, data_model.DigestP90:
							v = row.val[3]
						case data_model.DigestStdDev, data_model.DigestP95:
							v = row.val[4]
						case data_model.DigestStdVar:
							v = row.val[4] * row.val[4]
						case data_model.DigestP99:
							v = row.val[5]
						case data_model.DigestP999:
							v = row.val[6]
						default:
							v = math.NaN()
						}
						(*res.Data[x+y].Values)[tx] = v
					}
				}
				tx++
			case data_model.RangeQuery, data_model.InstantQuery:
				data, err := h.cache.Get(ctx, version, args.qs, &args.pq, lod, qry.Options.AvoidCache)
				if err != nil {
					return promql.Series{}, nil, err
				}
				for i := 0; i < len(data); i++ {
					for j := 0; j < len(data[i]); j++ {
						x, ok := tagX[data[i][j].tsTags]
						if !ok {
							x = len(res.Data)
							tagX[data[i][j].tsTags] = x
							for _, fn := range args.what {
								v := h.Alloc(len(qry.Timescale.Time))
								buffers = append(buffers, v)
								for y := range *v {
									(*v)[y] = promql.NilValue
								}
								var h [2][]int32
								for z, qryHost := range qry.MinMaxHost {
									if qryHost {
										h[z] = make([]int32, len(qry.Timescale.Time))
									}
								}
								res.Data = append(res.Data, promql.SeriesData{
									Values:     v,
									MinMaxHost: h,
									What:       fn.sel,
								})
							}
						}
						k, err := lod.IndexOf(data[i][j].time)
						if err != nil {
							return promql.Series{}, nil, err
						}
						k += tx
						for y, what := range args.what {
							(*res.Data[x+y].Values)[k] = selectTSValue(what.qry, qry.MinMaxHost[0] || qry.MinMaxHost[1], int64(step), &data[i][j])
							for z, qryHost := range qry.MinMaxHost {
								if qryHost {
									res.Data[x+y].MinMaxHost[z][k] = data[i][j].host[z]
								}
							}
						}
					}
				}
				tx += len(data)
			case data_model.TagsQuery:
				data, err := h.cache.Get(ctx, version, args.qs, &args.pq, lod, qry.Options.AvoidCache)
				if err != nil {
					return promql.Series{}, nil, err
				}
				for i := 0; i < len(data); i++ {
					for j := 0; j < len(data[i]); j++ {
						if _, ok := tagX[data[i][j].tsTags]; !ok {
							tagX[data[i][j].tsTags] = len(tagX)
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
		for i, what := range args.what {
			for v, j := range tagX {
				for _, groupBy := range qry.GroupBy {
					switch groupBy {
					case format.StringTopTagID, qry.Metric.StringTopName:
						res.AddTagAt(i+j, &promql.SeriesTag{
							Metric: qry.Metric,
							Index:  format.StringTopTagIndex + promql.SeriesTagIndexOffset,
							ID:     format.StringTopTagID,
							Name:   qry.Metric.StringTopName,
							SValue: emptyToUnspecified(v.tagStr.String()),
						})
					case format.ShardTagID:
						res.AddTagAt(i+j, &promql.SeriesTag{
							Metric: qry.Metric,
							ID:     promql.LabelShard,
							Value:  int32(v.shardNum),
						})
					default:
						if m, ok := qry.Metric.Name2Tag[groupBy]; ok && m.Index < len(v.tag) {
							st := &promql.SeriesTag{
								Metric: qry.Metric,
								Index:  m.Index + promql.SeriesTagIndexOffset,
								ID:     format.TagID(m.Index),
								Name:   m.Name,
								Value:  v.tag[m.Index],
							}
							if qry.Options.Version == Version3 && m.Index < len(v.stag) {
								st.SValue = v.stag[m.Index]
							}
							res.AddTagAt(i+j, st)
						}
					}
				}
				if tagWhat {
					res.AddTagAt(i+j, &promql.SeriesTag{
						ID:    promql.LabelWhat,
						Value: int32(what.sel.Digest),
					})
				}
			}
			if qry.Options.Mode == data_model.TagsQuery {
				break
			}
		}
		tagX = make(map[tsTags]int, len(tagX))
	}
	res.Meta.Total = len(res.Data)
	succeeded = true // prevents deffered "cleanup"
	return res, cleanup, nil
}

func (h *Handler) QueryTagValueIDs(ctx context.Context, qry promql.TagValuesQuery) ([]int32, error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		return nil, errAccessViolation
	}
	var (
		version = data_model.VersionOrDefault(qry.Options.Version)
		pq      = &preparedTagValuesQuery{
			version:     version,
			metricID:    qry.Metric.MetricID,
			preKeyTagID: qry.Metric.PreKeyTagID,
			tagID:       format.TagID(qry.TagIndex),
			numResults:  math.MaxInt - 1,
		}
		tags = make(map[int32]bool)
	)
	for _, lod := range qry.Timescale.GetLODs(qry.Metric, qry.Offset) {
		body, args, err := tagValuesQuery(pq, lod)
		if err != nil {
			return nil, err
		}
		cols := newTagValuesSelectCols(args)
		isFast := lod.FromSec+fastQueryTimeInterval >= lod.ToSec
		err = h.doSelect(ctx, util.QueryMetaInto{
			IsFast:  isFast,
			IsLight: true,
			User:    ai.user,
			Metric:  qry.Metric.MetricID,
			Table:   lod.Table,
			Kind:    "load_tags",
		}, Version2, ch.Query{
			Body:   body,
			Result: cols.res,
			OnResult: func(_ context.Context, b proto.Block) error {
				for i := 0; i < b.Rows; i++ {
					tags[cols.rowAt(i).valID] = true
				}
				return nil
			}})
		if err != nil {
			return nil, err
		}
	}
	res := make([]int32, 0, len(tags))
	for v := range tags {
		res = append(res, v)
	}
	return res, nil
}

func (h *Handler) QueryStringTop(ctx context.Context, qry promql.TagValuesQuery) ([]string, error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		return nil, errAccessViolation
	}
	var (
		version = data_model.VersionOrDefault(qry.Options.Version)
		pq      = &preparedTagValuesQuery{
			version:     version,
			metricID:    qry.Metric.MetricID,
			preKeyTagID: qry.Metric.PreKeyTagID,
			tagID:       format.StringTopTagID,
			numResults:  math.MaxInt - 1,
		}
		tags = make(map[string]bool)
	)
	for _, lod := range qry.Timescale.GetLODs(qry.Metric, qry.Offset) {
		body, args, err := tagValuesQuery(pq, lod)
		if err != nil {
			return nil, err
		}
		cols := newTagValuesSelectCols(args)
		isFast := lod.FromSec+fastQueryTimeInterval >= lod.ToSec
		err = h.doSelect(ctx, util.QueryMetaInto{
			IsFast:  isFast,
			IsLight: true,
			User:    ai.user,
			Metric:  qry.Metric.MetricID,
			Table:   lod.Table,
			Kind:    "load_stag",
		}, Version2, ch.Query{
			Body:   body,
			Result: cols.res,
			OnResult: func(_ context.Context, b proto.Block) error {
				for i := 0; i < b.Rows; i++ {
					tags[cols.rowAt(i).val] = true
				}
				return nil
			}})
		if err != nil {
			return nil, err
		}
	}
	ret := make([]string, 0, len(tags))
	for id := range tags {
		ret = append(ret, id)
	}
	return ret, nil
}

type handlerArgs struct {
	qs   string // cache key
	pq   preparedPointsQuery
	what []handlerWhat
}

type handlerWhat struct {
	sel promql.SelectorWhat   // what was specified in the selector is not necessarily equal to
	qry data_model.DigestWhat // what we will request
}

func getHandlerArgs(qry *promql.SeriesQuery, ai *accessInfo, step int64) map[data_model.DigestKind]handlerArgs {
	// filtering
	var (
		filterIn   = make(map[string][]string)
		filterInM  = make(map[string][]any) // mapped
		filterInV3 = make(map[string][]maybeMappedTag)
	)
	for i, m := range qry.FilterIn {
		if i == 0 && qry.Options.Version == Version1 {
			continue
		}
		tagName := format.TagID(i)
		for tagValue, tagValueID := range m {
			filterIn[tagName] = append(filterIn[tagName], tagValue)
			filterInM[tagName] = append(filterInM[tagName], tagValueID)
			filterInV3[tagName] = append(filterInV3[tagName], maybeMappedTag{tagValue, tagValueID})
		}
	}
	for _, tagValue := range qry.SFilterIn {
		filterIn[format.StringTopTagID] = append(filterIn[format.StringTopTagID], promqlEncodeSTagValue(tagValue))
		filterInM[format.StringTopTagID] = append(filterInM[format.StringTopTagID], tagValue)
		filterInV3[format.StringTopTagIDV3] = append(filterInV3[format.StringTopTagIDV3], maybeMappedTag{Value: tagValue})
	}
	var (
		filterOut   = make(map[string][]string)
		filterOutM  = make(map[string][]any) // mapped
		filterOutV3 = make(map[string][]maybeMappedTag)
	)
	for i, m := range qry.FilterOut {
		if i == 0 && qry.Options.Version == Version1 {
			continue
		}
		tagID := format.TagID(i)
		for tagValue, tagValueID := range m {
			filterOut[tagID] = append(filterOut[tagID], tagValue)
			filterOutM[tagID] = append(filterOutM[tagID], tagValueID)
			filterOutV3[tagID] = append(filterOutV3[tagID], maybeMappedTag{tagValue, tagValueID})
		}
	}
	for _, tagValue := range qry.SFilterOut {
		filterOut[format.StringTopTagID] = append(filterOut[format.StringTopTagID], promqlEncodeSTagValue(tagValue))
		filterOutM[format.StringTopTagID] = append(filterOutM[format.StringTopTagID], tagValue)
		filterOutV3[format.StringTopTagIDV3] = append(filterOutV3[format.StringTopTagIDV3], maybeMappedTag{Value: tagValue})
	}
	// grouping
	var groupBy []string
	switch qry.Options.Version {
	case Version1:
		for _, v := range qry.GroupBy {
			if v != format.EnvTagID {
				groupBy = append(groupBy, v)
			}
		}
	default:
		groupBy = qry.GroupBy
	}
	// get "queryFn"
	res := make(map[data_model.DigestKind]handlerArgs)
	pointQuery := qry.Options.Mode == data_model.PointQuery
	for _, v := range qry.Whats {
		queryWhat := v.Digest
		if pointQuery || step == 0 || step == _1M {
			switch v.Digest {
			case data_model.DigestCount:
				queryWhat = data_model.DigestCountRaw
			case data_model.DigestSum:
				queryWhat = data_model.DigestSumRaw
			case data_model.DigestCardinality:
				queryWhat = data_model.DigestCardinalityRaw
			case data_model.DigestUnique:
				queryWhat = data_model.DigestUniqueRaw
			}
		}
		kind := queryWhat.Kind(qry.MinMaxHost[0] || qry.MinMaxHost[1])
		args := res[kind]
		args.what = append(args.what, handlerWhat{v, queryWhat})
		res[kind] = args
	}
	// all kinds contain counter value, there is
	// no sence therefore to query counter separetely
	if v, ok := res[data_model.DigestKindCount]; ok {
		for kind, args := range res {
			if kind == data_model.DigestKindCount {
				continue
			}
			args.what = append(args.what, v.what...)
			res[kind] = args
			delete(res, data_model.DigestKindCount)
			break
		}
	}
	// cache key & query
	for kind, args := range res {
		// TODO switch to v3 filters, for now we always use v2
		args.qs = normalizedQueryString(qry.Metric.Name, kind, groupBy, filterIn, filterOut, false)
		args.pq = preparedPointsQuery{
			user:          ai.user,
			version:       data_model.VersionOrDefault(qry.Options.Version),
			metricID:      qry.Metric.MetricID,
			preKeyTagID:   qry.Metric.PreKeyTagID,
			kind:          kind,
			by:            qry.GroupBy,
			filterIn:      filterInM,
			filterNotIn:   filterOutM,
			filterInV3:    filterInV3,
			filterNotInV3: filterOutV3,
		}
		res[kind] = args
	}
	return res
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

func (h *Handler) getPromQuery(req seriesRequest) (string, error) {
	if len(req.promQL) != 0 {
		return req.promQL, nil
	}
	// query function
	const (
		nat int = iota // native
		cum            // cumulative
		der            // derivative
	)
	var whats [3][]QueryFunc
	for _, v := range req.what {
		if v.Cumul {
			whats[cum] = append(whats[cum], v)
		} else if v.Deriv {
			whats[der] = append(whats[der], v)
		} else {
			whats[nat] = append(whats[nat], v)
		}
	}
	// filtering and grouping
	var filterGroupBy []string
	var m [1]*format.MetricMetaValue
	matcher := labels.Matcher{Type: labels.MatchEqual, Value: req.metricName}
	copy(m[:], h.metricsStorage.MatchMetrics(&matcher, "", h.showInvisible, m[:0]))
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
			w := promql.DigestWhatString(qw.What)
			sb.WriteString(w)
			if qw.Name != w {
				sb.WriteByte(':')
				sb.WriteString(qw.Name)
			}
		}
		if req.maxHost {
			sb.WriteByte(',')
			sb.WriteString(promql.MaxHost)
		}
		expr := fmt.Sprintf("@what=%q", sb.String())
		expr = strings.Join(append([]string{expr}, filterGroupBy...), ",")
		expr = fmt.Sprintf("%s{%s}", req.metricName, expr)
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
		if t := m.Name2Tag[tagID]; t.Raw && !t.IsMetric && !t.IsNamespace && !t.IsGroup && len(t.ValueComments) != 0 {
			if v := t.ValueComments[s]; v != "" {
				return v
			}
		}
	}
	return s
}

func promqlEncodeSTagValue(s string) string {
	if s == "" {
		return format.TagValueCodeZero
	}
	return s
}

func promqlTagName(tagID string, m *format.MetricMetaValue) string {
	if m != nil {
		if t := m.Name2Tag[tagID]; t.Name != "" {
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

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

func (h *Handler) handlePromQuery(w http.ResponseWriter, r *http.Request, rangeQuery bool) {
	// parse access token
	ai, err := h.parseAccessToken(r, nil)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	// parse query
	var parse func(*http.Request) (promql.Query, error)
	if rangeQuery {
		parse = parsePromRangeQuery
	} else {
		parse = parsePromInstantQuery
	}
	q, err := parse(r)
	if err != nil {
		promRespondError(w, promErrorBadData, err)
		return
	}
	q.End++ // handler expects half open interval [start, end)
	q.Options.Namespace = r.Header.Get("X-StatsHouse-Namespace")
	// execute query
	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()
	res, dispose, err := h.promEngine.Exec(withAccessInfo(ctx, &ai), q)
	if err != nil {
		promRespondError(w, promErrorExec, err)
		return
	}
	defer dispose()
	promRespond(w, promResponseData{ResultType: res.Type(), Result: res})
}

func (h *Handler) HandlePromInstantQuery(w http.ResponseWriter, r *http.Request) {
	h.handlePromQuery(w, r, false)
}

func (h *Handler) HandlePromRangeQuery(w http.ResponseWriter, r *http.Request) {
	h.handlePromQuery(w, r, true)
}

func (h *Handler) HandlePromLabelValuesQuery(w http.ResponseWriter, r *http.Request) {
	ai, err := h.parseAccessToken(r, nil)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}

	name := mux.Vars(r)["name"]
	if name != "__name__" {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	s := make([]string, 0)
	for _, m := range format.BuiltinMetrics {
		if ai.CanViewMetric(*m) {
			s = append(s, m.Name)
		}
	}
	for _, v := range h.metricsStorage.GetMetaMetricList(h.showInvisible) {
		if ai.CanViewMetric(*v) {
			s = append(s, v.Name)
		}
	}
	promRespond(w, s)
}

// region Request

func parsePromRangeQuery(r *http.Request) (q promql.Query, err error) {
	q.Start, err = parseTime(r.FormValue("start"))
	if err != nil {
		return q, fmt.Errorf("invalid parameter start: %w", err)
	}

	q.End, err = parseTime(r.FormValue("end"))
	if err != nil {
		return q, fmt.Errorf("invalid parameter end: %w", err)
	}
	if q.End < q.Start {
		return q, fmt.Errorf("invalid parameter end: end timestamp must not be before start time")
	}

	q.Step, err = parseDuration(r.FormValue("step"))
	if err != nil {
		return q, fmt.Errorf("invalid parameter step: %w", err)
	}
	if q.Step <= 0 {
		return q, fmt.Errorf("invalid parameter step: zero or negative handleQuery resolution step widths are not accepted. Try a positive integer")
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if (q.End-q.Start)/q.Step > data_model.MaxSlice {
		return q, fmt.Errorf("exceeded maximum resolution of %d points per timeseries. Try decreasing the query resolution (?step=XX)", data_model.MaxSlice)
	}

	q.Expr = r.FormValue("query")
	return q, nil
}

func parsePromInstantQuery(r *http.Request) (q promql.Query, err error) {
	v := r.FormValue("time")
	if v == "" {
		q.Start = time.Now().Unix()
	} else {
		q.Start, err = parseTime(v)
		if err != nil {
			return q, fmt.Errorf("invalid parameter time: %w", err)
		}
	}
	q.End = q.Start
	q.Expr = r.FormValue("query")
	return q, nil
}

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
	if qry.Options.Collapse {
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
	if qry.Options.Collapse {
		// "point" query
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
		fns, qs, pq := args.fns, args.qs, args.pq
		for _, lod := range lods {
			var err error
			var data [][]tsSelectRow
			if qry.Options.Collapse { // "point" query
				if s, err := h.pointsCache.get(ctx, qs, &pq, lod, qry.Options.AvoidCache); err == nil {
					data = make([][]tsSelectRow, 1)
					data[0] = make([]tsSelectRow, len(s))
					for i := range s {
						data[0][i] = tsSelectRow{tsTags: s[i].tsTags, tsValues: s[i].tsValues}
					}
				}
			} else {
				data, err = h.cache.Get(ctx, version, qs, &pq, lod, qry.Options.AvoidCache)
			}
			if err != nil {
				return promql.Series{}, nil, err
			}
			for i := 0; i < len(data); i++ {
				for j := 0; j < len(data[i]); j++ {
					k := tx
					if !qry.Options.Collapse { // "point" query does not return timestamp
						x, err := lod.IndexOf(data[i][j].time)
						if err != nil {
							return promql.Series{}, nil, err
						}
						k += x
					}
					x, ok := tagX[data[i][j].tsTags]
					if !ok {
						x = len(res.Data)
						tagX[data[i][j].tsTags] = x
						for _, fn := range fns {
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
								What:       fn,
							})
						}
					}
					for y, fn := range fns {
						(*res.Data[x+y].Values)[k] = selectTSValue(fn.Digest, qry.MinMaxHost[0] || qry.MinMaxHost[1], int64(step), &data[i][j])
						for z, qryHost := range qry.MinMaxHost {
							if qryHost {
								res.Data[x+y].MinMaxHost[z][k] = data[i][j].host[z]
							}
						}
					}
				}
			}
			tx += len(data)
		}
		tagWhat := len(qry.Whats) > 1 || qry.Options.TagWhat
		for i, what := range fns {
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
							res.AddTagAt(i+j, &promql.SeriesTag{
								Metric: qry.Metric,
								Index:  m.Index + promql.SeriesTagIndexOffset,
								ID:     format.TagID(m.Index),
								Name:   m.Name,
								Value:  v.tag[m.Index],
							})
						}
					}
				}
				if tagWhat {
					res.AddTagAt(i+j, &promql.SeriesTag{
						ID:    promql.LabelWhat,
						Value: int32(what.Digest),
					})
				}
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
	qs  string // cache key
	pq  preparedPointsQuery
	fns []promql.SelectorWhat
}

func getHandlerArgs(qry *promql.SeriesQuery, ai *accessInfo, step int64) map[data_model.DigestKind]handlerArgs {
	// filtering
	var (
		filterIn  = make(map[string][]string)
		filterInM = make(map[string][]any) // mapped
	)
	for i, m := range qry.FilterIn {
		if i == 0 && qry.Options.Version == Version1 {
			continue
		}
		tagID := format.TagID(i)
		for tagValueID, tagValue := range m {
			filterIn[tagID] = append(filterIn[tagID], tagValue)
			filterInM[tagID] = append(filterInM[tagID], tagValueID)
		}
	}
	for _, tagValue := range qry.SFilterIn {
		filterIn[format.StringTopTagID] = append(filterIn[format.StringTopTagID], promqlEncodeSTagValue(tagValue))
		filterInM[format.StringTopTagID] = append(filterInM[format.StringTopTagID], tagValue)
	}
	var (
		filterOut  = make(map[string][]string)
		filterOutM = make(map[string][]any) // mapped
	)
	for i, m := range qry.FilterOut {
		if i == 0 && qry.Options.Version == Version1 {
			continue
		}
		tagID := format.TagID(i)
		for tagValueID, tagValue := range m {
			filterOut[tagID] = append(filterOut[tagID], tagValue)
			filterOutM[tagID] = append(filterOutM[tagID], tagValueID)
		}
	}
	for _, tagValue := range qry.SFilterOut {
		filterOut[format.StringTopTagID] = append(filterOut[format.StringTopTagID], promqlEncodeSTagValue(tagValue))
		filterOutM[format.StringTopTagID] = append(filterOutM[format.StringTopTagID], tagValue)
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
	for _, v := range qry.Whats {
		if qry.Options.Collapse || step == 0 || step == _1M {
			switch v.Digest {
			case data_model.DigestCount:
				v.Digest = data_model.DigestCountRaw
			case data_model.DigestSum:
				v.Digest = data_model.DigestSumRaw
			case data_model.DigestCardinality:
				v.Digest = data_model.DigestCardinalityRaw
			}
		}
		kind := v.Digest.Kind(qry.MinMaxHost[0] || qry.MinMaxHost[1])
		args := res[kind]
		args.fns = append(args.fns, v)
		res[kind] = args
	}
	// all kinds contain counter value, there is
	// no sence therefore to query counter separetely
	if v, ok := res[data_model.DigestKindCount]; ok {
		for kind, args := range res {
			if kind == data_model.DigestKindCount {
				continue
			}
			args.fns = append(args.fns, v.fns...)
			res[kind] = args
			delete(res, data_model.DigestKindCount)
			break
		}
	}
	// cache key & query
	for kind, args := range res {
		args.qs = normalizedQueryString(qry.Metric.Name, kind, groupBy, filterIn, filterOut, false)
		args.pq = preparedPointsQuery{
			user:        ai.user,
			version:     data_model.VersionOrDefault(qry.Options.Version),
			metricID:    qry.Metric.MetricID,
			preKeyTagID: qry.Metric.PreKeyTagID,
			kind:        kind,
			by:          qry.GroupBy,
			filterIn:    filterInM,
			filterNotIn: filterOutM,
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

func getPromQuery(req seriesRequest) (string, error) {
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
	if len(req.by) != 0 {
		by, err := promqlGetBy(req.by)
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
			filterGroupBy = append(filterGroupBy, fmt.Sprintf("%s=%q", tid, promqlGetFilterValue(tid, v)))
		}
	}
	for t, out := range req.filterNotIn {
		for _, v := range out {
			tid, err := format.APICompatNormalizeTagID(t)
			if err != nil {
				return "", err
			}
			filterGroupBy = append(filterGroupBy, fmt.Sprintf("%s!=%q", tid, promqlGetFilterValue(tid, v)))
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
		expr = fmt.Sprintf("%s{%s}", req.metricWithNamespace, expr)
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

func promqlGetBy(by []string) (string, error) {
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
			by = append(by, strconv.Itoa(i))
		}
	}
	if skey {
		by = append(by, format.StringTopTagID)
	}
	return strings.Join(by, ","), nil
}

func promqlGetFilterValue(tagID string, s string) string {
	if tagID == format.StringTopTagID && s == format.TagValueCodeZero {
		return ""
	}
	return s
}

func promqlEncodeSTagValue(s string) string {
	if s == "" {
		return format.TagValueCodeZero
	}
	return s
}

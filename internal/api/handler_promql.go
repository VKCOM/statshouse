// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"encoding/json"
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
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql"
	"github.com/vkcom/statshouse/internal/promql/parser"
	"golang.org/x/exp/slices"
)

var accessInfoKey contextKey

func (h *Handler) handlePromQuery(w http.ResponseWriter, r *http.Request, rangeQuery bool) {
	// parse access token
	ai, ok := h.parseAccessToken(w, r, nil)
	if !ok {
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
	// execute query
	ctx, cancel := context.WithTimeout(r.Context(), querySelectTimeout)
	defer cancel()
	res, dispose, err := h.promEngine.Exec(context.WithValue(ctx, accessInfoKey, &ai), q)
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
	ai, ok := h.parseAccessToken(w, r, nil)
	if !ok {
		return
	}

	name := mux.Vars(r)["name"]
	if name != "__name__" {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	s := make([]string, 0)
	for _, m := range format.BuiltinMetrics {
		s = append(s, m.Name)
	}
	for _, v := range h.metricsStorage.GetMetaMetricList(h.showInvisible) {
		if ai.canViewMetric(v.Name) {
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
	if (q.End-q.Start)/q.Step > maxSlice {
		return q, fmt.Errorf("exceeded maximum resolution of %d points per timeseries. Try decreasing the query resolution (?step=XX)", maxSlice)
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

func (h *Handler) MatchMetrics(ctx context.Context, matcher *labels.Matcher) ([]*format.MetricMetaValue, []string, error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		// should not happen, return empty set to not reveal security issue
		return nil, nil, nil
	}
	var (
		s1 []*format.MetricMetaValue // metrics
		s2 []string                  // metric match names
		fn = func(metric *format.MetricMetaValue) {
			var name string
			switch {
			case matcher.Matches(metric.Name):
				name = metric.Name
			case matcher.Matches(metric.Name + "_bucket"):
				name = metric.Name + "_bucket"
			default:
				return
			}
			if ai.canViewMetric(matcher.Name) {
				s1 = append(s1, metric)
				s2 = append(s2, name)
			}
		}
	)
	for _, m := range format.BuiltinMetrics {
		fn(m)
	}
	for _, m := range h.metricsStorage.GetMetaMetricList(h.showInvisible) {
		fn(m)
	}
	return s1, s2, nil
}

func (h *Handler) GetQueryLODs(qry promql.Query, maxOffset map[*format.MetricMetaValue]int64, now int64) ([]promql.LOD, error) {
	if len(maxOffset) == 0 {
		return nil, nil
	}
	var widthKind int
	if qry.Options.StepAuto {
		widthKind = widthAutoRes
	} else {
		widthKind = widthAutoRes
	}
	s := make([][]promql.LOD, 0, len(maxOffset))
	for metric, offset := range maxOffset {
		lods := selectQueryLODs(
			Version2,
			int64(metric.PreKeyFrom),
			metric.Resolution,
			false,
			metric.StringTopDescription != "",
			now,
			shiftTimestamp(qry.Start, 1, -offset, h.location),
			shiftTimestamp(qry.End, 1, -offset, h.location),
			h.utcOffset,
			int(qry.Step),
			widthKind,
			h.location)
		if len(lods) != 0 {
			intervals := make([]promql.LOD, 0, len(lods))
			for _, lod := range lods {
				intervals = append(intervals, promql.LOD{Len: (lod.toSec - lod.fromSec) / lod.stepSec, Step: lod.stepSec})
			}
			s = append(s, intervals)
		}
	}
	if len(s) == 0 {
		return nil, nil
	}
	slices.SortFunc(s, func(a, b []promql.LOD) bool {
		d := a[0].Step - b[0].Step
		if d == 0 {
			return a[0].Len > b[0].Len
		} else {
			return d > 0
		}
	})
	return s[0], nil
}

func (h *Handler) GetTagValue(id int32) string {
	v, err := h.getTagValue(id)
	if err != nil {
		return format.CodeTagValue(id)
	}
	return v
}

func (h *Handler) GetTagValueID(v string) (int32, error) {
	return h.getTagValueID(v)
}

func (h *Handler) QuerySeries(ctx context.Context, qry *promql.SeriesQuery) (promql.SeriesBag, func(), error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		// should not happen, return empty set to not reveal security issue
		return promql.SeriesBag{}, func() {}, nil
	}
	var (
		what, qs, pq  = getHandlerArgs(qry, ai)
		lods, timeLen = getHandlerLODs(qry, h.location)
		data, buffers []*[]float64
		maxHost       [][]int32
		tagX          = make(map[tsTags]int, len(qry.GroupBy))
		cleanup       = func() {
			for _, s := range buffers {
				h.putFloatsSlice(s)
			}
		}
		tx int // time index
	)
	for _, lod := range lods {
		m, err := h.cache.Get(ctx, Version2, qs, &pq, lod, false)
		if err != nil {
			cleanup()
			return promql.SeriesBag{}, nil, err
		}
		var (
			t      = lod.fromSec
			factor = qry.Factor
		)
		if factor == 0 {
			factor = lod.stepSec
		}
		for _, s := range m {
			for _, d := range s {
				i, ok := tagX[d.tsTags]
				if !ok {
					i = len(data)
					tagX[d.tsTags] = i
					v := h.getFloatsSlice(timeLen)
					for k := range *v {
						(*v)[k] = promql.NilValue
					}
					buffers = append(buffers, v)
					data = append(data, v)
					if qry.MaxHost {
						maxHost = append(maxHost, make([]int32, timeLen))
					}
				}
				(*data[i])[tx] = selectTSValue(what, qry.MaxHost, lod.stepSec, factor, &d)
				if qry.MaxHost {
					maxHost[i][tx] = d.maxHost
				}
			}
			t += lod.stepSec
			tx++
		}
	}
	meta := make([]promql.SeriesMeta, len(tagX))
	for t, i := range tagX {
		for j, valueID := range t.tag {
			if valueID != 0 && j < len(qry.Meta.Tags) {
				var (
					tag  = qry.Meta.Tags[j]
					name = tag.Name
				)
				if len(name) == 0 {
					name = format.TagID(tag.Index)
				}
				meta[i].SetTag(name, valueID)
			}
		}
	}
	for _, tagID := range qry.GroupBy {
		if tagID == format.StringTopTagID || tagID == qry.Meta.StringTopName {
			name := qry.Meta.StringTopName
			if len(name) == 0 {
				name = format.StringTopTagID
			}
			for v, i := range tagX {
				meta[i].SetSTag(name, emptyToUnspecified(v.tagStr.String()))
			}
			break
		}
	}
	for i := range meta {
		meta[i].Metric = qry.Meta
	}
	if qry.Accumulate {
		for _, row := range data {
			accumulateSeries(*row)
		}
	}
	return promql.SeriesBag{Data: data, Meta: meta, MaxHost: maxHost}, cleanup, nil
}

func (h *Handler) QueryTagValues(ctx context.Context, metric *format.MetricMetaValue, tagX int, from, to int64) ([]int32, error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		return nil, fmt.Errorf("tag not found")
	}
	lods := selectTagValueLODs(
		Version2,
		int64(metric.PreKeyFrom),
		metric.Resolution,
		false,
		metric.StringTopDescription != "",
		time.Now().Unix(),
		from,
		to,
		h.utcOffset,
		h.location,
	)
	pq := &preparedTagValuesQuery{
		version:     Version2,
		metricID:    metric.MetricID,
		preKeyTagID: metric.PreKeyTagID,
		tagID:       format.TagID(tagX),
		numResults:  math.MaxInt - 1,
	}
	tags := make(map[int32]bool)
	for _, lod := range lods {
		body, args, err := tagValuesQuery(pq, lod)
		if err != nil {
			return nil, err
		}
		cols := newTagValuesSelectCols(args)
		isFast := lod.fromSec+fastQueryTimeInterval >= lod.toSec
		err = h.doSelect(ctx, isFast, true, ai.user, Version2, ch.Query{
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

func (h *Handler) QuerySTagValues(ctx context.Context, metric *format.MetricMetaValue, from, to int64) ([]string, error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		return nil, fmt.Errorf("tag not found")
	}
	lods := selectTagValueLODs(
		Version2,
		int64(metric.PreKeyFrom),
		metric.Resolution,
		false,
		metric.StringTopDescription != "",
		time.Now().Unix(),
		from,
		to,
		h.utcOffset,
		h.location,
	)
	pq := &preparedTagValuesQuery{
		version:     Version2,
		metricID:    metric.MetricID,
		preKeyTagID: metric.PreKeyTagID,
		tagID:       format.StringTopTagID,
		numResults:  math.MaxInt - 1,
	}
	tags := make(map[string]bool)
	for _, lod := range lods {
		body, args, err := tagValuesQuery(pq, lod)
		if err != nil {
			return nil, err
		}
		cols := newTagValuesSelectCols(args)
		isFast := lod.fromSec+fastQueryTimeInterval >= lod.toSec
		err = h.doSelect(ctx, isFast, true, ai.user, Version2, ch.Query{
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

func getHandlerArgs(qry *promql.SeriesQuery, ai *accessInfo) (queryFn, string, preparedPointsQuery) {
	// convert "Filter"
	var (
		filterIn  = make(map[string][]string)
		filterInM = make(map[string][]any) // mapped
	)
	for i, m := range qry.FilterIn {
		tagID := format.TagID(i)
		for tagValueID, tagValue := range m {
			filterIn[tagID] = append(filterIn[tagID], tagValue)
			filterInM[tagID] = append(filterInM[tagID], tagValueID)
		}
	}
	for _, tagValue := range qry.SFilterIn {
		filterIn[format.StringTopTagID] = append(filterIn[format.StringTopTagID], tagValue)
		filterInM[format.StringTopTagID] = append(filterInM[format.StringTopTagID], tagValue)
	}
	var (
		filterOut  = make(map[string][]string)
		filterOutM = make(map[string][]any) // mapped
	)
	for i, m := range qry.FilterOut {
		tagID := format.TagID(i)
		for tagValueID, tagValue := range m {
			filterOut[tagID] = append(filterOut[tagID], tagValue)
			filterOutM[tagID] = append(filterOutM[tagID], tagValueID)
		}
	}
	for _, tagValue := range qry.SFilterOut {
		filterOut[format.StringTopTagID] = append(filterOut[format.StringTopTagID], tagValue)
		filterOutM[format.StringTopTagID] = append(filterOutM[format.StringTopTagID], tagValue)
	}
	// get "queryFn"
	var what queryFn
	switch qry.What {
	case promql.DigestCount:
		what = queryFnCount
	case promql.DigestMin:
		what = queryFnMin
	case promql.DigestMax:
		what = queryFnMax
	case promql.DigestSum:
		what = queryFnSum
	case promql.DigestAvg:
		what = queryFnAvg
	case promql.DigestStdDev:
		what = queryFnStddev
	case promql.DigestStdVar:
		what = queryFnStdvar
	case promql.DigestP25:
		what = queryFnP25
	case promql.DigestP50:
		what = queryFnP50
	case promql.DigestP75:
		what = queryFnP75
	case promql.DigestP90:
		what = queryFnP90
	case promql.DigestP95:
		what = queryFnP95
	case promql.DigestP99:
		what = queryFnP99
	case promql.DigestP999:
		what = queryFnP999
	default:
		panic(fmt.Errorf("unrecognized what: %v", qry.What))
	}
	// the rest
	kind := queryFnToQueryFnKind(what, qry.MaxHost)
	qs := normalizedQueryString(qry.Meta.Name, kind, qry.GroupBy, filterIn, filterOut)
	pq := preparedPointsQuery{
		user:        ai.user,
		version:     Version2,
		metricID:    qry.Meta.MetricID,
		preKeyTagID: qry.Meta.PreKeyTagID,
		isStringTop: qry.Meta.StringTopDescription != "",
		kind:        kind,
		by:          qry.GroupBy,
		filterIn:    filterInM,
		filterNotIn: filterOutM,
	}
	return what, qs, pq
}

func getHandlerLODs(qry *promql.SeriesQuery, loc *time.Location) ([]lodInfo, int) {
	var (
		from       = qry.From
		preKeyFrom = int64(qry.Meta.PreKeyFrom)
		lods       = make([]lodInfo, 0, len(qry.LODs))
		timeLen    int
	)
	for _, lod := range qry.LODs {
		to := from + lod.Step*lod.Len
		if from <= preKeyFrom && preKeyFrom < to {
			split := from + lod.Step*((preKeyFrom-from-1)/lod.Step+1)
			lods = append(lods, lodInfo{
				fromSec:   from,
				toSec:     split,
				stepSec:   lod.Step,
				table:     lodTables[Version2][lod.Step],
				hasPreKey: false,
				location:  loc,
			})
			from = split
		}
		lods = append(lods, lodInfo{
			fromSec:   from,
			toSec:     to,
			stepSec:   lod.Step,
			table:     lodTables[Version2][lod.Step],
			hasPreKey: preKeyFrom < from,
			location:  loc,
		})
		timeLen += int(lod.Len)
		from = to
	}
	return lods, timeLen
}

func getAccessInfo(ctx context.Context) *accessInfo {
	if ai, ok := ctx.Value(accessInfoKey).(*accessInfo); ok {
		return ai
	}
	return nil
}

func (h *Handler) Alloc(n int) *[]float64 {
	if n > maxSlice {
		panic(fmt.Errorf("exceeded maximum resolution of %d points per timeseries. Try decreasing the query resolution (?step=XX)", maxSlice))
	}
	return h.getFloatsSlice(n)
}
func (h *Handler) Free(s *[]float64) {
	h.putFloatsSlice(s)
}

func getPromQuery(req getQueryReq) string {
	var res []string
	for _, fn := range req.what {
		name, ok := validQueryFn(fn)
		if !ok {
			continue
		}
		var (
			what string
			aggr string
			rate bool
		)
		switch name {
		case queryFnCount:
			what = promql.Count
			aggr = "sum"
		case queryFnCountNorm:
			what = promql.CountSec
			aggr = "sum"
		case queryFnCumulCount:
			what = promql.CountAcc
			aggr = "sum"
		case queryFnCardinality:
			what = promql.Cardinality
			aggr = "sum"
			continue
		case queryFnCardinalityNorm:
			what = promql.CardinalitySec
			aggr = "sum"
			continue
		case queryFnCumulCardinality:
			what = promql.CardinalityAcc
			aggr = "sum"
			continue
		case queryFnMin:
			what = promql.Min
			aggr = "min"
		case queryFnMax:
			what = promql.Max
			aggr = "max"
		case queryFnAvg:
			what = promql.Avg
			aggr = "avg"
		case queryFnCumulAvg:
			what = promql.AvgAcc
			aggr = "avg"
		case queryFnSum:
			what = promql.Sum
			aggr = "sum"
		case queryFnSumNorm:
			what = promql.SumSec
			aggr = "sum"
		case queryFnCumulSum:
			what = promql.SumAcc
			aggr = "sum"
		case queryFnStddev:
			what = promql.StdDev
			aggr = "stddev"
		case queryFnP25:
			what = promql.P25
		case queryFnP50:
			what = promql.P50
		case queryFnP75:
			what = promql.P75
		case queryFnP90:
			what = promql.P90
		case queryFnP95:
			what = promql.P95
		case queryFnP99:
			what = promql.P99
		case queryFnP999:
			what = promql.P999
		case queryFnUnique:
			what = promql.Unique
			continue
		case queryFnUniqueNorm:
			what = promql.UniqueSec
			continue
		case queryFnMaxHost:
			req.maxHost = true
			continue
		case queryFnMaxCountHost:
			req.maxHost = true
			continue
		case queryFnDerivativeCount:
			what = promql.Count
			aggr = "sum"
			rate = true
		case queryFnDerivativeSum:
			what = promql.Sum
			aggr = "sum"
			rate = true
		case queryFnDerivativeAvg:
			what = promql.Avg
			aggr = "avg"
			rate = true
		case queryFnDerivativeCountNorm:
			what = promql.CountSec
			aggr = "sum"
			rate = true
		case queryFnDerivativeSumNorm:
			what = promql.SumSec
			aggr = "sum"
			rate = true
		case queryFnDerivativeMin:
			what = promql.Min
			aggr = "min"
			rate = true
		case queryFnDerivativeMax:
			what = promql.Max
			aggr = "max"
			rate = true
		case queryFnDerivativeUnique:
			what = promql.Unique
			aggr = "sum"
			rate = true
			continue
		case queryFnDerivativeUniqueNorm:
			what = promql.UniqueSec
			aggr = "sum"
			rate = true
			continue
		default:
			continue
		}
		// vector selectors
		var s []string
		s = append(s, fmt.Sprintf("__what__=%q", what))
		if req.maxHost {
			s = append(s, fmt.Sprintf("__what__=%q", promql.MaxHost))
		}
		for t, v := range req.filterIn {
			s = append(s, fmt.Sprintf("%s=~%q", t, strings.Join(v, "|")))
		}
		for t, v := range req.filterNotIn {
			s = append(s, fmt.Sprintf("%s!~%q", t, strings.Join(v, "|")))
		}
		q := fmt.Sprintf("%s{%s}", req.metricWithNamespace, strings.Join(s, ","))
		// transformations
		if aggr != "" {
			q = fmt.Sprintf("%s by (%s) (%s)", aggr, strings.Join(req.by, ","), q)
		}
		if rate {
			q = fmt.Sprintf("idelta(%s)", q)
		}
		// label_replace
		q = fmt.Sprintf("label_replace(%s,%q,%q,%q,%q)", q, "__name__", name.String(), "__name__", ".*")
		res = append(res, q)
	}
	return fmt.Sprintf("topk(%s,%s)", req.numResults, strings.Join(res, " or "))
}

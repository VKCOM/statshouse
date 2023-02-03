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
		err = fmt.Errorf("invalid parameter start: %w", err)
		return
	}

	q.End, err = parseTime(r.FormValue("end"))
	if err != nil {
		err = fmt.Errorf("invalid parameter end: %w", err)
		return
	}
	if q.End < q.Start {
		err = fmt.Errorf("invalid parameter end: end timestamp must not be before start time")
		return
	}

	q.Step, err = parseDuration(r.FormValue("step"))
	if err != nil {
		err = fmt.Errorf("invalid parameter step: %w", err)
		return
	}
	if q.Step <= 0 {
		err = fmt.Errorf("invalid parameter step: zero or negative handleQuery resolution step widths are not accepted. Try a positive integer")
		return
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if (q.End-q.Start)/q.Step > maxSlice {
		err = fmt.Errorf("exceeded maximum resolution of %d points per timeseries. Try decreasing the query resolution (?step=XX)", maxSlice)
		return
	}

	q.Expr = r.FormValue("query")
	return
}

func parsePromInstantQuery(r *http.Request) (q promql.Query, err error) {
	v := r.FormValue("time")
	if v == "" {
		q.Start = time.Now().Unix()
	} else {
		q.Start, err = parseTime(v)
		if err != nil {
			err = fmt.Errorf("invalid parameter time: %w", err)
			return
		}
	}
	q.End = q.Start
	q.Expr = r.FormValue("query")
	return
}

func parseTime(s string) (int64, error) {
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		sec, ns := math.Modf(v)
		if ns != 0 {
			return 0, fmt.Errorf("duration cannot be less than a second")
		}
		return int64(sec), nil
	}
	if v, err := time.Parse(time.RFC3339, s); err == nil {
		return v.Unix(), nil
	}
	return 0, fmt.Errorf("cannot parse %qs to a valid timestamp", s)
}

func parseDuration(s string) (int64, error) {
	if v, err := strconv.ParseInt(s, 10, 32); err == nil {
		if v < 1 {
			return 0, fmt.Errorf("duration cannot be less than a second")
		}
		return v, nil
	}
	if v, err := model.ParseDuration(s); err == nil {
		rem := v % model.Duration(time.Second)
		if rem != 0 {
			return 0, fmt.Errorf("duration cannot be less than a second")
		}
		return int64(v / model.Duration(time.Second)), nil
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

// region Data Access

func (h *Handler) MatchMetrics(ctx context.Context, matcher *labels.Matcher) ([]*format.MetricMetaValue, error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		// should not happen, return empty set to not reveal potential security issue
		return nil, nil
	}
	var s []*format.MetricMetaValue
	match := func(meta *format.MetricMetaValue, matcher *labels.Matcher) {
		if matcher.Matches(meta.Name) || matcher.Matches(meta.Name+"_bucket") {
			if ai.canViewMetric(matcher.Name) {
				s = append(s, meta)
			}
		}
	}
	for _, meta := range format.BuiltinMetrics {
		match(meta, matcher)
	}
	for _, meta := range h.metricsStorage.GetMetaMetricList(h.showInvisible) {
		match(meta, matcher)
	}
	return s, nil
}

func (h *Handler) GetQueryLODs(qry promql.Query, maxOffset map[*format.MetricMetaValue]int64, now int64) ([]promql.LOD, error) {
	if len(maxOffset) == 0 {
		return nil, nil
	}
	s := make([][]promql.LOD, 0, len(maxOffset))
	for meta, offset := range maxOffset {
		lods := selectQueryLODs(
			Version2,
			int64(meta.PreKeyFrom),
			meta.Resolution,
			false,
			meta.StringTopDescription != "",
			now,
			shiftTimestamp(qry.Start, 1, -offset, h.location),
			shiftTimestamp(qry.End+1, 1, -offset, h.location),
			h.utcOffset,
			int(qry.Step),
			widthLODRes,
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
		// should not happen, return empty set to not reveal potential security issue
		return promql.SeriesBag{}, func() {}, nil
	}
	var (
		i             int
		what, qs, pq  = getHandlerArgs(qry, ai)
		lods, timeLen = getHandlerLODs(qry, h.location)
		data          []*[]float64
		tagsIx        = make(map[tsTags]int, len(qry.GroupBy))
		buffers       []*[]float64
		cleanup       = func() {
			for _, s := range buffers {
				h.putFloatsSlice(s)
			}
		}
	)
	for _, lod := range lods {
		m, err := h.cache.Get(ctx, Version2, qs, &pq, lod, false)
		if err != nil {
			cleanup()
			return promql.SeriesBag{}, nil, err
		}
		t := lod.fromSec
		for _, s := range m {
			for _, p := range s {
				j, ok := tagsIx[p.tsTags]
				if !ok {
					j = len(data)
					tagsIx[p.tsTags] = j
					v := h.getFloatsSlice(timeLen)
					for k := range *v {
						(*v)[k] = promql.NilValue
					}
					buffers = append(buffers, v)
					data = append(data, v)
				}
				(*data[j])[i] = selectTSValue(what, lod.stepSec, lod.stepSec, &p)
			}
			t += lod.stepSec
			i++
		}
	}
	tags := make([]map[string]int32, len(tagsIx))
	stags := make([]map[string]string, len(tagsIx))
	for v, j := range tagsIx {
		ti := map[string]int32{}
		for ix, id := range v.tag {
			if id != 0 && ix < len(qry.Meta.Tags) {
				ti[qry.Meta.Tags[ix].Name] = id
			}
		}
		tags[j] = ti
		stags[j] = map[string]string{labels.MetricName: qry.Meta.Name}
	}
	for _, tagID := range qry.GroupBy {
		if tagID == format.StringTopTagID || tagID == qry.Meta.StringTopName {
			name := qry.Meta.StringTopName
			if len(name) == 0 {
				name = format.StringTopTagID
			}
			for v, j := range tagsIx {
				stags[j][name] = emptyToUnspecified(v.tagStr.String())
			}
			break
		}
	}
	if qry.What == promql.AggregateCount {
		for _, row := range data {
			accumulateSeries(*row)
		}
	}
	return promql.SeriesBag{Data: data, Tags: tags, STags: stags}, cleanup, nil
}

func (h *Handler) QueryTagValues(ctx context.Context, meta *format.MetricMetaValue, tagIx int, from, to int64) ([]int32, error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		return nil, fmt.Errorf("tag not found")
	}
	lods := selectTagValueLODs(
		Version2,
		int64(meta.PreKeyFrom),
		meta.Resolution,
		false,
		meta.StringTopDescription != "",
		time.Now().Unix(),
		from,
		to,
		h.utcOffset,
		h.location,
	)
	pq := &preparedTagValuesQuery{
		version:     Version2,
		metricID:    meta.MetricID,
		preKeyTagID: meta.PreKeyTagID,
		tagID:       format.TagID(tagIx),
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
	ids := make([]int32, 0, len(tags))
	for id := range tags {
		ids = append(ids, id)
	}
	return ids, nil
}

func (h *Handler) QuerySTagValues(ctx context.Context, meta *format.MetricMetaValue, from, to int64) ([]string, error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		return nil, fmt.Errorf("tag not found")
	}
	lods := selectTagValueLODs(
		Version2,
		int64(meta.PreKeyFrom),
		meta.Resolution,
		false,
		meta.StringTopDescription != "",
		time.Now().Unix(),
		from,
		to,
		h.utcOffset,
		h.location,
	)
	pq := &preparedTagValuesQuery{
		version:     Version2,
		metricID:    meta.MetricID,
		preKeyTagID: meta.PreKeyTagID,
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

func getHandlerArgs(qry *promql.SeriesQuery, ai *accessInfo) (what queryFn, qs string, pq preparedPointsQuery) {
	// convert "Filter"
	var (
		filterIn       = make(map[string][]string)
		mappedFilterIn = make(map[string][]any)
	)
	for ix, values := range qry.FilterIn {
		tagID := format.TagID(ix)
		for id, str := range values {
			filterIn[tagID] = append(filterIn[tagID], str)
			mappedFilterIn[tagID] = append(mappedFilterIn[tagID], id)
		}
	}
	for _, str := range qry.SFilterIn {
		filterIn[format.StringTopTagID] = append(filterIn[format.StringTopTagID], str)
		mappedFilterIn[format.StringTopTagID] = append(mappedFilterIn[format.StringTopTagID], str)
	}
	var (
		filterNotIn       = make(map[string][]string)
		mappedFilterNotIn = make(map[string][]any)
	)
	for ix, values := range qry.FilterOut {
		tagID := format.TagID(ix)
		for id, str := range values {
			filterNotIn[tagID] = append(filterNotIn[tagID], str)
			mappedFilterNotIn[tagID] = append(mappedFilterNotIn[tagID], id)
		}
	}
	for _, str := range qry.SFilterOut {
		filterNotIn[format.StringTopTagID] = append(filterNotIn[format.StringTopTagID], str)
		mappedFilterNotIn[format.StringTopTagID] = append(mappedFilterNotIn[format.StringTopTagID], str)
	}
	// get "queryFn"
	switch {
	case qry.What == promql.AggregateCount:
		what = queryFnCount
	case qry.What == promql.Min:
		what = queryFnMin
	case qry.What == promql.Max:
		what = queryFnMax
	case qry.What == promql.Sum:
		what = queryFnSumNorm
	case qry.What == promql.Avg:
		what = queryFnAvg
	default:
		what = queryFnCountNorm
	}
	// the rest
	kind := queryFnToQueryFnKind(what)
	qs = normalizedQueryString(qry.Meta.Name, kind, qry.GroupBy, filterIn, filterNotIn)
	pq = preparedPointsQuery{
		user:        ai.user,
		version:     Version2,
		metricID:    qry.Meta.MetricID,
		preKeyTagID: qry.Meta.PreKeyTagID,
		isStringTop: qry.Meta.StringTopDescription != "",
		kind:        kind,
		by:          qry.GroupBy,
		filterIn:    mappedFilterIn,
		filterNotIn: mappedFilterNotIn,
	}
	return
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

// endregion

// region Allocator

func (h *Handler) Alloc(n int) *[]float64 {
	if n > maxSlice {
		panic(fmt.Errorf("exceeded maximum resolution of %d points per timeseries. Try decreasing the query resolution (?step=XX)", maxSlice))
	}
	return h.getFloatsSlice(n)
}
func (h *Handler) Free(s *[]float64) {
	h.putFloatsSlice(s)
}

// endregion

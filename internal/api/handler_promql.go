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
	"golang.org/x/exp/slices"

	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql"
	"github.com/vkcom/statshouse/internal/promql/parser"
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

func (h *Handler) GetTimescale(qry promql.Query, offsets map[*format.MetricMetaValue]int64) (promql.Timescale, error) {
	var widthKind int
	if qry.Options.StepAuto {
		widthKind = widthAutoRes
	} else {
		widthKind = widthLODRes
	}
	getLODs := func(metric *format.MetricMetaValue, offset int64) []lodInfo {
		var (
			resolution int
			stringTop  bool
		)
		if metric != nil {
			resolution = metric.Resolution
			stringTop = metric.StringTopDescription != ""
		}
		return selectQueryLODs(
			promqlVersionOrDefault(qry.Options.Version),
			0,
			resolution,
			false,
			stringTop,
			qry.Options.TimeNow,
			shiftTimestamp(qry.Start, qry.Step, -offset, h.location),
			shiftTimestamp(qry.End, qry.Step, -offset, h.location),
			h.utcOffset,
			int(qry.Step),
			widthKind,
			h.location)
	}
	type timescale struct {
		lods   []lodInfo
		offset int64
	}
	var t timescale
	if len(offsets) == 0 {
		t.lods = getLODs(nil, 0)
	} else {
		res := make([]timescale, 0, len(offsets))
		for metric, offset := range offsets {
			if s := getLODs(metric, offset); len(s) != 0 {
				res = append(res, timescale{s, offset})
			}
		}
		if len(res) == 0 {
			return promql.Timescale{}, fmt.Errorf("empty time interval")
		}
		slices.SortFunc(res, func(a, b timescale) bool {
			d := a.lods[0].stepSec - b.lods[0].stepSec
			if d == 0 {
				return a.lods[0].toSec-a.lods[0].fromSec > b.lods[0].toSec-b.lods[0].fromSec
			} else {
				return d > 0
			}
		})
		t = res[0]
	}
	var (
		fl  = t.lods[0]             // first LOD
		ll  = t.lods[len(t.lods)-1] // last LOD
		res = promql.Timescale{
			Start:  shiftTimestamp(fl.fromSec, fl.stepSec, t.offset, h.location),   // inclusive
			End:    shiftTimestamp(ll.toSec, ll.stepSec, t.offset, h.location) + 1, // exclusive
			Offset: t.offset,
		}
	)
	// extend the interval by one from the left so that the
	// derivative (if any) at the first point can be calculated
	t.lods[0].fromSec -= t.lods[0].stepSec
	// generate time
	for _, lod := range t.lods {
		s := lod.generateTimePoints(-t.offset)
		res.LODs = append(res.LODs, promql.LOD{
			Start: lod.fromSec,
			End:   lod.toSec,
			Step:  lod.stepSec,
			Len:   len(s),
		})
		res.Time = append(res.Time, s...)
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
	return h.getRichTagValue(qry.Metric, promqlVersionOrDefault(qry.Version), tagID, qry.TagValueID)
}

func (h *Handler) GetTagValueID(qry promql.TagValueIDQuery) (int32, error) {
	res, err := h.getRichTagValueID(&qry.Metric.Tags[qry.TagIndex], qry.Version, qry.TagValue)
	if err != nil && httpCode(err) == http.StatusNotFound {
		err = promql.ErrNotFound
	}
	return res, err
}

func (h *Handler) QuerySeries(ctx context.Context, qry *promql.SeriesQuery) (promql.SeriesBag, func(), error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		// should not happen, return empty set to not reveal security issue
		return promql.SeriesBag{}, func() {}, nil
	}
	var (
		version       = promqlVersionOrDefault(qry.Options.Version)
		what, qs, pq  = getHandlerArgs(qry, ai)
		lods          = getHandlerLODs(qry, h.location)
		shift         = qry.Timescale.Offset - qry.Offset
		timeLen       = len(qry.Timescale.Time)
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
		li := lodInfo{
			fromSec:   shiftTimestamp(lod.fromSec, lod.stepSec, shift, h.location),
			toSec:     shiftTimestamp(lod.toSec, lod.stepSec, shift, h.location),
			stepSec:   lod.stepSec,
			table:     lod.table,
			hasPreKey: lod.hasPreKey,
			location:  lod.location,
		}
		if qry.Options.SeriesQueryCallback != nil {
			qry.Options.SeriesQueryCallback(version, qs, &pq, li, qry.Options.AvoidCache)
		}
		m, err := h.cache.Get(ctx, version, qs, &pq, li, qry.Options.AvoidCache)
		if err != nil {
			cleanup()
			return promql.SeriesBag{}, nil, err
		}
		factor := qry.Factor
		if factor == 0 {
			factor = lod.stepSec
		}
		for _, col := range m {
			for _, d := range col {
				var (
					i, ok = tagX[d.tsTags]
					j     = tx + lod.lodInfo.getIndexForTimestamp(d.time, shift)
				)
				if !ok {
					i = len(data)
					tagX[d.tsTags] = i
					s := h.getFloatsSlice(timeLen)
					for k := range *s {
						(*s)[k] = promql.NilValue
					}
					buffers = append(buffers, s)
					data = append(data, s)
					if qry.MaxHost {
						maxHost = append(maxHost, make([]int32, timeLen))
					}
				}
				(*data[i])[j] = selectTSValue(what, qry.MaxHost, lod.stepSec, factor, &d)
				if qry.MaxHost {
					maxHost[i][j] = d.maxHost
				}
			}
		}
		tx += lod.len
	}
	meta := make([]promql.SeriesMeta, len(tagX))
	for t, i := range tagX {
		for _, tagID := range qry.GroupBy {
			if tagID == format.StringTopTagID || tagID == qry.Metric.StringTopName {
				name := qry.Metric.StringTopName
				if len(name) == 0 {
					name = format.StringTopTagID
				}
				meta[i].SetSTag(name, emptyToUnspecified(t.tagStr.String()))
			} else if tag, ok := qry.Metric.Name2Tag[tagID]; ok && tag.Index < len(t.tag) {
				var name string
				if len(tag.Name) == 0 {
					name = format.TagID(tag.Index)
				} else {
					name = tag.Name
				}
				meta[i].SetTag(name, t.tag[tag.Index])
			}
		}
	}
	for i := range meta {
		meta[i].Metric = qry.Metric
	}
	return promql.SeriesBag{Data: data, Meta: meta, MaxHost: maxHost}, cleanup, nil
}

func (h *Handler) QueryTagValueIDs(ctx context.Context, qry promql.TagValuesQuery) ([]int32, error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		return nil, fmt.Errorf("tag not found")
	}
	var (
		version = promqlVersionOrDefault(qry.Version)
		fl      = qry.Timescale.LODs[0]                         // first LOD
		ll      = qry.Timescale.LODs[len(qry.Timescale.LODs)-1] // last LOD
		lods    = selectTagValueLODs(
			version,
			int64(qry.Metric.PreKeyFrom),
			qry.Metric.Resolution,
			false,
			qry.Metric.StringTopDescription != "",
			qry.Options.TimeNow,
			shiftTimestamp(fl.Start, fl.Step, qry.Offset, h.location),
			shiftTimestamp(ll.End, ll.Step, qry.Offset, h.location),
			h.utcOffset,
			h.location,
		)
		pq = &preparedTagValuesQuery{
			version:     version,
			metricID:    qry.Metric.MetricID,
			preKeyTagID: qry.Metric.PreKeyTagID,
			tagID:       format.TagID(qry.TagIndex),
			numResults:  math.MaxInt - 1,
		}
		tags = make(map[int32]bool)
	)
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

func (h *Handler) QuerySTagValues(ctx context.Context, qry promql.TagValuesQuery) ([]string, error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		return nil, fmt.Errorf("tag not found")
	}
	var (
		version = promqlVersionOrDefault(qry.Version)
		fl      = qry.Timescale.LODs[0]                         // first LOD
		ll      = qry.Timescale.LODs[len(qry.Timescale.LODs)-1] // last LOD
		lods    = selectTagValueLODs(
			version,
			int64(qry.Metric.PreKeyFrom),
			qry.Metric.Resolution,
			false,
			qry.Metric.StringTopDescription != "",
			time.Now().Unix(),
			shiftTimestamp(fl.Start, fl.Step, qry.Offset, h.location),
			shiftTimestamp(ll.End, ll.Step, qry.Offset, h.location),
			h.utcOffset,
			h.location,
		)
		pq = &preparedTagValuesQuery{
			version:     version,
			metricID:    qry.Metric.MetricID,
			preKeyTagID: qry.Metric.PreKeyTagID,
			tagID:       format.StringTopTagID,
			numResults:  math.MaxInt - 1,
		}
		tags = make(map[string]bool)
	)
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
		filterIn[format.StringTopTagID] = append(filterIn[format.StringTopTagID], promqlEncodeSTagValue(tagValue))
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
		filterOut[format.StringTopTagID] = append(filterOut[format.StringTopTagID], promqlEncodeSTagValue(tagValue))
		filterOutM[format.StringTopTagID] = append(filterOutM[format.StringTopTagID], tagValue)
	}
	// get "queryFn"
	var what queryFn
	switch qry.What {
	case promql.DigestCount:
		what = queryFnCount
	case promql.DigestCountSec:
		what = queryFnCountNorm
	case promql.DigestMin:
		what = queryFnMin
	case promql.DigestMax:
		what = queryFnMax
	case promql.DigestSum:
		what = queryFnSum
	case promql.DigestSumSec:
		what = queryFnSumNorm
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
	case promql.DigestCardinality:
		what = queryFnCardinality
	case promql.DigestCardinalitySec:
		what = queryFnCardinalityNorm
	case promql.DigestUnique:
		what = queryFnUnique
	case promql.DigestUniqueSec:
		what = queryFnUniqueNorm
	default:
		panic(fmt.Errorf("unrecognized what: %v", qry.What))
	}
	// the rest
	kind := queryFnToQueryFnKind(what, qry.MaxHost)
	qs := normalizedQueryString(qry.Metric.Name, kind, qry.GroupBy, filterIn, filterOut, false)
	pq := preparedPointsQuery{
		user:        ai.user,
		version:     promqlVersionOrDefault(qry.Options.Version),
		metricID:    qry.Metric.MetricID,
		preKeyTagID: qry.Metric.PreKeyTagID,
		isStringTop: qry.Metric.StringTopDescription != "",
		kind:        kind,
		by:          qry.GroupBy,
		filterIn:    filterInM,
		filterNotIn: filterOutM,
	}
	return what, qs, pq
}

type promqlLOD struct {
	lodInfo
	len int
}

func getHandlerLODs(qry *promql.SeriesQuery, loc *time.Location) []promqlLOD {
	var (
		lods       = make([]promqlLOD, 0, len(qry.Timescale.LODs))
		preKeyFrom = int64(qry.Metric.PreKeyFrom)
	)
	if preKeyFrom == 0 {
		preKeyFrom = math.MaxInt64 // "preKeyFrom < start" is always false
	}
	for _, lod := range qry.Timescale.LODs {
		lods = append(lods, promqlLOD{
			lodInfo: lodInfo{
				fromSec:   lod.Start,
				toSec:     lod.End,
				stepSec:   lod.Step,
				table:     lodTables[promqlVersionOrDefault(qry.Options.Version)][lod.Step],
				hasPreKey: preKeyFrom < lod.Start,
				location:  loc,
			},
			len: lod.Len,
		})
	}
	return lods
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

func getPromQuery(req seriesRequest) string {
	if len(req.promQL) != 0 {
		return req.promQL
	}
	var res []string
	for _, fn := range req.what {
		name, ok := validQueryFn(fn)
		if !ok {
			continue
		}
		var (
			what  string
			deriv bool
			cumul bool
		)
		switch name {
		case queryFnCount:
			what = promql.Count
		case queryFnCountNorm:
			what = promql.CountSec
		case queryFnCumulCount:
			what = promql.Count
			cumul = true
		case queryFnCardinality:
			what = promql.Cardinality
		case queryFnCardinalityNorm:
			what = promql.CardinalitySec
		case queryFnCumulCardinality:
			what = promql.Cardinality
			cumul = true
		case queryFnMin:
			what = promql.Min
		case queryFnMax:
			what = promql.Max
		case queryFnAvg:
			what = promql.Avg
		case queryFnCumulAvg:
			what = promql.Avg
			cumul = true
		case queryFnSum:
			what = promql.Sum
		case queryFnSumNorm:
			what = promql.SumSec
		case queryFnCumulSum:
			what = promql.Sum
			cumul = true
		case queryFnStddev:
			what = promql.StdDev
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
		case queryFnUniqueNorm:
			what = promql.UniqueSec
		case queryFnMaxHost:
			req.maxHost = true
		case queryFnMaxCountHost:
			req.maxHost = true
		case queryFnDerivativeCount:
			what = promql.Count
			deriv = true
		case queryFnDerivativeSum:
			what = promql.Sum
			deriv = true
		case queryFnDerivativeAvg:
			what = promql.Avg
			deriv = true
		case queryFnDerivativeCountNorm:
			what = promql.CountSec
			deriv = true
		case queryFnDerivativeSumNorm:
			what = promql.SumSec
			deriv = true
		case queryFnDerivativeMin:
			what = promql.Min
			deriv = true
		case queryFnDerivativeMax:
			what = promql.Max
			deriv = true
		case queryFnDerivativeUnique:
			what = promql.Unique
			deriv = true
		case queryFnDerivativeUniqueNorm:
			what = promql.UniqueSec
			deriv = true
		default:
			continue
		}
		var s []string
		s = append(s, fmt.Sprintf("__what__=%q", what))
		s = append(s, fmt.Sprintf("__by__=%q", strings.Join(req.by, ",")))
		for t, in := range req.filterIn {
			for _, v := range in {
				s = append(s, fmt.Sprintf("%s=%q", t, promqlGetFilterValue(t, v)))
			}
		}
		for t, out := range req.filterNotIn {
			for _, v := range out {
				s = append(s, fmt.Sprintf("%s!=%q", t, promqlGetFilterValue(t, v)))
			}
		}
		q := fmt.Sprintf("%s{%s}", req.metricWithNamespace, strings.Join(s, ","))
		if numResults, err := strconv.ParseInt(req.numResults, 10, 32); err == nil {
			if numResults < 0 {
				q = fmt.Sprintf("bottomk(%d,%s)", -numResults, q)
			} else {
				q = fmt.Sprintf("topk(%d,%s)", numResults, q)
			}
		}
		if deriv {
			q = fmt.Sprintf("idelta(%s)", q)
		}
		if cumul {
			q = fmt.Sprintf("prefix_sum(%s)", q)
		}
		q = fmt.Sprintf("label_replace(%s,%q,%q,%q,%q)", q, "__name__", name.String(), "__name__", ".*")
		res = append(res, q)
	}
	return strings.Join(res, " or ")
}

func promqlGetFilterValue(tagID string, s string) string {
	switch {
	case tagID == format.StringTopTagID && s == format.TagValueCodeZero:
		return ""
	default:
		return s
	}
}

func promqlEncodeSTagValue(s string) string {
	if s == "" {
		return format.TagValueCodeZero
	}
	return s
}

func promqlVersionOrDefault(version string) string {
	if len(version) != 0 {
		return version
	}
	return Version2
}

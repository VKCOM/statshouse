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

func (h *Handler) GetQueryLODs(qry promql.Query, maxOffset map[*format.MetricMetaValue]int64) ([]promql.LOD, int64) {
	var widthKind int
	if qry.Options.StepAuto {
		widthKind = widthAutoRes
	} else {
		widthKind = widthLODRes
	}
	getLODs := func(metric *format.MetricMetaValue, offset int64) ([]promql.LOD, int64) {
		var (
			preKeyFrom int64
			resolution int
			stringTop  bool
		)
		if metric != nil {
			preKeyFrom = int64(metric.PreKeyFrom)
			resolution = metric.Resolution
			stringTop = metric.StringTopDescription != ""
		}
		lods := selectQueryLODs(
			promqlVersionOrDefault(qry.Options.Version),
			preKeyFrom,
			resolution,
			false,
			stringTop,
			qry.Options.TimeNow,
			shiftTimestamp(qry.Start, 1, -offset, h.location),
			shiftTimestamp(qry.End, 1, -offset, h.location),
			h.utcOffset,
			int(qry.Step),
			widthKind,
			h.location)
		if len(lods) == 0 {
			return nil, 0
		}
		res := make([]promql.LOD, 0, len(lods))
		for _, lod := range lods {
			res = append(res, promql.LOD{Len: (lod.toSec - lod.fromSec) / lod.stepSec, Step: lod.stepSec})
		}
		return res, lods[0].fromSec
	}
	if len(maxOffset) == 0 {
		return getLODs(nil, 0)
	}
	type lods struct {
		v []promql.LOD
		t int64
	}
	res := make([]lods, 0, len(maxOffset))
	for metric, offset := range maxOffset {
		if s, t := getLODs(metric, offset); len(s) != 0 {
			res = append(res, lods{s, t})
		}
	}
	if len(res) == 0 {
		return nil, 0
	}
	slices.SortFunc(res, func(a, b lods) bool {
		d := a.v[0].Step - b.v[0].Step
		if d == 0 {
			return a.v[0].Len > b.v[0].Len
		} else {
			return d > 0
		}
	})
	return res[0].v, res[0].t
}

func (h *Handler) GetTagValue(id int32) string {
	v, err := h.getTagValue(id)
	if err != nil {
		return format.CodeTagValue(id)
	}
	return v
}

func (h *Handler) GetRichTagValue(qry promql.RichTagValueQuery) string {
	return h.getRichTagValue(qry.Meta, promqlVersionOrDefault(qry.Version), qry.TagID, qry.TagValueID)
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
		version       = promqlVersionOrDefault(qry.Options.Version)
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
		m, err := h.cache.Get(ctx, version, qs, &pq, lod, qry.Options.AvoidCache)
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
		for _, tagID := range qry.GroupBy {
			if tagID == format.StringTopTagID || tagID == qry.Meta.StringTopName {
				name := qry.Meta.StringTopName
				if len(name) == 0 {
					name = format.StringTopTagID
				}
				meta[i].SetSTag(name, emptyToUnspecified(t.tagStr.String()))
			} else if tag, ok := qry.Meta.Name2Tag[tagID]; ok && tag.Index < len(t.tag) {
				var name string
				if qry.Options.CanonicalTagNames || len(tag.Name) == 0 {
					name = format.TagID(tag.Index)
				} else {
					name = tag.Name
				}
				meta[i].SetTag(name, t.tag[tag.Index])
			}
		}
	}
	for i := range meta {
		meta[i].Metric = qry.Meta
	}
	return promql.SeriesBag{Data: data, Meta: meta, MaxHost: maxHost}, cleanup, nil
}

func (h *Handler) QueryTagValues(ctx context.Context, qry promql.TagValuesQuery) ([]int32, error) {
	ai := getAccessInfo(ctx)
	if ai == nil {
		return nil, fmt.Errorf("tag not found")
	}
	var (
		version = promqlVersionOrDefault(qry.Version)
		lods    = selectTagValueLODs(
			version,
			int64(qry.Meta.PreKeyFrom),
			qry.Meta.Resolution,
			false,
			qry.Meta.StringTopDescription != "",
			time.Now().Unix(),
			qry.Start,
			qry.End,
			h.utcOffset,
			h.location,
		)
		pq = &preparedTagValuesQuery{
			version:     version,
			metricID:    qry.Meta.MetricID,
			preKeyTagID: qry.Meta.PreKeyTagID,
			tagID:       format.TagID(qry.TagX),
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
		lods    = selectTagValueLODs(
			version,
			int64(qry.Meta.PreKeyFrom),
			qry.Meta.Resolution,
			false,
			qry.Meta.StringTopDescription != "",
			time.Now().Unix(),
			qry.Start,
			qry.End,
			h.utcOffset,
			h.location,
		)
		pq = &preparedTagValuesQuery{
			version:     version,
			metricID:    qry.Meta.MetricID,
			preKeyTagID: qry.Meta.PreKeyTagID,
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
		filterIn[format.StringTopTagID] = append(filterIn[format.StringTopTagID], promqlEmptyToUnspecified(tagValue))
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
		filterOut[format.StringTopTagID] = append(filterOut[format.StringTopTagID], promqlEmptyToUnspecified(tagValue))
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
	case promql.DigestCardinality:
		what = queryFnCardinality
	case promql.DigestUnique:
		what = queryFnUnique
	default:
		panic(fmt.Errorf("unrecognized what: %v", qry.What))
	}
	// the rest
	kind := queryFnToQueryFnKind(what, qry.MaxHost)
	qs := normalizedQueryString(qry.Meta.Name, kind, qry.GroupBy, filterIn, filterOut)
	pq := preparedPointsQuery{
		user:        ai.user,
		version:     promqlVersionOrDefault(qry.Options.Version),
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
	width, _, err := parseWidth(req.width, req.widthAgg)
	if err != nil {
		return ""
	}
	shifts, err := parseTimeShifts(req.timeShifts, width)
	if err != nil {
		return ""
	}
	for _, fn := range req.what {
		name, ok := validQueryFn(fn)
		if !ok {
			continue
		}
		var (
			what  string
			norm  bool
			deriv bool
			cumul bool
		)
		switch name {
		case queryFnCount:
			what = promql.Count
		case queryFnCountNorm:
			what = promql.Count
			norm = true
		case queryFnCumulCount:
			what = promql.Count
			cumul = true
		case queryFnCardinality:
			what = promql.Cardinality
		case queryFnCardinalityNorm:
			what = promql.Cardinality
			norm = true
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
			what = promql.Sum
			norm = true
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
			what = promql.Unique
			norm = true
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
			what = promql.Count
			norm = true
			deriv = true
		case queryFnDerivativeSumNorm:
			what = promql.Sum
			norm = true
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
			what = promql.Unique
			norm = true
			deriv = true
		default:
			continue
		}
		s := make([]string, 0, 4)
		for _, shift := range shifts {
			w := make([]string, 0, 2)
			w = append(w, what)
			if req.maxHost {
				w = append(w, promql.MaxHost)
			}
			s = append(s, fmt.Sprintf("__what__=%q", strings.Join(w, ",")))
			s = append(s, fmt.Sprintf("__by__=%q", strings.Join(req.by, ",")))
			for t, v := range req.filterIn {
				s = append(s, fmt.Sprintf("%s=~%q", t, promqlGetFilterValue(t, v)))
			}
			for t, v := range req.filterNotIn {
				s = append(s, fmt.Sprintf("%s!~%q", t, promqlGetFilterValue(t, v)))
			}
			q := fmt.Sprintf("%s{%s}", req.metricWithNamespace, strings.Join(s, ","))
			if shift != 0 {
				q = fmt.Sprintf("%s offset %ds", q, -shift/time.Second)
			}
			q = fmt.Sprintf("topk(%s,%s)", req.numResults, q)
			if norm {
				q = fmt.Sprintf("(%s/lod_step_sec())", q)
			}
			if deriv {
				q = fmt.Sprintf("idelta(%s)", q)
			}
			if cumul {
				q = fmt.Sprintf("prefix_sum(%s)", q)
			}
			q = fmt.Sprintf("label_replace(%s,%q,%q,%q,%q)", q, "__name__", name.String(), "__name__", ".*")
			res = append(res, q)
			s = s[:0]
		}
	}
	return strings.Join(res, " or ")
}

func promqlGetFilterValue(tagID string, s []string) string {
	if tagID != format.StringTopTagID {
		return strings.Join(s, "|")
	}
	s2 := make([]string, 0, len(s))
	for _, v := range s {
		s2 = append(s2, promqlUnspecifiedToEmpty(v))
	}
	return strings.Join(s2, "|")
}

func promqlEmptyToUnspecified(s string) string {
	if s == "^$" {
		return format.CodeTagValue(format.TagValueIDUnspecified)
	}
	return s
}

func promqlUnspecifiedToEmpty(s string) string {
	if s == format.CodeTagValue(format.TagValueIDUnspecified) {
		return "^$"
	}
	return s
}

func promqlVersionOrDefault(version string) string {
	if len(version) != 0 {
		return version
	}
	return Version2
}

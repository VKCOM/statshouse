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
	prom "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse/internal/api/dac"
	"github.com/vkcom/statshouse/internal/api/model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql"
	"github.com/vkcom/statshouse/internal/promql/parser"
	"github.com/vkcom/statshouse/internal/util"
)

var errQueryOutOfRange = fmt.Errorf("exceeded maximum resolution of %d points per timeseries", maxSlice)
var errAccessViolation = fmt.Errorf("metric access violation")

func (h *Handler) handlePromQuery(w http.ResponseWriter, r *http.Request, rangeQuery bool) {
	// parse access token
	ai, err := h.parseAccessToken(r, nil)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.User, nil)
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
	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()
	res, dispose, err := h.promEngine.Exec(model.WithAccessInfo(ctx, &ai), q)
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
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.User, nil)
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
	if v, err := prom.ParseDuration(s); err == nil {
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
	ai := model.GetAccessInfo(ctx)
	if ai == nil {
		return nil, nil, errAccessViolation
	}
	var (
		s1 []*format.MetricMetaValue // metrics
		s2 []string                  // metric match names
		fn = func(metric *format.MetricMetaValue) error {
			var name string
			switch {
			case matcher.Matches(metric.Name):
				name = metric.Name
			case matcher.Matches(metric.Name + "_bucket"):
				name = metric.Name + "_bucket"
			default:
				return nil
			}
			if !ai.CanViewMetric(*metric) {
				return model.HttpErr(http.StatusForbidden, fmt.Errorf("metric %q forbidden", metric.Name))
			}
			s1 = append(s1, metric)
			s2 = append(s2, name)
			return nil
		}
	)
	for _, m := range format.BuiltinMetrics {
		if err := fn(m); err != nil {
			return nil, nil, err
		}
	}
	for _, m := range h.metricsStorage.GetMetaMetricList(h.showInvisible) {
		if err := fn(m); err != nil {
			return nil, nil, err
		}
	}
	return s1, s2, nil
}

func promqlStepForward(start, step int64, loc *time.Location) int64 {
	if step == _1M {
		return time.Unix(start, 0).In(loc).AddDate(0, 1, 0).UTC().Unix()
	} else {
		return start + step
	}
}

func (h *Handler) promqlLODStart(start, step int64) int64 {
	if step == _1M {
		t := time.Unix(start, 0).In(h.location)
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, h.location).UTC().Unix()
	} else {
		return roundTime(start, step, h.utcOffset)
	}
}

func (h *Handler) promqlLODEnd(start, step, end int64, le bool) (int64, int) {
	if step <= 0 {
		// infinite loop guard
		panic(fmt.Errorf("negative step not allowed: %v", step))
	}
	n := 0
	for ; start < end; n++ {
		t := promqlStepForward(start, step, h.location)
		if le && end < t {
			break
		}
		start = t
	}
	return start, n
}

func (h *Handler) promqlLODs(t *promql.Timescale, version string, offset int64, metric *format.MetricMetaValue) []dac.LodInfo {
	start := t.Time[0]
	if offset != 0 {
		start = h.promqlLODStart(start-offset, t.LODs[0].Step)
	}
	res := make([]dac.LodInfo, 0, len(t.LODs))
	for _, lod := range t.LODs {
		end := start
		for i := 0; i < lod.Len; i++ {
			end = promqlStepForward(end, lod.Step, h.location)
		}
		res = append(res, dac.LodInfo{
			FromSec:    start,
			ToSec:      end,
			StepSec:    lod.Step,
			Table:      dac.LodTables[version][lod.Step],
			HasPreKey:  metric.PreKeyOnly || (metric.PreKeyFrom != 0 && int64(metric.PreKeyFrom) <= start),
			PreKeyOnly: metric.PreKeyOnly,
			Location:   h.location,
		})
		start = end
	}
	return res
}

func (h *Handler) GetTimescale(qry promql.Query, offsets map[*format.MetricMetaValue]int64) (promql.Timescale, error) {
	if qry.End <= qry.Start || qry.Step < 0 {
		return promql.Timescale{}, nil
	}
	// gather query info
	var (
		maxOffset    int
		maxMetricRes int64 // max metric resolution
		hasStringTop bool
		hasUnique    bool
	)
	for k, v := range offsets {
		if maxOffset < int(v) {
			maxOffset = int(v)
		}
		if maxMetricRes < int64(k.Resolution) {
			maxMetricRes = int64(k.Resolution)
		}
		if len(k.StringTopDescription) != 0 {
			hasStringTop = true
		}
	}
	var (
		levels  []dac.LodSwitch // depends on query and version
		version = promqlVersionOrDefault(qry.Options.Version)
	)
	// find appropriate LOD table
	switch {
	case qry.Step == _1M:
		switch {
		case version == Version1:
			switch {
			case hasUnique:
				levels = dac.LodLevelsV1MonthlyUnique
			case hasStringTop:
				levels = dac.LodLevelsV1MonthlyStringTop
			default:
				levels = dac.LodLevelsV1Monthly
			}
		default:
			levels = dac.LodLevelsV2Monthly
		}
	case version == Version1:
		switch {
		case hasUnique:
			levels = dac.LodLevelsV1Unique
		case hasStringTop:
			levels = dac.LodLevelsV1StringTop
		default:
			levels = dac.LodLevels[version]
		}
	default:
		levels = dac.LodLevels[version]
	}
	// generate LODs
	var minStep int64
	if qry.Options.Collapse {
		minStep = maxMetricRes
	} else {
		if 0 < qry.Step {
			minStep = qry.Step
		} else {
			minStep = maxMetricRes
		}
	}
	start := qry.Start - int64(maxOffset)
	end := qry.End - int64(maxOffset)
	res := promql.Timescale{Step: qry.Step}
	var resLen int
	var lod promql.LOD // last LOD
	for i := 0; i < len(levels) && start < end; i++ {
		edge := qry.Options.TimeNow - levels[i].RelSwitch
		if edge < start {
			continue
		}
		if end < edge || qry.Options.Collapse {
			edge = end
		}
		lod.Len = 0      // reset LOD length, keep last step
		var lodEnd int64 // next "start"
		for _, step := range levels[i].Levels {
			if 0 < lod.Step && lod.Step < step {
				continue // step can not grow
			}
			if lod.Len != 0 && step < minStep {
				break // take previously computed LOD
			}
			lodStart := start
			if len(res.LODs) == 0 {
				lodStart = h.promqlLODStart(start, step)
			}
			// calculate number of points up to the "edge"
			var lodLen, n int
			lodEnd, lodLen = h.promqlLODEnd(lodStart, step, edge, false)
			if !qry.Options.Collapse {
				// plus up to the query and to ensure current "step" does not exceed "maxPoints" limit
				_, m := h.promqlLODEnd(lodEnd, step, end, false)
				n = resLen + lodLen + m
				if maxPoints < n {
					// "maxPoints" limit exceed
					if lod.Step == 0 {
						// at largest "step" possible
						return promql.Timescale{}, errQueryOutOfRange
					}
					// use previous (larger) "step" to the end
					if len(res.LODs) == 0 {
						lodStart = h.promqlLODStart(start, lod.Step)
					}
					lodEnd, lod.Len = h.promqlLODEnd(lodStart, lod.Step, end, false)
					break
				}
			}
			lod = promql.LOD{Step: step, Len: lodLen}
			if qry.Options.ScreenWidth != 0 && int(qry.Options.ScreenWidth) < n {
				// use current "step" to the end
				lodEnd, lodLen = h.promqlLODEnd(lodEnd, step, end, false)
				lod.Len += lodLen
				break
			}
		}
		if lod.Step <= 0 || lod.Step > _1M || lod.Len <= 0 || !(qry.Options.Collapse || lod.Len <= maxPoints) {
			// should not happen
			return promql.Timescale{}, fmt.Errorf("LOD out of range: step=%d, len=%d", lod.Step, lod.Len)
		}
		start = lodEnd
		resLen += lod.Len
		if len(res.LODs) != 0 && res.LODs[len(res.LODs)-1].Step == lod.Step {
			res.LODs[len(res.LODs)-1].Len += lod.Len
		} else {
			res.LODs = append(res.LODs, lod)
		}
	}
	if len(res.LODs) == 0 {
		return promql.Timescale{}, nil
	}
	// verify offset is multiple of largest LOD step
	for _, v := range offsets {
		if v%res.LODs[0].Step != 0 {
			return promql.Timescale{}, fmt.Errorf("offset %d is not multiple of step %d", v, res.LODs[0].Step)
		}
	}
	// generate time
	p := &res.LODs[0]
	t := h.promqlLODStart(qry.Start, p.Step)
	if qry.Options.Collapse {
		if t < qry.Start && !qry.Options.Extend {
			t = promqlStepForward(t, p.Step, h.location)
		}
		res.Time = []int64{t, 0}
		res.Time[1], _ = h.promqlLODEnd(t, p.Step, qry.End, !qry.Options.Extend)
		if res.Time[0] == res.Time[1] {
			return promql.Timescale{}, nil
		}
		res.ViewEndX = 1
	} else {
		if t < qry.Start {
			if !qry.Options.Extend {
				res.StartX++
			}
			res.ViewStartX++
		} else if qry.Options.Extend {
			t = h.promqlLODStart(t-1, p.Step)
			p.Len++
			res.ViewStartX++
		}
		if res.StartX == 0 {
			t = h.promqlLODStart(t-1, p.Step)
			p.Len++
			res.StartX++
			res.ViewStartX++
		}
		resLen += 3 // account all possible extensions
		res.Time = make([]int64, 0, resLen)
		for i := range res.LODs {
			p = &res.LODs[i]
			res.Time = append(res.Time, t)
			for j := 1; j < p.Len; j++ {
				t = promqlStepForward(t, p.Step, h.location)
				res.Time = append(res.Time, t)
			}
		}
		if res.ViewStartX < len(res.Time) {
			res.ViewEndX = len(res.Time)
		} else {
			res.ViewEndX = res.ViewStartX
		}
		if qry.Options.Extend {
			t = promqlStepForward(t, p.Step, h.location)
			res.Time = append(res.Time, t)
			p.Len++
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
	return h.getRichTagValue(qry.Metric, promqlVersionOrDefault(qry.Version), tagID, qry.TagValueID)
}

func (h *Handler) GetTagValueID(qry promql.TagValueIDQuery) (int32, error) {
	res, err := h.getRichTagValueID(&qry.Metric.Tags[qry.TagIndex], qry.Version, qry.TagValue)
	if err != nil {
		var httpErr model.HttpError
		if errors.As(err, &httpErr) && httpErr.Code == http.StatusNotFound {
			err = promql.ErrNotFound
		}
	}
	return res, err
}

func (h *Handler) QuerySeries(ctx context.Context, qry *promql.SeriesQuery) (promql.Series, func(), error) {
	ai := model.GetAccessInfo(ctx)
	if ai == nil {
		return promql.Series{}, nil, errAccessViolation
	}
	if !ai.CanViewMetricName(qry.Metric.Name) {
		return promql.Series{}, func() {}, model.HttpErr(http.StatusForbidden, fmt.Errorf("metric %q forbidden", qry.Metric.Name))
	}
	if qry.Options.Collapse {
		for _, what := range qry.Whats {
			switch what {
			case promql.DigestCount, promql.DigestMin, promql.DigestMax, promql.DigestAvg,
				promql.DigestSum, promql.DigestP25, promql.DigestP50, promql.DigestP75,
				promql.DigestP90, promql.DigestP95, promql.DigestP99, promql.DigestP999,
				promql.DigestUnique:
				// pass
			default:
				return promql.Series{}, func() {}, fmt.Errorf("function %s is not supported", what.String())
			}
		}
	}
	var step int64
	if qry.Range != 0 {
		step = qry.Range
	} else {
		step = qry.Timescale.Step
	}
	qryRaw := make([]bool, len(qry.Whats))
	for i, what := range qry.Whats {
		switch what {
		case promql.DigestCountRaw, promql.DigestSumRaw, promql.DigestCardinalityRaw:
			qryRaw[i] = true
		default:
			qryRaw[i] = qry.Options.Collapse || step == 0 || step == _1M
		}
	}
	res := promql.Series{Meta: promql.SeriesMeta{Metric: qry.Metric}}
	if len(qry.Whats) == 1 {
		switch qry.Whats[0] {
		case promql.DigestCount, promql.DigestCountSec, promql.DigestCountRaw,
			promql.DigestStdVar, promql.DigestCardinality, promql.DigestCardinalitySec,
			promql.DigestCardinalityRaw, promql.DigestUnique, promql.DigestUniqueSec:
			// measure units does not apply to counters
		default:
			res.Meta.Units = qry.Metric.MetricType
		}
	}
	version := promqlVersionOrDefault(qry.Options.Version)
	var lods []dac.LodInfo
	if qry.Options.Collapse {
		// "point" query
		lod0 := qry.Timescale.LODs[0]
		start := qry.Timescale.Time[0]
		metric := qry.Metric
		lods = []dac.LodInfo{{
			FromSec:    qry.Timescale.Time[0] - qry.Offset,
			ToSec:      qry.Timescale.Time[1] - qry.Offset,
			StepSec:    lod0.Step,
			Table:      dac.LodTables[version][lod0.Step],
			HasPreKey:  metric.PreKeyOnly || (metric.PreKeyFrom != 0 && int64(metric.PreKeyFrom) <= start),
			PreKeyOnly: metric.PreKeyOnly,
			Location:   h.location,
		}}
	} else {
		lods = h.promqlLODs(&qry.Timescale, version, qry.Offset, qry.Metric)
	}
	tagX := make(map[dac.TsTags]int, len(qry.GroupBy))
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
	for _, args := range getHandlerArgs(qry, ai) {
		var tx int // time index
		fns, qs, pq := args.fns, args.qs, args.pq
		for _, lod := range lods {
			var err error
			var data [][]dac.TsSelectRow
			if qry.Options.Collapse { // "point" query
				if s, err := h.pointsCache.get(ctx, qs, &pq, lod, qry.Options.AvoidCache); err == nil {
					data = make([][]dac.TsSelectRow, 1)
					data[0] = make([]dac.TsSelectRow, len(s))
					for i := range s {
						data[0][i] = dac.TsSelectRow{TsTags: s[i].TsTags, TsValues: s[i].TsValues}
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
						x, err := lod.IndexOf(data[i][j].Time)
						if err != nil {
							return promql.Series{}, nil, err
						}
						k += x
					}
					x, ok := tagX[data[i][j].TsTags]
					if !ok {
						x = len(res.Data)
						tagX[data[i][j].TsTags] = x
						for range fns {
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
							})
						}
					}
					for y, fn := range fns {
						(*res.Data[x+y].Values)[k] = selectTSValue(fn, qry.MinMaxHost[0] || qry.MinMaxHost[1], qryRaw[y], int64(step), &data[i][j])
						for z, qryHost := range qry.MinMaxHost {
							if qryHost {
								res.Data[x+y].MinMaxHost[z][k] = data[i][j].Host[z]
							}
						}
					}
				}
			}
			tx += len(data)
		}
		tagWhat := len(qry.Whats) > 1 || qry.Options.TagWhat
		for i, what := range args.whats {
			for v, j := range tagX {
				for _, groupBy := range qry.GroupBy {
					switch groupBy {
					case format.StringTopTagID, qry.Metric.StringTopName:
						res.AddTagAt(i+j, &promql.SeriesTag{
							Metric: qry.Metric,
							Index:  format.StringTopTagIndex + promql.SeriesTagIndexOffset,
							ID:     format.StringTopTagID,
							Name:   qry.Metric.StringTopName,
							SValue: emptyToUnspecified(v.TagStr.String()),
						})
					case format.ShardTagID:
						res.AddTagAt(i+j, &promql.SeriesTag{
							Metric: qry.Metric,
							ID:     promql.LabelShard,
							Value:  int32(v.ShardNum),
						})
					default:
						if m, ok := qry.Metric.Name2Tag[groupBy]; ok && m.Index < len(v.Tag) {
							res.AddTagAt(i+j, &promql.SeriesTag{
								Metric: qry.Metric,
								Index:  m.Index + promql.SeriesTagIndexOffset,
								ID:     format.TagID(m.Index),
								Name:   m.Name,
								Value:  v.Tag[m.Index],
							})
						}
					}
				}
				if tagWhat {
					res.AddTagAt(i+j, &promql.SeriesTag{
						ID:    promql.LabelWhat,
						Value: int32(what),
					})
				}
			}
		}
		tagX = make(map[dac.TsTags]int, len(tagX))
	}
	res.Meta.Total = len(res.Data)
	succeeded = true // prevents deffered "cleanup"
	return res, cleanup, nil
}

func (h *Handler) QueryTagValueIDs(ctx context.Context, qry promql.TagValuesQuery) ([]int32, error) {
	ai := model.GetAccessInfo(ctx)
	if ai == nil {
		return nil, errAccessViolation
	}
	var (
		version = promqlVersionOrDefault(qry.Options.Version)
		pq      = &dac.PreparedTagValuesQuery{
			Version:     version,
			MetricID:    qry.Metric.MetricID,
			PreKeyTagID: qry.Metric.PreKeyTagID,
			TagID:       format.TagID(qry.TagIndex),
			NumResults:  math.MaxInt - 1,
		}
		tags = make(map[int32]bool)
	)
	for _, lod := range h.promqlLODs(&qry.Timescale, version, qry.Offset, qry.Metric) {
		body, args, err := dac.TagValuesQuery(pq, lod)
		if err != nil {
			return nil, err
		}
		cols := newTagValuesSelectCols(args)
		isFast := lod.FromSec+fastQueryTimeInterval >= lod.ToSec
		err = h.dac.DoSelect(ctx, util.QueryMetaInto{
			IsFast:  isFast,
			IsLight: true,
			User:    ai.User,
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
	ai := model.GetAccessInfo(ctx)
	if ai == nil {
		return nil, errAccessViolation
	}
	var (
		version = promqlVersionOrDefault(qry.Options.Version)
		pq      = &dac.PreparedTagValuesQuery{
			Version:     version,
			MetricID:    qry.Metric.MetricID,
			PreKeyTagID: qry.Metric.PreKeyTagID,
			TagID:       format.StringTopTagID,
			NumResults:  math.MaxInt - 1,
		}
		tags = make(map[string]bool)
	)
	for _, lod := range h.promqlLODs(&qry.Timescale, version, qry.Offset, qry.Metric) {
		body, args, err := dac.TagValuesQuery(pq, lod)
		if err != nil {
			return nil, err
		}
		cols := newTagValuesSelectCols(args)
		isFast := lod.FromSec+fastQueryTimeInterval >= lod.ToSec
		err = h.dac.DoSelect(ctx, util.QueryMetaInto{
			IsFast:  isFast,
			IsLight: true,
			User:    ai.User,
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
	qs    string // cache key
	pq    dac.PreparedPointsQuery
	fns   []model.QueryFn
	whats []promql.DigestWhat
}

func getHandlerArgs(qry *promql.SeriesQuery, ai *model.AccessInfo) map[model.QueryFnKind]handlerArgs {
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
	res := make(map[model.QueryFnKind]handlerArgs)
	for _, v := range qry.Whats {
		var fn model.QueryFn
		switch v {
		case promql.DigestCount, promql.DigestCountRaw:
			fn = model.QueryFnCount
		case promql.DigestCountSec:
			fn = model.QueryFnCountNorm
		case promql.DigestMin:
			fn = model.QueryFnMin
		case promql.DigestMax:
			fn = model.QueryFnMax
		case promql.DigestSum, promql.DigestSumRaw:
			fn = model.QueryFnSum
		case promql.DigestSumSec:
			fn = model.QueryFnSumNorm
		case promql.DigestAvg:
			fn = model.QueryFnAvg
		case promql.DigestStdDev:
			fn = model.QueryFnStddev
		case promql.DigestStdVar:
			fn = model.QueryFnStdvar
		case promql.DigestP0_1:
			fn = model.QueryFnP0_1
		case promql.DigestP1:
			fn = model.QueryFnP1
		case promql.DigestP5:
			fn = model.QueryFnP5
		case promql.DigestP10:
			fn = model.QueryFnP10
		case promql.DigestP25:
			fn = model.QueryFnP25
		case promql.DigestP50:
			fn = model.QueryFnP50
		case promql.DigestP75:
			fn = model.QueryFnP75
		case promql.DigestP90:
			fn = model.QueryFnP90
		case promql.DigestP95:
			fn = model.QueryFnP95
		case promql.DigestP99:
			fn = model.QueryFnP99
		case promql.DigestP999:
			fn = model.QueryFnP999
		case promql.DigestCardinality, promql.DigestCardinalityRaw:
			fn = model.QueryFnCardinality
		case promql.DigestCardinalitySec:
			fn = model.QueryFnCardinalityNorm
		case promql.DigestUnique:
			fn = model.QueryFnUnique
		case promql.DigestUniqueSec:
			fn = model.QueryFnUniqueNorm
		default:
			panic(fmt.Errorf("unrecognized what: %v", qry.Whats))
		}
		kind := model.QueryFnToQueryFnKind(fn, qry.MinMaxHost[0] || qry.MinMaxHost[1])
		args := res[kind]
		args.fns = append(args.fns, fn)
		args.whats = append(args.whats, v)
		res[kind] = args
	}
	// cache key & query
	for kind, args := range res {
		args.qs = normalizedQueryString(qry.Metric.Name, kind, groupBy, filterIn, filterOut, false)
		args.pq = dac.PreparedPointsQuery{
			User:        ai.User,
			Version:     promqlVersionOrDefault(qry.Options.Version),
			MetricID:    qry.Metric.MetricID,
			PreKeyTagID: qry.Metric.PreKeyTagID,
			Kind:        kind,
			By:          qry.GroupBy,
			FilterIn:    filterInM,
			FilterNotIn: filterOutM,
		}
		res[kind] = args
	}
	return res
}

func (h *Handler) Alloc(n int) *[]float64 {
	if n > maxSlice {
		panic(model.HttpErr(http.StatusBadRequest, errQueryOutOfRange))
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
	var whats [3][]string
	for _, fn := range req.what {
		name, ok := model.ValidQueryFn(fn)
		if !ok {
			return "", fmt.Errorf("invalid %q value: %q", ParamQueryWhat, fn)
		}
		var what string
		x := nat
		switch name {
		case model.QueryFnCount:
			what = promql.Count
		case model.QueryFnCountNorm:
			what = promql.CountSec
		case model.QueryFnCumulCount:
			what = promql.CountRaw
			x = cum
		case model.QueryFnCardinality:
			what = promql.Cardinality
		case model.QueryFnCardinalityNorm:
			what = promql.CardinalitySec
		case model.QueryFnCumulCardinality:
			what = promql.CardinalityRaw
			x = cum
		case model.QueryFnMin:
			what = promql.Min
		case model.QueryFnMax:
			what = promql.Max
		case model.QueryFnAvg:
			what = promql.Avg
		case model.QueryFnCumulAvg:
			what = promql.Avg
			x = cum
		case model.QueryFnSum:
			what = promql.Sum
		case model.QueryFnSumNorm:
			what = promql.SumSec
		case model.QueryFnCumulSum:
			what = promql.SumRaw
			x = cum
		case model.QueryFnStddev:
			what = promql.StdDev
		case model.QueryFnP0_1:
			what = promql.P0_1
		case model.QueryFnP1:
			what = promql.P1
		case model.QueryFnP5:
			what = promql.P5
		case model.QueryFnP10:
			what = promql.P10
		case model.QueryFnP25:
			what = promql.P25
		case model.QueryFnP50:
			what = promql.P50
		case model.QueryFnP75:
			what = promql.P75
		case model.QueryFnP90:
			what = promql.P90
		case model.QueryFnP95:
			what = promql.P95
		case model.QueryFnP99:
			what = promql.P99
		case model.QueryFnP999:
			what = promql.P999
		case model.QueryFnUnique:
			what = promql.Unique
		case model.QueryFnUniqueNorm:
			what = promql.UniqueSec
		case model.QueryFnMaxHost:
			req.maxHost = true
		case model.QueryFnMaxCountHost:
			what = promql.Max
			req.maxHost = true
		case model.QueryFnDerivativeCount:
			what = promql.Count
			x = der
		case model.QueryFnDerivativeSum:
			what = promql.Sum
			x = der
		case model.QueryFnDerivativeAvg:
			what = promql.Avg
			x = der
		case model.QueryFnDerivativeCountNorm:
			what = promql.CountSec
			x = der
		case model.QueryFnDerivativeSumNorm:
			what = promql.SumSec
			x = der
		case model.QueryFnDerivativeMin:
			what = promql.Min
			x = der
		case model.QueryFnDerivativeMax:
			what = promql.Max
			x = der
		case model.QueryFnDerivativeUnique:
			what = promql.Unique
			x = der
		case model.QueryFnDerivativeUniqueNorm:
			what = promql.UniqueSec
			x = der
		default:
			continue
		}
		whats[x] = append(whats[x], what)
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
	for i, v := range whats {
		if len(v) == 0 {
			continue
		}
		if req.maxHost {
			v = append(v, promql.MaxHost)
		}
		s := fmt.Sprintf("@what=%q", strings.Join(v, ","))
		s = strings.Join(append([]string{s}, filterGroupBy...), ",")
		s = fmt.Sprintf("%s{%s}", req.metricWithNamespace, s)
		switch i {
		case cum:
			s = fmt.Sprintf("prefix_sum(%s)", s)
		case der:
			s = fmt.Sprintf("idelta(%s)", s)
		}
		q = append(q, s)
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

func promqlVersionOrDefault(version string) string {
	if len(version) != 0 {
		return version
	}
	return Version2
}

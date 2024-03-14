// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouseApi"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"github.com/vkcom/statshouse/internal/vkgo/vkuth"
	"pgregory.net/rand"
)

const (
	chunkMaxSize   = 1_000_000 // little less than 1 MiB
	bigResponseTTL = time.Second * 30

	rpcErrorCodeAuthFailed          = 5000
	rpcErrorCodeNotFound            = 5001
	rpcErrorCodeForbidden           = 5002
	rpcErrorCodeBadChunkID          = 5003
	rpcErrorCodeUnknownMetric       = 5004
	rpcErrorCodeQueryParsingFailed  = 5005
	rpcErrorCodeQueryHandlingFailed = 5006
	rpcErrorCodeChunkStorageFailed  = 5007
)

var (
	fnToString = map[tlstatshouseApi.Function]string{
		tlstatshouseApi.FnCount():                ParamQueryFnCount,
		tlstatshouseApi.FnCountNorm():            ParamQueryFnCountNorm,
		tlstatshouseApi.FnCumulCount():           ParamQueryFnCumulCount,
		tlstatshouseApi.FnMin():                  ParamQueryFnMin,
		tlstatshouseApi.FnMax():                  ParamQueryFnMax,
		tlstatshouseApi.FnAvg():                  ParamQueryFnAvg,
		tlstatshouseApi.FnCumulAvg():             ParamQueryFnCumulAvg,
		tlstatshouseApi.FnSum():                  ParamQueryFnSum,
		tlstatshouseApi.FnSumNorm():              ParamQueryFnSumNorm,
		tlstatshouseApi.FnCumulSum():             ParamQueryFnCumulSum,
		tlstatshouseApi.FnStddev():               ParamQueryFnStddev,
		tlstatshouseApi.FnP01():                  ParamQueryFnP0_1,
		tlstatshouseApi.FnP1():                   ParamQueryFnP1,
		tlstatshouseApi.FnP5():                   ParamQueryFnP5,
		tlstatshouseApi.FnP10():                  ParamQueryFnP10,
		tlstatshouseApi.FnP25():                  ParamQueryFnP25,
		tlstatshouseApi.FnP50():                  ParamQueryFnP50,
		tlstatshouseApi.FnP75():                  ParamQueryFnP75,
		tlstatshouseApi.FnP90():                  ParamQueryFnP90,
		tlstatshouseApi.FnP95():                  ParamQueryFnP95,
		tlstatshouseApi.FnP99():                  ParamQueryFnP99,
		tlstatshouseApi.FnP999():                 ParamQueryFnP999,
		tlstatshouseApi.FnUnique():               ParamQueryFnUnique,
		tlstatshouseApi.FnUniqueNorm():           ParamQueryFnUniqueNorm,
		tlstatshouseApi.FnMaxHost():              ParamQueryFnMaxHost,
		tlstatshouseApi.FnMaxCountHost():         ParamQueryFnMaxCountHost,
		tlstatshouseApi.FnDerivativeCount():      ParamQueryFnDerivativeCount,
		tlstatshouseApi.FnDerivativeCountNorm():  ParamQueryFnDerivativeCountNorm,
		tlstatshouseApi.FnDerivativeSum():        ParamQueryFnDerivativeSum,
		tlstatshouseApi.FnDerivativeSumNorm():    ParamQueryFnDerivativeSumNorm,
		tlstatshouseApi.FnDerivativeMin():        ParamQueryFnDerivativeMin,
		tlstatshouseApi.FnDerivativeMax():        ParamQueryFnDerivativeMax,
		tlstatshouseApi.FnDerivativeAvg():        ParamQueryFnDerivativeAvg,
		tlstatshouseApi.FnDerivativeUnique():     ParamQueryFnDerivativeUnique,
		tlstatshouseApi.FnDerivativeUniqueNorm(): ParamQueryFnDerivativeUniqueNorm,
	}
	whatToFn = map[queryFn]tlstatshouseApi.Function{
		queryFnCount:                tlstatshouseApi.FnCount(),
		queryFnCountNorm:            tlstatshouseApi.FnCountNorm(),
		queryFnCumulCount:           tlstatshouseApi.FnCumulCount(),
		queryFnMin:                  tlstatshouseApi.FnMin(),
		queryFnMax:                  tlstatshouseApi.FnMax(),
		queryFnAvg:                  tlstatshouseApi.FnAvg(),
		queryFnCumulAvg:             tlstatshouseApi.FnCumulAvg(),
		queryFnSum:                  tlstatshouseApi.FnSum(),
		queryFnSumNorm:              tlstatshouseApi.FnSumNorm(),
		queryFnStddev:               tlstatshouseApi.FnStddev(),
		queryFnP0_1:                 tlstatshouseApi.FnP01(),
		queryFnP1:                   tlstatshouseApi.FnP1(),
		queryFnP5:                   tlstatshouseApi.FnP5(),
		queryFnP10:                  tlstatshouseApi.FnP10(),
		queryFnP25:                  tlstatshouseApi.FnP25(),
		queryFnP50:                  tlstatshouseApi.FnP50(),
		queryFnP75:                  tlstatshouseApi.FnP75(),
		queryFnP90:                  tlstatshouseApi.FnP90(),
		queryFnP95:                  tlstatshouseApi.FnP95(),
		queryFnP99:                  tlstatshouseApi.FnP99(),
		queryFnP999:                 tlstatshouseApi.FnP999(),
		queryFnUnique:               tlstatshouseApi.FnUnique(),
		queryFnUniqueNorm:           tlstatshouseApi.FnUniqueNorm(),
		queryFnMaxHost:              tlstatshouseApi.FnMaxHost(),
		queryFnMaxCountHost:         tlstatshouseApi.FnMaxCountHost(),
		queryFnCumulSum:             tlstatshouseApi.FnCumulSum(),
		queryFnDerivativeCount:      tlstatshouseApi.FnDerivativeCount(),
		queryFnDerivativeCountNorm:  tlstatshouseApi.FnDerivativeCountNorm(),
		queryFnDerivativeSum:        tlstatshouseApi.FnDerivativeSum(),
		queryFnDerivativeSumNorm:    tlstatshouseApi.FnDerivativeSumNorm(),
		queryFnDerivativeMin:        tlstatshouseApi.FnDerivativeMin(),
		queryFnDerivativeMax:        tlstatshouseApi.FnDerivativeMax(),
		queryFnDerivativeAvg:        tlstatshouseApi.FnDerivativeAvg(),
		queryFnDerivativeUnique:     tlstatshouseApi.FnDerivativeUnique(),
		queryFnDerivativeUniqueNorm: tlstatshouseApi.FnDerivativeUniqueNorm(),
	}
)

type RPCHandler struct {
	ah                *Handler
	brs               *BigResponseStorage
	jwtHelper         *vkuth.JWTHelper
	protectedPrefixes []string
	localMode         bool
	insecureMode      bool
}

func NewRpcHandler(
	ah *Handler,
	brs *BigResponseStorage,
	jwtHelper *vkuth.JWTHelper,
	opt HandlerOptions,
) *RPCHandler {
	return &RPCHandler{
		ah:                ah,
		brs:               brs,
		jwtHelper:         jwtHelper,
		protectedPrefixes: opt.protectedMetricPrefixes,
		localMode:         opt.LocalMode,
		insecureMode:      opt.insecureMode,
	}
}

func (h *RPCHandler) RawGetQueryPoint(ctx context.Context, hctx *rpc.HandlerContext) error {
	arg, qry, err := h.getPointQuery(hctx)
	if err != nil {
		return err
	}
	var sr seriesResponse
	defer func() {
		log.Printf("POINT QUERY err=%v, res=%v", err, sr)
		qry.stat.reportServiceTime(0, err)
	}()
	var req seriesRequest
	req, err = qry.toSeriesRequest(h)
	if err != nil {
		return err
	}
	sr, cancel, err := h.ah.handleSeriesRequest(ctx, req, seriesRequestOptions{collapse: true, trace: true})
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeQueryHandlingFailed, Description: fmt.Sprintf("can't handle query: %v", err)}
		return err
	}
	defer cancel()
	res := tlstatshouseApi.GetQueryPointResponse{}
	for i, d := range sr.Series.Data {
		if len(sr.Time) < 2 {
			continue
		}
		meta := tlstatshouseApi.PointMeta{
			From: sr.Time[0],
			To:   sr.Time[1],
		}
		var what queryFn
		var tags map[string]SeriesMetaTag
		what, meta.TimeShift, tags = sr.queryFnShiftAndTagsAt(i)
		meta.SetWhat(whatToFn[what])
		meta.Tags = make(map[string]string, len(tags))
		for k, v := range tags {
			meta.Tags[k] = v.Value
		}
		res.Meta = append(res.Meta, meta)
		res.Data = append(res.Data, (*d.Values)[0])
	}
	if hctx.Response, err = arg.WriteResult(hctx.Response, res); err != nil {
		return fmt.Errorf("failed to serialize tlstatshouseApi.GetQueryPointResponse response: %w", err)
	}
	return nil
}

func (h *RPCHandler) RawGetQuery(ctx context.Context, hctx *rpc.HandlerContext) error {
	arg, qry, err := h.getSeriesQuery(hctx)
	if err != nil {
		return err
	}
	defer func() {
		qry.stat.reportServiceTime(0, err)
	}()
	req, err := qry.toSeriesRequest(h)
	if err != nil {
		return err
	}
	srs, cancel, err := h.ah.handleSeriesRequestS(ctx, req, qry.stat, make([]seriesResponse, 1))
	if err != nil {
		return err
	}
	defer cancel()
	sr := h.ah.buildSeriesResponse(srs...)
	res := tlstatshouseApi.GetQueryResponse{
		TotalTimePoints: int32(len(sr.Series.Time)),
		SeriesMeta:      make([]tlstatshouseApi.SeriesMeta, 0, len(sr.Series.SeriesMeta)),
	}
	for _, meta := range sr.Series.SeriesMeta {
		m := tlstatshouseApi.SeriesMeta{
			TimeShift: meta.TimeShift,
			Tags:      map[string]string{},
			Name:      meta.Name,
			Color:     meta.Color,
			MaxHosts:  meta.MaxHosts,
			Total:     int32(meta.Total),
		}
		for k, v := range meta.Tags {
			m.Tags[k] = v.Value
		}
		m.SetWhat(whatToFn[meta.What])
		res.SeriesMeta = append(res.SeriesMeta, m)
	}
	if arg.Query.IsSetExcessPointsFlag() {
		res.SetExcessPointLeft(sr.ExcessPointLeft)
		res.SetExcessPointRight(sr.ExcessPointRight)
	}
	columnSize, totalSize, metaSize := estimateResponseSize(sr)
	if totalSize <= chunkMaxSize {
		res.Series.Time = sr.Series.Time
		res.Series.SeriesData = make([][]float64, 0, len(sr.Series.SeriesData))
		for _, data := range sr.Series.SeriesData {
			res.Series.SeriesData = append(res.Series.SeriesData, *data)
		}
	} else {
		chunks := chunkResponse(sr, columnSize, totalSize, metaSize)
		res.Series = chunks[0] // return first chunk immediately
		rid := int64(rand.Uint64())
		if err = h.brs.Set(ctx, rid, req.ai.user, chunks[1:], bigResponseTTL); err != nil {
			return rpc.Error{Code: rpcErrorCodeChunkStorageFailed, Description: fmt.Sprintf("can't save chunks: %v", err)}
		}
		res.ResponseId = rid
		res.ChunkIds = make([]int32, 0, len(chunks)-1)
		for i := 1; i < len(chunks); i++ {
			res.ChunkIds = append(res.ChunkIds, int32(i-1))
		}
	}
	if hctx.Response, err = arg.WriteResult(hctx.Response, res); err != nil {
		return fmt.Errorf("failed to serialize statshouseApi.getQuery response: %w", err)
	}
	return nil
}

func (h *RPCHandler) GetChunk(_ context.Context, args tlstatshouseApi.GetChunk) (tlstatshouseApi.GetChunkResponse, error) {
	var err error
	es := newEndpointStatRPC(endpointChunk, args.TLName())
	defer func() {
		es.reportServiceTime(0, err)
	}()

	ai, err := h.parseAccessToken(args.AccessToken)
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeAuthFailed, Description: fmt.Sprintf("can't parse access token: %v", err)}
		return tlstatshouseApi.GetChunkResponse{}, err
	}
	es.setAccessInfo(ai)

	br, ok := h.brs.Get(args.ResponseId)
	if !ok {
		err = rpc.Error{Code: rpcErrorCodeNotFound, Description: fmt.Sprintf("can't find response %q", args.ResponseId)}
		return tlstatshouseApi.GetChunkResponse{}, err
	}
	if br.owner != ai.user {
		err = rpc.Error{Code: rpcErrorCodeForbidden, Description: fmt.Sprintf("response %d belongs to another user", args.ResponseId)}
		return tlstatshouseApi.GetChunkResponse{}, err
	}
	if int(args.ChunkId) > len(br.chunks)-1 {
		err = rpc.Error{Code: rpcErrorCodeBadChunkID, Description: fmt.Sprintf("got id %q, there are only %d chunks", args.ResponseId, len(br.chunks))}
		return tlstatshouseApi.GetChunkResponse{}, err
	}

	res := tlstatshouseApi.GetChunkResponse{
		Series: br.chunks[int(args.ChunkId)],
		Index:  args.ChunkId,
	}
	return res, nil
}

func (h *RPCHandler) ReleaseChunks(_ context.Context, args tlstatshouseApi.ReleaseChunks) (tlstatshouseApi.ReleaseChunksResponse, error) {
	es := newEndpointStatRPC(endpointChunk, args.TLName())
	ai, err := h.parseAccessToken(args.AccessToken)
	defer func() {
		es.reportServiceTime(0, err)
	}()
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeAuthFailed, Description: fmt.Sprintf("can't parse access token: %v", err)}
		return tlstatshouseApi.ReleaseChunksResponse{}, err
	}
	es.setAccessInfo(ai)
	br, ok := h.brs.Get(args.ResponseId)
	if !ok {
		err = rpc.Error{Code: rpcErrorCodeNotFound, Description: fmt.Sprintf("can't find response %q", args.ResponseId)}
		return tlstatshouseApi.ReleaseChunksResponse{}, err
	}
	if br.owner != ai.user {
		err = rpc.Error{Code: rpcErrorCodeForbidden, Description: fmt.Sprintf("response %q belongs to another user", args.ResponseId)}
		return tlstatshouseApi.ReleaseChunksResponse{}, err
	}
	res := tlstatshouseApi.ReleaseChunksResponse{
		ReleasedChunkCount: int32(h.brs.Release(args.ResponseId)),
	}
	return res, nil
}

func (h *RPCHandler) parseAccessToken(token string) (accessInfo, error) {
	return parseAccessToken(h.jwtHelper, token, h.protectedPrefixes, h.localMode, h.insecureMode)
}

type seriesRequestRPC struct {
	stat        *endpointStat
	accessToken string
	filter      []tlstatshouseApi.Filter
	function    tlstatshouseApi.Function
	groupBy     []string
	interval    string
	metricName  string
	promQL      string
	timeFrom    int64
	timeShift   []int64
	timeTo      int64
	topN        int32
	version     int32
	what        []tlstatshouseApi.Function
	widthAgg    string
	whatFlagSet bool
}

func (h *RPCHandler) getSeriesQuery(hctx *rpc.HandlerContext) (tlstatshouseApi.GetQuery, seriesRequestRPC, error) {
	var q tlstatshouseApi.GetQuery
	_, err := q.Read(hctx.Request)
	if err != nil {
		err = fmt.Errorf("failed to deserialize statshouseApi.getQuery request: %w", err)
		return tlstatshouseApi.GetQuery{}, seriesRequestRPC{}, err
	}
	r := seriesRequestRPC{
		accessToken: q.AccessToken,
		stat:        newEndpointStatRPC(EndpointQuery, q.TLName()),
		filter:      q.Query.Filter,
		function:    q.Query.Function,
		groupBy:     q.Query.GroupBy,
		interval:    q.Query.Interval,
		metricName:  q.Query.MetricName,
		promQL:      q.Query.Promql,
		timeFrom:    q.Query.TimeFrom,
		timeShift:   q.Query.TimeShift,
		timeTo:      q.Query.TimeTo,
		topN:        q.Query.TopN,
		version:     q.Query.Version,
		what:        q.Query.What,
		widthAgg:    q.Query.WidthAgg,
		whatFlagSet: q.Query.IsSetWhat(),
	}
	return q, r, nil
}

func (h *RPCHandler) getPointQuery(hctx *rpc.HandlerContext) (tlstatshouseApi.GetQueryPoint, seriesRequestRPC, error) {
	var q tlstatshouseApi.GetQueryPoint
	_, err := q.Read(hctx.Request)
	if err != nil {
		err = fmt.Errorf("failed to deserialize statshouseApi.GetQueryPoint request: %w", err)
		return tlstatshouseApi.GetQueryPoint{}, seriesRequestRPC{}, err
	}
	r := seriesRequestRPC{
		accessToken: q.AccessToken,
		stat:        newEndpointStatRPC(EndpointQuery, q.TLName()),
		filter:      q.Query.Filter,
		function:    q.Query.Function,
		groupBy:     q.Query.GroupBy,
		metricName:  q.Query.MetricName,
		timeFrom:    q.Query.TimeFrom,
		timeShift:   q.Query.TimeShift,
		timeTo:      q.Query.TimeTo,
		topN:        q.Query.TopN,
		version:     q.Query.Version,
		what:        q.Query.What,
		whatFlagSet: q.Query.IsSetWhat(),
	}
	return q, r, nil
}

func (q *seriesRequestRPC) toSeriesRequest(h *RPCHandler) (seriesRequest, error) {
	req := seriesRequest{
		version:             strconv.FormatInt(int64(q.version), 10),
		numResults:          int(q.topN),
		metricWithNamespace: q.metricName,
		from:                time.Unix(q.timeFrom, 0),
		to:                  time.Unix(q.timeTo, 0),
		by:                  q.groupBy,
		promQL:              q.promQL,
	}
	var err error
	req.ai, err = h.parseAccessToken(q.accessToken)
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeAuthFailed, Description: fmt.Sprintf("can't parse access token: %v", err)}
		return seriesRequest{}, err
	}
	q.stat.setAccessInfo(req.ai)
	req.metric, err = h.ah.getMetricMeta(req.ai, q.metricName)
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeUnknownMetric, Description: fmt.Sprintf("can't get metric's meta: %v", err)}
		return seriesRequest{}, err
	}
	q.stat.setMetricMeta(req.metric)
	if req.metric != nil {
		req.filterIn, req.filterNotIn, err = parseFilterValues(q.filter, req.metric)
		if err != nil {
			err = fmt.Errorf("can't parse filter: %v", err)
			return seriesRequest{}, err
		}
	} else if len(q.promQL) == 0 {
		err = fmt.Errorf("neither metric name nor PromQL expression specified")
		return seriesRequest{}, err
	}
	if len(q.interval) != 0 || len(q.widthAgg) != 0 {
		req.width, req.widthKind, err = parseWidth(q.interval, q.widthAgg)
		if err != nil {
			err = fmt.Errorf("can't parse interval: %v", err)
			return seriesRequest{}, err
		}
	}
	req.shifts = make([]time.Duration, 0, len(q.timeShift))
	for _, ts := range q.timeShift {
		if req.width == _1M && ts%_1M != 0 {
			err = fmt.Errorf("time shift %d can't be used with month interval", ts)
			return seriesRequest{}, err
		}
		req.shifts = append(req.shifts, time.Duration(ts)*time.Second)
	}
	if q.whatFlagSet {
		req.what = make([]string, 0, len(q.what))
		for _, fn := range q.what {
			req.what = append(req.what, fnToString[fn])
		}
	} else {
		req.what = []string{fnToString[q.function]}
	}
	return req, nil
}

func parseFilterValues(filter []tlstatshouseApi.Filter, meta *format.MetricMetaValue) (map[string][]string, map[string][]string, error) {
	filterIn := map[string][]string{}
	filterNotIn := map[string][]string{}
	for _, f := range filter {
		tid, err := format.APICompatNormalizeTagID(f.Key)
		if err != nil {
			return nil, nil, err
		}
		for _, fv := range f.Values {
			tagValue := fv.Value
			switch fv.Flag {
			case tlstatshouseApi.FlagRaw():
				tagValue = format.AddRawValuePrefix(tagValue)
			case tlstatshouseApi.FlagAuto():
				tag, ok := meta.Name2Tag[tid]
				if !ok {
					return nil, nil, fmt.Errorf("tag with name %q not found", f.Key)
				}
				if !tag.Raw {
					break
				}
				if format.HasRawValuePrefix(fv.Value) {
					break
				}
				if _, ok := tag.Comment2Value[fv.Value]; ok {
					break
				}
				tagValue = format.AddRawValuePrefix(tagValue)
			}
			if fv.In {
				filterIn[tid] = append(filterIn[tid], tagValue)
			} else {
				filterNotIn[tid] = append(filterNotIn[tid], tagValue)
			}
		}
	}

	return filterIn, filterNotIn, nil
}

func chunkResponse(res *SeriesResponse, columnSize int, totalSize int, metaSize int) []tlstatshouseApi.Series {
	firstColumnPerChunk := (chunkMaxSize - metaSize) / columnSize
	columnsPerChunk := chunkMaxSize / columnSize
	chunksCount := 1 + int(math.Ceil(float64(totalSize-metaSize-firstColumnPerChunk*columnSize)/float64(columnsPerChunk*columnSize)))

	chunks := make([]tlstatshouseApi.Series, 0, chunksCount)

	firstChunk := tlstatshouseApi.Series{Time: res.Series.Time[0:firstColumnPerChunk]}
	firstChunk.SeriesData = make([][]float64, 0, len(res.Series.SeriesData))
	for _, data := range res.Series.SeriesData {
		firstChunk.SeriesData = append(firstChunk.SeriesData, (*data)[0:firstColumnPerChunk])
	}
	chunks = append(chunks, firstChunk)

	for i := 0; i < chunksCount-1; i++ {
		low := i*columnsPerChunk + firstColumnPerChunk
		high := (i+1)*columnsPerChunk + firstColumnPerChunk
		if high > len(res.Series.Time) {
			high = len(res.Series.Time)
		}

		chunk := tlstatshouseApi.Series{Time: res.Series.Time[low:high]}
		chunk.SeriesData = make([][]float64, 0, len(res.Series.SeriesData))
		for _, data := range res.Series.SeriesData {
			chunk.SeriesData = append(chunk.SeriesData, (*data)[low:high])
		}

		chunks = append(chunks, chunk)
	}

	return chunks
}

func estimateResponseSize(data *SeriesResponse) (int, int, int) {
	if data == nil {
		return 0, 0, 0
	}

	columnSize := 4 + 8 + len(data.Series.SeriesData)*8 // fields_mask + timestamp + (point*len(timestamps))

	tagsCount := 0
	if len(data.Series.SeriesMeta) > 0 {
		tagsCount = len(data.Series.SeriesMeta[0].Tags)
	}
	metaSize := (4 + 8 + 8 + 20*tagsCount) * len(data.Series.SeriesMeta) // (fields_mask + shift + what + avg_size(tag)*len(tags))*len(metas)

	return columnSize, columnSize*len(data.Series.Time) + metaSize, metaSize
}

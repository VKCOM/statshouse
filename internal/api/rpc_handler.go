// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouseApi"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"github.com/vkcom/statshouse/internal/vkgo/vkuth"
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
	protectedPrefixes []string,
	localMode bool,
	insecureMode bool,
) *RPCHandler {
	return &RPCHandler{
		ah:                ah,
		brs:               brs,
		jwtHelper:         jwtHelper,
		protectedPrefixes: protectedPrefixes,
		localMode:         localMode,
		insecureMode:      insecureMode,
	}
}

func (h *RPCHandler) prepareQuery(metricName, accessToken string) (*format.MetricMetaValue, accessInfo, error) {
	ai, err := h.parseAccessToken(accessToken)
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeAuthFailed, Description: fmt.Sprintf("can't parse access token: %v", err)}
		return nil, ai, err
	}

	metricMeta, err := h.ah.getMetricMeta(ai, metricName)
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeUnknownMetric, Description: fmt.Sprintf("can't get metric's meta: %v", err)}
		return nil, ai, err
	}
	return metricMeta, ai, nil
}

func (h *RPCHandler) GetQueryPoint(ctx context.Context, args tlstatshouseApi.GetQueryPoint) (tlstatshouseApi.GetQueryPointResponse, error) {
	var (
		ai         accessInfo
		response   tlstatshouseApi.GetQueryPointResponse
		metricMeta *format.MetricMetaValue
		err        error
		ms         = newRPCMethodStat(EndpointPoint, args.TLName())
	)

	defer func() {
		ms.serviceTime(ai, metricMeta, err)
		ms.serviceTimeDeprecated(ai, err)
	}()

	metricMeta, ai, err = h.prepareQuery(args.Query.MetricName, args.AccessToken)
	if err != nil {
		return response, err
	}
	req, err := transformPointQuery(args.Query, metricMeta)
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeQueryParsingFailed, Description: fmt.Sprintf("can't transform query: %v", err)}
		return response, err
	}

	r, _, err := h.ah.handleGetPoint(withRPCEndpointStat(ctx, ms), ai, seriesRequestOptions{}, req)
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeQueryHandlingFailed, Description: fmt.Sprintf("can't handle query: %v", err)}
		return response, err
	}
	response.Data = make([]float64, len(r.PointData))
	response.Meta = make([]tlstatshouseApi.PointMeta, len(r.PointData))
	for i, data := range r.PointData {
		response.Data[i] = data
		meta := r.PointMeta[i]
		m := tlstatshouseApi.PointMeta{
			TimeShift: meta.TimeShift,
			Tags:      map[string]string{},
			From:      meta.FromSec,
			To:        meta.ToSec,
		}
		for k, v := range meta.Tags {
			m.Tags[k] = v.Value
		}
		m.SetWhat(whatToFn[meta.What])
		response.Meta[i] = m
	}
	return response, nil
}

func (h *RPCHandler) RawGetQuery(ctx context.Context, hctx *rpc.HandlerContext) error {
	var (
		args   tlstatshouseApi.GetQuery
		resp   tlstatshouseApi.GetQueryResponse
		cancel func()
		err    error
	)
	if _, err = args.Read(hctx.Request); err != nil {
		return fmt.Errorf("failed to deserialize statshouseApi.getQuery request: %w", err)
	}
	if resp, cancel, err = h.GetQuery(hctx.WithContext(ctx), args); err != nil {
		return err
	}
	defer cancel()
	if hctx.Response, err = args.WriteResult(hctx.Response, resp); err != nil {
		return fmt.Errorf("failed to serialize statshouseApi.getQuery response: %w", err)
	}
	return nil
}

func (h *RPCHandler) GetQuery(ctx context.Context, args tlstatshouseApi.GetQuery) (response tlstatshouseApi.GetQueryResponse, cancel func(), err error) {
	var (
		ai         accessInfo
		metricMeta *format.MetricMetaValue
		ms         = newRPCMethodStat(EndpointQuery, args.TLName())
	)

	defer func() {
		if cancel != nil && err != nil {
			cancel()
		}
	}()
	defer func() {
		ms.serviceTime(ai, metricMeta, err)
		ms.serviceTimeDeprecated(ai, err)
	}()

	ai, err = h.parseAccessToken(args.AccessToken)
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeAuthFailed, Description: fmt.Sprintf("can't parse access token: %v", err)}
		return response, cancel, err
	}
	if len(args.Query.Promql) == 0 {
		metricMeta, err = h.ah.getMetricMeta(ai, args.Query.MetricName)
		if err != nil {
			err = rpc.Error{Code: rpcErrorCodeUnknownMetric, Description: fmt.Sprintf("can't get metric's meta: %v", err)}
			return response, cancel, err
		}
		LogMetric(format.TagValueIDRPC, ai.user, strconv.FormatInt(int64(metricMeta.MetricID), 10))
	}

	req, err := transformQuery(args.Query, metricMeta)
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeQueryParsingFailed, Description: fmt.Sprintf("can't transform query: %v", err)}
		return response, cancel, err
	}

	res, cancel, err := h.ah.handlePromqlQuery(withRPCEndpointStat(ctx, ms), ai, req, seriesRequestOptions{})
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeQueryHandlingFailed, Description: fmt.Sprintf("can't handle query: %v", err)}
		return response, cancel, err
	}

	response.TotalTimePoints = int32(len(res.Series.Time))
	response.SeriesMeta = make([]tlstatshouseApi.SeriesMeta, 0, len(res.Series.SeriesMeta))
	for _, meta := range res.Series.SeriesMeta {
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
		response.SeriesMeta = append(response.SeriesMeta, m)
	}
	if args.Query.IsSetExcessPointsFlag() {
		response.SetExcessPointLeft(res.ExcessPointLeft)
		response.SetExcessPointRight(res.ExcessPointRight)
	}

	columnSize, totalSize, metaSize := estimateResponseSize(res)
	if totalSize <= chunkMaxSize {
		response.Series.Time = res.Series.Time
		response.Series.SeriesData = make([][]float64, 0, len(res.Series.SeriesData))
		for _, data := range res.Series.SeriesData {
			response.Series.SeriesData = append(response.Series.SeriesData, *data)
		}

		return response, cancel, nil
	}

	chunks := chunkResponse(res, columnSize, totalSize, metaSize)

	response.Series = chunks[0] // return first chunk immediately

	rid := int64(rand.Uint64())
	err = h.brs.Set(ctx, rid, ai.user, chunks[1:], bigResponseTTL)
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeChunkStorageFailed, Description: fmt.Sprintf("can't save chunks: %v", err)}
		return response, cancel, err
	}

	response.ResponseId = rid
	response.ChunkIds = make([]int32, 0, len(chunks)-1)
	for i := 1; i < len(chunks); i++ {
		response.ChunkIds = append(response.ChunkIds, int32(i-1))
	}

	return response, cancel, nil
}

func (h *RPCHandler) GetChunk(_ context.Context, args tlstatshouseApi.GetChunk) (tlstatshouseApi.GetChunkResponse, error) {
	var (
		ai       accessInfo
		response tlstatshouseApi.GetChunkResponse
		err      error
	)

	defer func(ms *rpcMethodStat) {
		ms.serviceTime(ai, nil, err)
		ms.serviceTimeDeprecated(ai, err)
	}(newRPCMethodStat(endpointChunk, args.TLName()))

	ai, err = h.parseAccessToken(args.AccessToken)
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeAuthFailed, Description: fmt.Sprintf("can't parse access token: %v", err)}
		return response, err
	}

	rid := args.ResponseId
	br, ok := h.brs.Get(rid)
	if !ok {
		err = rpc.Error{Code: rpcErrorCodeNotFound, Description: fmt.Sprintf("can't find response %q", rid)}
		return response, err
	}

	if br.owner != ai.user {
		err = rpc.Error{Code: rpcErrorCodeForbidden, Description: fmt.Sprintf("response %d belongs to another user", rid)}
		return response, err
	}

	cid := int(args.ChunkId)
	if cid > len(br.chunks)-1 {
		err = rpc.Error{Code: rpcErrorCodeBadChunkID, Description: fmt.Sprintf("got id %q, there are only %d chunks", rid, len(br.chunks))}
		return response, err
	}

	response.Series = br.chunks[cid]
	response.Index = int32(cid)

	return response, err
}

func (h *RPCHandler) ReleaseChunks(_ context.Context, args tlstatshouseApi.ReleaseChunks) (tlstatshouseApi.ReleaseChunksResponse, error) {
	var (
		ai       accessInfo
		response tlstatshouseApi.ReleaseChunksResponse
		err      error
	)

	defer func(ms *rpcMethodStat) {
		ms.serviceTime(ai, nil, err)
		ms.serviceTimeDeprecated(ai, err)
	}(newRPCMethodStat(endpointChunk, args.TLName()))

	ai, err = h.parseAccessToken(args.AccessToken)
	if err != nil {
		err = rpc.Error{Code: rpcErrorCodeAuthFailed, Description: fmt.Sprintf("can't parse access token: %v", err)}
		return response, err
	}

	rid := args.ResponseId
	br, ok := h.brs.Get(rid)
	if !ok {
		err = rpc.Error{Code: rpcErrorCodeNotFound, Description: fmt.Sprintf("can't find response %q", rid)}
		return response, err
	}

	if br.owner != ai.user {
		err = rpc.Error{Code: rpcErrorCodeForbidden, Description: fmt.Sprintf("response %q belongs to another user", rid)}
		return response, err
	}

	response.ReleasedChunkCount = int32(h.brs.Release(rid))

	return response, nil
}

func (h *RPCHandler) parseAccessToken(token string) (accessInfo, error) {
	return parseAccessToken(h.jwtHelper, token, h.protectedPrefixes, h.localMode, h.insecureMode)
}

func transformQuery(q tlstatshouseApi.Query, meta *format.MetricMetaValue) (req seriesRequest, err error) {
	var filterIn, filterNotIn map[string][]string
	if meta != nil {
		filterIn, filterNotIn, err = parseFilterValues(q.Filter, meta)
		if err != nil {
			return req, fmt.Errorf("can't parse filter: %v", err)
		}
	} else if len(q.Promql) == 0 {
		return req, fmt.Errorf("neither metric name nor PromQL expression specified")
	}

	width, widthKind, err := parseWidth(q.Interval, q.WidthAgg)
	if err != nil {
		return req, fmt.Errorf("can't parse interval: %v", err)
	}

	timeShifts := make([]time.Duration, 0, len(q.TimeShift))
	for _, ts := range q.TimeShift {
		if width == _1M && ts%_1M != 0 {
			return req, fmt.Errorf("time shift %d can't be used with month interval", ts)
		}
		timeShifts = append(timeShifts, time.Duration(ts)*time.Second)
	}

	var what []string
	if q.IsSetWhat() {
		what = make([]string, 0, len(q.What))
		for _, fn := range q.What {
			what = append(what, fnToString[fn])
		}
	} else {
		what = []string{fnToString[q.Function]}
	}

	req = seriesRequest{
		version:             strconv.FormatInt(int64(q.Version), 10),
		numResults:          int(q.TopN),
		metricWithNamespace: q.MetricName,
		from:                time.Unix(q.TimeFrom, 0),
		to:                  time.Unix(q.TimeTo, 0),
		width:               width,
		widthKind:           widthKind,
		shifts:              timeShifts,
		what:                what,
		by:                  q.GroupBy,
		filterIn:            filterIn,
		filterNotIn:         filterNotIn,
		promQL:              q.Promql,
		maxHost:             q.IsSetMaxHostFlag(),
		expandToLODBoundary: q.IsSetExcessPointsFlag(),
	}
	return req, nil
}

func transformPointQuery(q tlstatshouseApi.QueryPoint, meta *format.MetricMetaValue) (req seriesRequest, err error) {
	filterIn, filterNotIn, err := parseFilterValues(q.Filter, meta)
	if err != nil {
		return req, fmt.Errorf("can't parse filter: %v", err)
	}

	timeShifts := make([]time.Duration, 0, len(q.TimeShift))
	for _, ts := range q.TimeShift {
		timeShifts = append(timeShifts, time.Duration(ts)*time.Second)
	}

	var what []string
	if q.IsSetWhat() {
		what = make([]string, 0, len(q.What))
		for _, fn := range q.What {
			what = append(what, fnToString[fn])
		}
	} else {
		what = []string{fnToString[q.Function]}
	}

	req = seriesRequest{
		version:             strconv.FormatInt(int64(q.Version), 10),
		numResults:          int(q.TopN),
		metricWithNamespace: q.MetricName,
		from:                time.Unix(q.TimeFrom, 0),
		to:                  time.Unix(q.TimeTo, 0),
		shifts:              timeShifts,
		what:                what,
		by:                  q.GroupBy,
		filterIn:            filterIn,
		filterNotIn:         filterNotIn,
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

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouseApi"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/promql"
	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
	"pgregory.net/rand"
)

const (
	chunkMaxSize   = 10_000_000 // little less than 10 MiB
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

type rpcRouter struct {
	*Handler
	brs *BigResponseStorage
}

type rpcRequestHandler struct {
	requestHandler
	brs *BigResponseStorage
}

func NewRPCRouter(ah *Handler, brs *BigResponseStorage) *rpcRouter {
	return &rpcRouter{Handler: ah, brs: brs}
}

func (h *rpcRouter) Handle(ctx context.Context, hctx *rpc.HandlerContext) (err error) {
	timeNow := time.Now()
	w := rpcRequestHandler{
		requestHandler: requestHandler{
			Handler: h.Handler,
			endpointStat: endpointStat{
				timestamp:  timeNow,
				protocol:   format.TagValueIDRPC,
				dataFormat: "TL",
				timings: ServerTimingHeader{
					Timings: make(map[string][]time.Duration),
					started: timeNow,
				},
			},
		},
		brs: h.brs,
	}
	defer func() {
		var code int // "0" means "success"
		if r := recover(); r != nil {
			w.savePanic(hctx.RequestFunctionName, r, debug.Stack())
			code = rpc.TlErrorInternal
			err = &rpc.Error{Code: rpc.TlErrorInternal}
		} else if err != nil {
			code = rpcCode(err)
		}
		w.endpointStat.report(code, format.BuiltinMetricMetaAPIResponseTime.Name)
	}()
	var tag uint32
	var req []byte
	var hfn func(context.Context, *rpc.HandlerContext) error
	tag, req, _ = basictl.NatReadTag(hctx.Request)
	switch tag {
	case 0x0c7349bb:
		hctx.RequestFunctionName = "statshouseApi.getQuery"
		hfn = w.rawGetQuery
		w.endpointStat.endpoint = EndpointQuery
	case 0x0c7348bb:
		hctx.RequestFunctionName = "statshouseApi.getQueryPoint"
		hfn = w.rawGetQueryPoint
		w.endpointStat.endpoint = EndpointPoint
	case 0x52721884:
		hctx.RequestFunctionName = "statshouseApi.getChunk"
		hfn = w.rawGetChunk
		w.endpointStat.endpoint = endpointChunk
	case 0x62adc773:
		hctx.RequestFunctionName = "statshouseApi.releaseChunks"
		w.endpointStat.endpoint = endpointChunk
		hfn = w.rawReleaseChunks
	default:
		return rpc.ErrNoHandler
	}
	hctx.Request = req
	if err = hfn(ctx, hctx); err != nil {
		return fmt.Errorf("failed to handle %s: %w", hctx.RequestFunctionName, err)
	}
	return nil
}

func (h *rpcRequestHandler) init(accessToken string, version int32) (err error) {
	var s string
	if version != 0 {
		s = strconv.Itoa(int(version))
	}
	return h.requestHandler.init(accessToken, s)
}

func (h *rpcRequestHandler) rawGetQueryPoint(ctx context.Context, hctx *rpc.HandlerContext) (err error) {
	var args tlstatshouseApi.GetQueryPoint
	if _, err = args.Read(hctx.Request); err != nil {
		err = fmt.Errorf("failed to deserialize statshouseApi.GetQueryPoint request: %w", err)
		return err
	}
	if err = h.init(args.AccessToken, args.Query.Version); err != nil {
		return err
	}
	qry := seriesRequestRPC{
		filter:      args.Query.Filter,
		function:    args.Query.Function,
		groupBy:     args.Query.GroupBy,
		metricName:  args.Query.MetricName,
		timeFrom:    args.Query.TimeFrom,
		timeShift:   args.Query.TimeShift,
		timeTo:      args.Query.TimeTo,
		topN:        args.Query.TopN,
		what:        args.Query.What,
		whatFlagSet: args.Query.IsSetWhat(),
	}
	req, err := qry.toSeriesRequest(h)
	if err != nil {
		return err
	}
	sr, cancel, err := h.handleSeriesRequest(ctx, req, seriesRequestOptions{mode: data_model.PointQuery})
	h.endpointStat.report(rpcCode(err), format.BuiltinMetricMetaAPIServiceTime.Name)
	if err != nil {
		err = &rpc.Error{Code: rpcErrorCodeQueryHandlingFailed, Description: fmt.Sprintf("can't handle query: %v", err)}
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
		var what string
		var tags map[string]SeriesMetaTag
		what, meta.TimeShift, tags = sr.queryFuncShiftAndTagsAt(i)
		if v, ok := ParseTLFunc(what); ok {
			meta.SetWhat(v)
		}
		meta.Tags = make(map[string]string, len(tags))
		for k, v := range tags {
			meta.Tags[k] = v.Value
		}
		res.Meta = append(res.Meta, meta)
		res.Data = append(res.Data, (*d.Values)[0])
	}
	if hctx.Response, err = args.WriteResult(hctx.Response, res); err != nil {
		return fmt.Errorf("failed to serialize tlstatshouseApi.GetQueryPointResponse response: %w", err)
	}
	return nil
}

func (h *rpcRequestHandler) rawGetQuery(ctx context.Context, hctx *rpc.HandlerContext) (err error) {
	var args tlstatshouseApi.GetQuery
	if _, err = args.Read(hctx.Request); err != nil {
		err = fmt.Errorf("failed to deserialize statshouseApi.getQuery request: %w", err)
		return err
	}
	if err = h.init(args.AccessToken, args.Query.Version); err != nil {
		return err
	}
	qry := seriesRequestRPC{
		filter:      args.Query.Filter,
		function:    args.Query.Function,
		groupBy:     args.Query.GroupBy,
		interval:    args.Query.Interval,
		metricName:  args.Query.MetricName,
		promQL:      args.Query.Promql,
		timeFrom:    args.Query.TimeFrom,
		timeShift:   args.Query.TimeShift,
		timeTo:      args.Query.TimeTo,
		topN:        args.Query.TopN,
		what:        args.Query.What,
		widthAgg:    args.Query.WidthAgg,
		whatFlagSet: args.Query.IsSetWhat(),
	}
	req, err := qry.toSeriesRequest(h)
	if err != nil {
		return err
	}
	srs, cancel, err := h.handleSeriesRequestS(ctx, req, make([]seriesResponse, 1))
	h.endpointStat.report(rpcCode(err), format.BuiltinMetricMetaAPIServiceTime.Name)
	if err != nil {
		return err
	}
	defer cancel()
	sr := h.buildSeriesResponse(srs...)
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
		if v, ok := ParseTLFunc(meta.What); ok {
			m.SetWhat(v)
		}
		res.SeriesMeta = append(res.SeriesMeta, m)
	}
	if args.Query.IsSetExcessPointsFlag() {
		res.SetExcessPointLeft(sr.ExcessPointLeft)
		res.SetExcessPointRight(sr.ExcessPointRight)
	}
	columnSize, totalSize, metaSize := estimateResponseSize(sr)
	if totalSize <= chunkMaxSize {
		res.Series.Time = sr.Series.Time
		res.Series.SeriesData = make([][]float64, 0, len(sr.Series.SeriesData))
		for _, data := range sr.Series.SeriesData {
			res.Series.SeriesData = append(res.Series.SeriesData, *FloatSlicePtrToNative(data))
		}
	} else if chunkMaxSize < metaSize {
		return &rpc.Error{
			Code:        rpcErrorCodeChunkStorageFailed,
			Description: fmt.Sprintf("response metadata size %d out of range", metaSize),
		}
	} else {
		chunks := chunkResponse(sr, columnSize, totalSize, metaSize)
		res.Series = chunks[0] // return first chunk immediately
		rid := int64(rand.Uint64())
		if err = h.brs.Set(ctx, rid, h.accessInfo.user, chunks[1:], bigResponseTTL); err != nil {
			return &rpc.Error{Code: rpcErrorCodeChunkStorageFailed, Description: fmt.Sprintf("can't save chunks: %v", err)}
		}
		res.ResponseId = rid
		res.ChunkIds = make([]int32, 0, len(chunks)-1)
		for i := 1; i < len(chunks); i++ {
			res.ChunkIds = append(res.ChunkIds, int32(i-1))
		}
	}
	if hctx.Response, err = args.WriteResult(hctx.Response, res); err != nil {
		return fmt.Errorf("failed to serialize statshouseApi.getQuery response: %w", err)
	}
	return nil
}

type seriesRequestRPC struct {
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
	what        []tlstatshouseApi.Function
	widthAgg    string
	whatFlagSet bool
}

func (h *rpcRequestHandler) rawGetChunk(ctx context.Context, hctx *rpc.HandlerContext) (err error) {
	var args tlstatshouseApi.GetChunk
	if _, err = args.Read(hctx.Request); err != nil {
		return fmt.Errorf("failed to deserialize %s request: %w", args.TLName(), err)
	}
	if err = h.init(args.AccessToken, 0); err != nil {
		err = &rpc.Error{Code: rpcErrorCodeAuthFailed, Description: fmt.Sprintf("can't parse access token: %v", err)}
		return err
	}
	br, ok := h.brs.Get(args.ResponseId)
	if !ok {
		err = &rpc.Error{Code: rpcErrorCodeNotFound, Description: fmt.Sprintf("can't find response %q", args.ResponseId)}
		h.endpointStat.report(rpcCode(err), format.BuiltinMetricMetaAPIServiceTime.Name)
		return err
	}
	if br.owner != h.accessInfo.user {
		err = &rpc.Error{Code: rpcErrorCodeForbidden, Description: fmt.Sprintf("response %d belongs to another user", args.ResponseId)}
		h.endpointStat.report(rpcCode(err), format.BuiltinMetricMetaAPIServiceTime.Name)
		return err
	}
	if int(args.ChunkId) > len(br.chunks)-1 {
		err = &rpc.Error{Code: rpcErrorCodeBadChunkID, Description: fmt.Sprintf("got id %q, there are only %d chunks", args.ResponseId, len(br.chunks))}
		h.endpointStat.report(rpcCode(err), format.BuiltinMetricMetaAPIServiceTime.Name)
		return err
	}
	h.endpointStat.report(0, format.BuiltinMetricMetaAPIServiceTime.Name)
	res := tlstatshouseApi.GetChunkResponse{
		Series: br.chunks[int(args.ChunkId)],
		Index:  args.ChunkId,
	}
	if hctx.Response, err = args.WriteResult(hctx.Response, res); err != nil {
		return fmt.Errorf("failed to serialize %s response: %w", res.TLName(), err)
	}
	return nil
}

func (h *rpcRequestHandler) rawReleaseChunks(ctx context.Context, hctx *rpc.HandlerContext) (err error) {
	var args tlstatshouseApi.ReleaseChunks
	if _, err = args.Read(hctx.Request); err != nil {
		return fmt.Errorf("failed to deserialize %s request: %w", args.TLName(), err)
	}
	if err = h.init(args.AccessToken, 0); err != nil {
		err = &rpc.Error{Code: rpcErrorCodeAuthFailed, Description: fmt.Sprintf("can't parse access token: %v", err)}
		return err
	}
	br, ok := h.brs.Get(args.ResponseId)
	if !ok {
		err = &rpc.Error{Code: rpcErrorCodeNotFound, Description: fmt.Sprintf("can't find response %q", args.ResponseId)}
		h.endpointStat.report(rpcCode(err), format.BuiltinMetricMetaAPIServiceTime.Name)
		return err
	}
	if br.owner != h.accessInfo.user {
		err = &rpc.Error{Code: rpcErrorCodeForbidden, Description: fmt.Sprintf("response %q belongs to another user", args.ResponseId)}
		h.endpointStat.report(rpcCode(err), format.BuiltinMetricMetaAPIServiceTime.Name)
		return err
	}
	h.endpointStat.report(0, format.BuiltinMetricMetaAPIServiceTime.Name)
	res := tlstatshouseApi.ReleaseChunksResponse{
		ReleasedChunkCount: int32(h.brs.Release(args.ResponseId)),
	}
	if hctx.Response, err = args.WriteResult(hctx.Response, res); err != nil {
		return fmt.Errorf("failed to serialize %s response: %w", res.TLName(), err)
	}
	return nil
}

func (qry *seriesRequestRPC) toSeriesRequest(h *rpcRequestHandler) (seriesRequest, error) {
	req := seriesRequest{
		version:    h.version,
		numResults: int(qry.topN),
		metricName: qry.metricName,
		from:       time.Unix(qry.timeFrom, 0),
		to:         time.Unix(qry.timeTo, 0),
		by:         qry.groupBy,
		promQL:     qry.promQL,
	}
	metric, err := h.getMetricMeta(qry.metricName)
	if err != nil {
		err = &rpc.Error{Code: rpcErrorCodeUnknownMetric, Description: fmt.Sprintf("can't get metric's meta: %v", err)}
		return seriesRequest{}, err
	}
	h.endpointStat.setMetricMeta(metric)
	if metric != nil {
		req.filterIn, req.filterNotIn, err = parseFilterValues(qry.filter, metric)
		if err != nil {
			err = fmt.Errorf("can't parse filter: %v", err)
			return seriesRequest{}, err
		}
	} else if len(qry.promQL) == 0 {
		err = fmt.Errorf("neither metric name nor PromQL expression specified")
		return seriesRequest{}, err
	}
	if len(qry.interval) != 0 || len(qry.widthAgg) != 0 {
		width, widthKind, err := parseWidth(qry.interval, qry.widthAgg)
		if err != nil {
			err = fmt.Errorf("can't parse interval: %v", err)
			return seriesRequest{}, err
		}
		if widthKind == widthAutoRes {
			req.screenWidth = int64(width)
		} else {
			req.step = int64(width)
		}
	}
	req.shifts = make([]time.Duration, 0, len(qry.timeShift))
	for _, ts := range qry.timeShift {
		if req.step == _1M && ts%_1M != 0 {
			err = fmt.Errorf("time shift %d can't be used with month interval", ts)
			return seriesRequest{}, err
		}
		req.shifts = append(req.shifts, time.Duration(ts)*time.Second)
	}
	if qry.whatFlagSet {
		req.what = make([]promql.SelectorWhat, 0, len(qry.what))
		for _, fn := range qry.what {
			req.what = append(req.what, promql.QueryFuncFromTLFunc(fn, &req.maxHost))
		}
	} else {
		req.what = []promql.SelectorWhat{promql.QueryFuncFromTLFunc(qry.function, &req.maxHost)}
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
				tag := meta.Name2Tag(tid)
				if tag == nil {
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
		firstChunk.SeriesData = append(firstChunk.SeriesData, (*FloatSlicePtrToNative(data))[0:firstColumnPerChunk])
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
			chunk.SeriesData = append(chunk.SeriesData, (*FloatSlicePtrToNative(data))[low:high])
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

func rpcCode(err error) int {
	if err == nil {
		return 0
	}
	var rpcError *rpc.Error
	if errors.As(err, &rpcError) {
		return int(rpcError.Code)
	}
	return -1
}

package api

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
)

type (
	tableReqParams struct {
		req               seriesRequest
		user              string
		metricMeta        *format.MetricMetaValue
		isStringTop       bool
		mappedFilterIn    data_model.TagFilters
		mappedFilterNotIn data_model.TagFilters
		rawValue          bool
		desiredStepMul    int64
		location          *time.Location
	}

	tableRowKey struct {
		time int64
		tsTags
	}
)

type loadPointsFunc func(ctx context.Context, h *requestHandler, pq *queryBuilder, lod data_model.LOD, avoidCache bool) ([][]tsSelectRow, error)

func (h *requestHandler) getTableFromLODs(ctx context.Context, lods []data_model.LOD, tableReqParams tableReqParams,
	loadPoints loadPointsFunc) (_ []queryTableRow, hasMore bool, _ error) {
	req := tableReqParams.req
	metricMeta := tableReqParams.metricMeta
	rowsIdx := make(map[tableRowKey]int)
	queryRows := make(queryTableRows, 0)
	used := map[int]struct{}{}
	shouldSort := false
	fromTime, toTime := tableReqParams.req.fromRow.Time, tableReqParams.req.toRow.Time
	if tableReqParams.req.fromEnd {
		fromTime, toTime = toTime, fromTime
	}
	if toTime == 0 {
		toTime = math.MaxInt
	}
	what := h.getHandlerWhat(req.what)
	for qIndex, q := range what {
		rowsCount := 0
		for k := range lods {
			if tableReqParams.req.fromEnd {
				k = len(lods) - k - 1
			}
			lod := lods[k]
			if toTime < lod.FromSec || lod.ToSec < fromTime {
				continue
			}
			pq := queryBuilder{
				version:          h.version,
				user:             tableReqParams.user,
				metric:           metricMeta,
				what:             q.qry,
				by:               metricMeta.GroupBy(req.by),
				filterIn:         tableReqParams.mappedFilterIn,
				filterNotIn:      tableReqParams.mappedFilterNotIn,
				sort:             req.tableSort(),
				strcmpOff:        h.Version3StrcmpOff.Load(),
				utcOffset:        h.utcOffset,
				newShardingStart: h.NewShardingStart.Load(),
			}
			m, err := loadPoints(ctx, h, &pq, data_model.LOD{
				FromSec:     shiftTimestamp(lod.FromSec, lod.StepSec, 0, lod.Location),
				ToSec:       shiftTimestamp(lod.ToSec, lod.StepSec, 0, lod.Location),
				StepSec:     lod.StepSec,
				Version:     lod.Version,
				Metric:      pq.metric,
				NewSharding: h.newSharding(metricMeta, lod.FromSec),
				HasPreKey:   lod.HasPreKey,
				PreKeyOnly:  lod.PreKeyOnly,
				Location:    tableReqParams.location,
			}, req.avoidCache)
			if err != nil {
				return nil, false, err
			}
			var rowRepr RowMarker
			rows, hasMoreValues := limitQueries(m, req.fromRow, req.toRow, req.fromEnd, req.numResults-rowsCount)
			for i := 0; i < len(rows); i++ {
				if toTime < rows[i].time || rows[i].time < fromTime {
					continue
				}
				rowsCount++
				rowRepr.Time = rows[i].time
				rowRepr.Tags = rowRepr.Tags[:0]
				tags := &rows[i].tsTags
				kvs := make(map[string]SeriesMetaTag, 16)
				for j := 0; j < format.MaxTags; j++ {
					wasAdded := h.maybeAddQuerySeriesTagValue(kvs, metricMeta, req.version, req.by, j, tags)
					if wasAdded {
						rowRepr.Tags = append(rowRepr.Tags, RawTag{
							Index: j,
							Value: tags.tag[j],
						})
					}
				}
				if tags.stag[format.StringTopTagIndexV3] != "" {
					rowRepr.SKey = maybeAddQuerySeriesTagValueString(kvs, req.by, tags.stag[format.StringTopTagIndexV3])
				} else if tags.tag[format.StringTopTagIndexV3] != 0 {
					v := h.getRichTagValue(metricMeta, req.version, format.StringTopTagID, tags.tag[format.StringTopTagIndexV3])
					v = emptyToUnspecified(v)
					kvs[format.LegacyStringTopTagID] = SeriesMetaTag{Value: v}
					rowRepr.SKey = v
				}
				key := tableRowKey{
					time:   rows[i].time,
					tsTags: rows[i].tsTags,
				}
				var ix int
				var ok bool
				if ix, ok = rowsIdx[key]; !ok {
					ix = len(queryRows)
					rowsIdx[key] = ix
					queryRows = append(queryRows, queryTableRow{
						Time:    rows[i].time,
						Data:    make([]Float64, 0, len(req.what)),
						Tags:    kvs,
						row:     rows[i],
						rowRepr: rowRepr,
					})
					for j := 0; j < qIndex; j++ {
						queryRows[ix].Data = append(queryRows[ix].Data, NaN())
					}
					shouldSort = shouldSort || qIndex > 0
				}
				used[ix] = struct{}{}
				queryRows[ix].Data = q.appendRowValues(queryRows[ix].Data, &rows[i], tableReqParams.desiredStepMul, &lod)
			}
			for _, ix := range rowsIdx {
				if _, ok := used[ix]; ok {
					delete(used, ix)
				} else {
					queryRows[ix].Data = append(queryRows[ix].Data, NaN())
				}
			}
			if hasMoreValues {
				hasMore = true
				break
			}
		}
	}
	if tableReqParams.req.fromEnd {
		sort.Sort(sort.Reverse(queryRows))
	} else {
		sort.Sort(queryRows)
	}
	return queryRows, hasMore, nil
}

func (h *Handler) newSharding(metric *format.MetricMetaValue, timestamp int64) bool {
	return metric.NewSharding(timestamp, h.NewShardingStart.Load())
}

func limitQueries(rowsByTime [][]tsSelectRow, from, to RowMarker, fromEnd bool, limit int) (res []tsSelectRow, hasMore bool) {
	if limit <= 0 {
		return nil, len(rowsByTime) > 0
	}
	limitedRows := make([]tsSelectRow, 0, limit)
	for i := range rowsByTime {
		if fromEnd {
			i = len(rowsByTime) - i - 1
		}
		rows := rowsByTime[i]
		if len(rows) > 0 && !inRange(rows[0], from, to, fromEnd) &&
			!inRange(rows[len(rows)-1], from, to, fromEnd) {
			continue
		}
		for _, row := range rows {
			if len(limitedRows) == limit {
				return limitedRows, true
			}
			if !inRange(row, from, to, fromEnd) {
				continue
			}
			limitedRows = append(limitedRows, row)
		}
	}
	return limitedRows, false
}

func inRange(row tsSelectRow, from, to RowMarker, fromEnd bool) bool {
	if from.Time != 0 {
		if !lessThan(from, row, row.tsTags.stag[format.StringTopTagIndexV3], false, fromEnd) {
			return false
		}
	}
	if to.Time != 0 {
		if lessThan(to, row, row.tsTags.stag[format.StringTopTagIndexV3], true, fromEnd) {
			return false
		}
	}
	return true
}

func (r *seriesRequest) tableSort() querySort {
	if r.fromEnd {
		return sortDescending
	} else {
		return sortAscending
	}
}

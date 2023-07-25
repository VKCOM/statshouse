package api

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/vkcom/statshouse/internal/format"
)

type (
	tableReqParams struct {
		req               tableRequest
		queries           []*query
		user              string
		metricMeta        *format.MetricMetaValue
		isStringTop       bool
		mappedFilterIn    map[string][]interface{}
		mappedFilterNotIn map[string][]interface{}
		rawValue          bool
		desiredStepMul    int64
		location          *time.Location
	}

	tableRowKey struct {
		time int64
		tsTags
	}
)

type loadPoints func(ctx context.Context, version string, key string, pq *preparedPointsQuery, lod lodInfo, avoidCache bool) ([][]tsSelectRow, error)
type maybeAddQuerySeriesTagValue func(m map[string]SeriesMetaTag, metricMeta *format.MetricMetaValue, version string, by []string, tagID string, id int32) bool

func getTableFromLODs(ctx context.Context, lods []lodInfo, tableReqParams tableReqParams,
	loadPoints loadPoints,
	maybeAddQuerySeriesTagValue maybeAddQuerySeriesTagValue) (_ []queryTableRow, hasMore bool, _ error) {
	req := tableReqParams.req
	queries := tableReqParams.queries
	metricMeta := tableReqParams.metricMeta
	rowsIdx := make(map[tableRowKey]int)
	queryRows := make([]queryTableRow, 0)
	used := map[int]struct{}{}
	shouldSort := false
	for qIndex, q := range queries {
		rowsCount := 0
		qs := normalizedQueryString(req.metricWithNamespace, q.whatKind, req.by, req.filterIn, req.filterNotIn, true)
		pq := &preparedPointsQuery{
			user:        tableReqParams.user,
			version:     req.version,
			metricID:    metricMeta.MetricID,
			preKeyTagID: metricMeta.PreKeyTagID,
			isStringTop: tableReqParams.isStringTop,
			kind:        q.whatKind,
			by:          q.by,
			filterIn:    tableReqParams.mappedFilterIn,
			filterNotIn: tableReqParams.mappedFilterNotIn,
			orderBy:     true,
			desc:        req.fromEnd,
		}

		for _, lod := range lods {
			m, err := loadPoints(ctx, req.version, qs, pq, lodInfo{
				fromSec:    shiftTimestamp(lod.fromSec, lod.stepSec, 0, lod.location),
				toSec:      shiftTimestamp(lod.toSec, lod.stepSec, 0, lod.location),
				stepSec:    lod.stepSec,
				table:      lod.table,
				hasPreKey:  lod.hasPreKey,
				preKeyOnly: lod.preKeyOnly,
				location:   tableReqParams.location,
			}, req.avoidCache)
			if err != nil {
				return nil, false, err
			}
			var rowRepr RowMarker
			rows, hasMoreValues := limitQueries(m, req.fromRow, req.toRow, req.fromEnd, req.limit-rowsCount)
			for i := 0; i < len(rows); i++ {
				rowsCount++
				rowRepr.Time = rows[i].time
				rowRepr.Tags = rowRepr.Tags[:0]
				tags := &rows[i].tsTags
				kvs := make(map[string]SeriesMetaTag, 16)
				for j := 0; j < format.MaxTags; j++ {
					tagName := format.TagID(j)
					wasAdded := maybeAddQuerySeriesTagValue(kvs, metricMeta, req.version, q.by, tagName, tags.tag[j])
					if wasAdded {
						rowRepr.Tags = append(rowRepr.Tags, RawTag{
							Index: j,
							Value: tags.tag[j],
						})
					}
				}
				skey := maybeAddQuerySeriesTagValueString(kvs, q.by, format.StringTopTagID, &tags.tagStr)
				rowRepr.SKey = skey
				data := selectTSValue(q.what, req.maxHost, tableReqParams.rawValue, tableReqParams.desiredStepMul, &rows[i])
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
						Data:    make([]float64, 0, len(queries)),
						Tags:    kvs,
						row:     rows[i],
						rowRepr: rowRepr,
					})
					for j := 0; j < qIndex; j++ {
						queryRows[ix].Data = append(queryRows[ix].Data, math.NaN())
					}
					shouldSort = shouldSort || qIndex > 0
				}
				used[ix] = struct{}{}
				queryRows[ix].Data = append(queryRows[ix].Data, data)
			}
			for _, ix := range rowsIdx {
				if _, ok := used[ix]; ok {
					delete(used, ix)
				} else {
					queryRows[ix].Data = append(queryRows[ix].Data, math.NaN())
				}
			}
			if hasMoreValues {
				hasMore = true
				break
			}
		}
	}
	sort.Slice(queryRows, func(i, j int) bool {
		return rowMarkerLessThan(queryRows[i].rowRepr, queryRows[j].rowRepr)
	})
	return queryRows, hasMore, nil
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
		if len(rows) > 0 && !inRange(rows[0], from, to) &&
			!inRange(rows[len(rows)-1], from, to) {
			continue
		}
		for _, row := range rows {
			if len(limitedRows) == limit {
				return limitedRows, true
			}
			if !inRange(row, from, to) {
				continue
			}
			limitedRows = append(limitedRows, row)
		}
	}
	return limitedRows, false
}

func inRange(row tsSelectRow, from, to RowMarker) bool {
	if from.Time != 0 {
		if !lessThan(from, row, skeyFromFixedString(&row.tsTags.tagStr), false) {
			return false
		}
	}
	if to.Time != 0 {
		if lessThan(to, row, skeyFromFixedString(&row.tsTags.tagStr), true) {
			return false
		}
	}
	return true
}

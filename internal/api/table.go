package api

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

type (
	tableReqParams struct {
		req               seriesRequest
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

type loadPoints func(ctx context.Context, version string, key string, pq *preparedPointsQuery, lod data_model.LOD, avoidCache bool) ([][]tsSelectRow, error)
type maybeAddQuerySeriesTagValue func(m map[string]SeriesMetaTag, metricMeta *format.MetricMetaValue, version string, by []string, tagIndex int, id int32) bool

func getTableFromLODs(ctx context.Context, lods []data_model.LOD, tableReqParams tableReqParams,
	loadPoints loadPoints,
	maybeAddQuerySeriesTagValue maybeAddQuerySeriesTagValue) (_ []queryTableRow, hasMore bool, _ error) {
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
	for qIndex, q := range tableReqParams.req.what {
		rowsCount := 0
		kind := q.What.Kind(req.maxHost)
		qs := normalizedQueryString(req.metricName, kind, req.by, req.filterIn, req.filterNotIn, true)
		pq := &preparedPointsQuery{
			user:        tableReqParams.user,
			version:     req.version,
			metricID:    metricMeta.MetricID,
			preKeyTagID: metricMeta.PreKeyTagID,
			isStringTop: tableReqParams.isStringTop,
			kind:        kind,
			by:          req.by,
			filterIn:    tableReqParams.mappedFilterIn,
			filterNotIn: tableReqParams.mappedFilterNotIn,
			orderBy:     true,
			desc:        req.fromEnd,
		}
		for k := range lods {
			if tableReqParams.req.fromEnd {
				k = len(lods) - k - 1
			}
			lod := lods[k]
			if toTime < lod.FromSec || lod.ToSec < fromTime {
				continue
			}
			m, err := loadPoints(ctx, req.version, qs, pq, data_model.LOD{
				FromSec:    shiftTimestamp(lod.FromSec, lod.StepSec, 0, lod.Location),
				ToSec:      shiftTimestamp(lod.ToSec, lod.StepSec, 0, lod.Location),
				StepSec:    lod.StepSec,
				Table:      lod.Table,
				HasPreKey:  lod.HasPreKey,
				PreKeyOnly: lod.PreKeyOnly,
				Location:   tableReqParams.location,
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
					wasAdded := maybeAddQuerySeriesTagValue(kvs, metricMeta, req.version, req.by, j, tags.tag[j])
					if wasAdded {
						rowRepr.Tags = append(rowRepr.Tags, RawTag{
							Index: j,
							Value: tags.tag[j],
						})
					}
				}
				skey := maybeAddQuerySeriesTagValueString(kvs, req.by, &tags.tagStr)
				rowRepr.SKey = skey
				data := selectTSValue(q.What, req.maxHost, tableReqParams.desiredStepMul, &rows[i])
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
						Data:    make([]float64, 0, len(req.what)),
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
	if tableReqParams.req.fromEnd {
		sort.Sort(sort.Reverse(queryRows))
	} else {
		sort.Sort(queryRows)
	}
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
		if !lessThan(from, row, skeyFromFixedString(&row.tsTags.tagStr), false, fromEnd) {
			return false
		}
	}
	if to.Time != 0 {
		if lessThan(to, row, skeyFromFixedString(&row.tsTags.tagStr), true, fromEnd) {
			return false
		}
	}
	return true
}

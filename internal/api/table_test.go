package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql"
)

// TODO add general CH mock

func Test_getTableFromLODs(t *testing.T) {
	t.Skip()
	return
	l, _ := time.LoadLocation("")
	p := tableReqParams{
		req: seriesRequest{numResults: 100, what: []promql.SelectorWhat{
			promql.SelectorWhat{Digest: promql.DigestCount},
		}},
		user:           "",
		metricMeta:     &format.MetricMetaValue{},
		desiredStepMul: 1,
		location:       l,
	}
	lod := data_model.LOD{
		FromSec:  1,
		ToSec:    10,
		StepSec:  1,
		Table:    "",
		Location: l,
	}
	_ = queryTableRow{
		Time:    0,
		Data:    nil,
		Tags:    nil,
		row:     tsSelectRow{},
		rowRepr: RowMarker{},
	}
	rows := []tsSelectRow{genRow(1), genRow(2), genRow(3), genRow(4), genRow(5), genRow(6), genRow(7), genRow(8)}
	var rowsByTime [][]tsSelectRow
	nop := func(m map[string]SeriesMetaTag, metricMeta *format.MetricMetaValue, version string, by []string, tagIndex int, id int64) bool {
		return false
	}
	load := func(ctx context.Context, h *requestHandler, pq *queryBuilder, lod data_model.LOD, avoidCache bool) ([][]tsSelectRow, error) {
		return rowsByTime, nil
	}
	type args struct {
		rows    []tsSelectRow
		from    RowMarker
		to      RowMarker
		fromEnd bool
		limit   int
	}
	tests := []struct {
		name        string
		args        args
		wantRes     []tsSelectRow
		wantHasMore bool
		err         error
	}{
		{"full", args{rows, RowMarker{}, RowMarker{}, false, 10}, rows, false, nil},
	}
	h := requestHandler{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := tt.args.rows
			rowsByTime = make([][]tsSelectRow, 8)
			for _, row := range rows {
				i := row.time - 1
				rowsByTime[i] = append(rowsByTime[i], row)
			}
			gotRes, gotHasMore, err := h.getTableFromLODs(context.Background(), []data_model.LOD{lod}, p, load, nop)
			assert.Equalf(t, tt.wantRes, gotRes, "limitQueries(%v, %v, %v, %v, %v)", tt.args.rows, tt.args.from, tt.args.to, tt.args.fromEnd, tt.args.limit)
			assert.Equalf(t, tt.wantHasMore, gotHasMore, "limitQueries(%v, %v, %v, %v, %v)", tt.args.rows, tt.args.from, tt.args.to, tt.args.fromEnd, tt.args.limit)
			assert.NoError(t, err)
		})
	}

}

func reverse(rows []tsSelectRow) []tsSelectRow {
	res := make([]tsSelectRow, 0, len(rows))
	for i := len(rows) - 1; i >= 0; i-- {
		res = append(res, rows[i])
	}
	return res
}

func genRow(time int64) tsSelectRow {
	return tsSelectRow{time: time}
}

func Test_limitQueries(t *testing.T) {
	rows := []tsSelectRow{genRow(1), genRow(2), genRow(3), genRow(4), genRow(5), genRow(6), genRow(7), genRow(8)}
	rowsByTime := make([][]tsSelectRow, 0, 8)
	for _, row := range rows {
		rowsByTime = append(rowsByTime, []tsSelectRow{row})
	}
	type args struct {
		rowsByTime [][]tsSelectRow
		from       RowMarker
		to         RowMarker
		fromEnd    bool
		limit      int
	}
	tests := []struct {
		name        string
		args        args
		wantRes     []tsSelectRow
		wantHasMore bool
		err         error
	}{
		{"subslice full", args{rowsByTime, RowMarker{Time: 2}, RowMarker{Time: 7}, false, 10}, rows[2:6], false, nil},
		{"subslice limited", args{rowsByTime, RowMarker{Time: 2}, RowMarker{Time: 7}, false, 2}, rows[2:4], true, nil},
		{"subslice full from end", args{rowsByTime, RowMarker{Time: 7}, RowMarker{Time: 2}, true, 10}, reverse(rows[2:6]), false, nil},
		{"subslice limited from end", args{rowsByTime, RowMarker{Time: 7}, RowMarker{Time: 2}, true, 2}, reverse(rows[4:6]), true, nil},
		{"slice full", args{rowsByTime, RowMarker{Time: 0}, RowMarker{Time: 10}, false, 10}, rows, false, nil},
		{"slice full limited", args{rowsByTime, RowMarker{Time: -1}, RowMarker{Time: -1}, false, 2}, []tsSelectRow{}, false, nil},
		{"slice full", args{rowsByTime, RowMarker{}, RowMarker{}, false, 10}, rows, false, nil},
		{"slice full", args{rowsByTime, RowMarker{}, RowMarker{}, false, 2}, rows[:2], true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRes, gotHasMore := limitQueries(tt.args.rowsByTime, tt.args.from, tt.args.to, tt.args.fromEnd, tt.args.limit)
			assert.Equalf(t, tt.wantRes, gotRes, "limitQueries(%v, %v, %v, %v, %v)", tt.args.rowsByTime, tt.args.from, tt.args.to, tt.args.fromEnd, tt.args.limit)
			assert.Equalf(t, tt.wantHasMore, gotHasMore, "limitQueries(%v, %v, %v, %v, %v)", tt.args.rowsByTime, tt.args.from, tt.args.to, tt.args.fromEnd, tt.args.limit)
		})
	}
}

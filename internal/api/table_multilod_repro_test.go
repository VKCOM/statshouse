package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/promql"
)

// Reproduces the multi-LOD column corruption: when a table query spans more
// than one LOD, rows from an earlier LOD used to get an extra NaN appended for
// every subsequent LOD, so Data column counts diverged between rows.
func TestGetTableFromLODs_MultiLODColumnAlignment(t *testing.T) {
	loc, _ := time.LoadLocation("")
	p := tableReqParams{
		req: seriesRequest{
			numResults: 100,
			what:       []promql.SelectorWhat{{Digest: promql.DigestCount}},
		},
		metricMeta:     &format.MetricMetaValue{},
		desiredStepMul: 1,
		location:       loc,
	}
	// two LODs covering disjoint time ranges
	lods := []data_model.LOD{
		{FromSec: 100, ToSec: 200, StepSec: 1, Location: loc},
		{FromSec: 200, ToSec: 300, StepSec: 1, Location: loc},
	}
	// one distinct row (distinct time) per LOD
	load := func(_ context.Context, _ *requestHandler, _ *queryBuilder, lod data_model.LOD, _ bool) ([][]tsSelectRow, error) {
		row := tsSelectRow{}
		if lod.FromSec == 100 {
			row.time = 150
		} else {
			row.time = 250
		}
		row.count = 1
		return [][]tsSelectRow{{row}}, nil
	}

	h := &requestHandler{Handler: &Handler{HandlerOptions: HandlerOptions{location: loc}}}
	rows, _, err := h.getTableFromLODs(context.Background(), lods, p, load)
	require.NoError(t, err)
	require.Len(t, rows, 2, "expected one row per LOD")

	// single what → every row must have exactly one Data column
	for _, r := range rows {
		require.Len(t, r.Data, 1, "row at time %d has misaligned Data columns", r.Time)
	}
}

package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
)

// Reproduces stale reads from pointsCache: cacheEntry.loadedAtNano is shared
// across time ranges of the same query, so loading range B "revalidates"
// range A's rows that were loaded before an invalidation.
func TestPointsCacheStaleAfterOtherRangeLoad(t *testing.T) {
	now := time.Now()
	nowF := func() time.Time { return now }
	generation := 0
	loader := func(_ context.Context, _ *requestHandler, _ *queryBuilder, _ data_model.LOD) ([]pSelectRow, error) {
		return []pSelectRow{{tsValues: tsValues{count: float64(generation)}}}, nil
	}
	c := newPointsCache(1000, 0, loader, nowF)
	h := &requestHandler{Handler: &Handler{}}
	q := &queryBuilder{}

	base := now.Unix() - 3600 // within the mutable 48h window
	lodA := data_model.LOD{FromSec: base, ToSec: base + 60}
	lodB := data_model.LOD{FromSec: base + 120, ToSec: base + 180}

	// load range A (generation 0)
	generation = 0
	rows, err := c.get(context.Background(), h, q, lodA, false)
	require.NoError(t, err)
	require.Equal(t, float64(0), rows[0].count)

	// data for a second inside range A changes; invalidation arrives
	now = now.Add(time.Second)
	c.invalidate([]int64{base + 30})
	generation = 1 // fresh data available from now on

	// immediately after invalidation, range A correctly misses cache
	_, ok := c.loadCached(q.getOrBuildCacheKey(), lodA.FromSec, lodA.ToSec)
	require.False(t, ok, "range A must be invalid right after invalidation")

	// more than invalidateLinger later, someone queries range B (same query key)
	now = now.Add(invalidateLinger + time.Second)
	_, err = c.get(context.Background(), h, q, lodB, false)
	require.NoError(t, err)

	// range A must still read as invalid: its rows were loaded before the
	// invalidation. Before the fix, B's load refreshed a shared loadedAtNano
	// and range A was served as fresh (returning pre-invalidation data).
	rows, ok = c.loadCached(q.getOrBuildCacheKey(), lodA.FromSec, lodA.ToSec)
	require.False(t, ok, "stale rows served: range A revalidated by range B's load, count=%v", rows[0].count)
}

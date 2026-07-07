package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
)

// Reproduces awaiter misdelivery: a loader that awaits a chunk being loaded
// by another loader, with a request range starting mid-chunk, receives no
// (or misplaced) data.
func TestCache2AwaiterOffset(t *testing.T) {
	h := &requestHandler{
		Handler: &Handler{
			HandlerOptions: HandlerOptions{
				location: time.Local,
			},
		},
	}
	firstLoadEntered := make(chan struct{})
	releaseFirstLoad := make(chan struct{})
	loadN := 0
	c := newCache2(h.Handler, 0, func(_ context.Context, _ *requestHandler, _ *queryBuilder, lod data_model.LOD, ret [][]tsSelectRow, _ int) (int, error) {
		loadN++
		if loadN == 1 {
			close(firstLoadEntered)
			<-releaseFirstLoad
		}
		for i := 0; i < len(ret); i++ {
			ret[i] = []tsSelectRow{{time: lod.FromSec + int64(i)*lod.StepSec}}
		}
		return len(ret), nil
	})
	defer c.shutdown()

	// chunk grid: 60s chunks at 1s step
	base := (time.Now().Unix() / 60) * 60
	lodA := data_model.LOD{Version: Version6, StepSec: 1, FromSec: base, ToSec: base + 60}
	lodB := data_model.LOD{Version: Version6, StepSec: 1, FromSec: base + 30, ToSec: base + 60}

	q := &queryBuilder{}
	resA := make(chan error, 1)
	go func() {
		_, err := c.Get(context.Background(), h, q, lodA, false)
		resA <- err
	}()
	<-firstLoadEntered // loader A is now loading chunk [base, base+60)

	resB := make(chan cache2Data, 1)
	errB := make(chan error, 1)
	go func() {
		data, err := c.Get(context.Background(), h, q, lodB, false)
		resB <- data
		errB <- err
	}()
	// wait until B registered itself as awaiter on the chunk
	require.Eventually(t, func() bool {
		shard := c.shards[time.Second]
		shard.mu.Lock()
		defer shard.mu.Unlock()
		for _, b := range shard.bucketM {
			b.mu.Lock()
			for _, chunk := range b.chunks {
				chunk.mu.Lock()
				n := len(chunk.awaiters)
				chunk.mu.Unlock()
				if n > 0 {
					b.mu.Unlock()
					return true
				}
			}
			b.mu.Unlock()
		}
		return false
	}, 5*time.Second, time.Millisecond)

	close(releaseFirstLoad)
	require.NoError(t, <-resA)
	data := <-resB
	require.NoError(t, <-errB)

	// B asked for [base+30, base+60): 30 slots, each must contain
	// exactly one row with the matching timestamp
	require.Len(t, data, 30)
	for i := 0; i < len(data); i++ {
		require.Len(t, data[i], 1, "slot %d has no data", i)
		require.Equal(t, lodB.FromSec+int64(i), data[i][0].time, "slot %d has wrong timestamp", i)
	}
}

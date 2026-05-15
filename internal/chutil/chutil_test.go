package chutil

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewConnPoolTimestamps(t *testing.T) {
	before := time.Now().UnixNano()
	pool := newConnPool("test", 10, 5, 3)
	after := time.Now().UnixNano()

	poolTS := pool.lastPoolQueryTimeNS.Load()
	require.GreaterOrEqual(t, poolTS, before, "lastPoolQueryTimeNS should be >= time before construction")
	require.LessOrEqual(t, poolTS, after, "lastPoolQueryTimeNS should be <= time after construction")

	require.Len(t, pool.lastShardQueryTimeNS, 3, "expected 3 shard timestamps")
	for i := range pool.lastShardQueryTimeNS {
		shardTS := pool.lastShardQueryTimeNS[i].Load()
		require.GreaterOrEqual(t, shardTS, before, "shard %d timestamp should be >= time before construction", i)
		require.LessOrEqual(t, shardTS, after, "shard %d timestamp should be <= time after construction", i)
	}
}

func TestRecordQueryDurationTimestamps(t *testing.T) {
	pool := newConnPool("test", 10, 5, 2)

	before := time.Now().UnixNano()
	pool.recordQueryDuration(100*time.Millisecond, -1)
	after := time.Now().UnixNano()

	poolTS := pool.lastPoolQueryTimeNS.Load()
	require.GreaterOrEqual(t, poolTS, before, "pool timestamp should be >= time before call")
	require.LessOrEqual(t, poolTS, after, "pool timestamp should be <= time after call")

	before = time.Now().UnixNano()
	pool.recordQueryDuration(100*time.Millisecond, 0)
	after = time.Now().UnixNano()

	shardTS := pool.lastShardQueryTimeNS[0].Load()
	require.GreaterOrEqual(t, shardTS, before, "shard 0 timestamp should be >= time before call")
	require.LessOrEqual(t, shardTS, after, "shard 0 timestamp should be <= time after call")

	shard1TS := pool.lastShardQueryTimeNS[1].Load()
	require.Less(t, shard1TS, before, "shard 1 timestamp should still be old value (not updated by shard 0 call)")
}

func TestDecayAvgNS(t *testing.T) {
	grace := time.Duration(avgDecayGracePeriodNS)
	halfLife := time.Duration(avgDecayHalfLifeNS)

	tests := []struct {
		name    string
		avgNS   int64
		elapsed time.Duration
		want    int64
	}{
		{
			name:    "zero_input",
			avgNS:   0,
			elapsed: 5 * time.Minute,
			want:    0,
		},
		{
			name:    "negative_input",
			avgNS:   -100,
			elapsed: 5 * time.Minute,
			want:    0,
		},
		{
			name:    "within_grace_period",
			avgNS:   1000,
			elapsed: 30 * time.Second,
			want:    1000,
		},
		{
			name:    "within_grace_period_near_boundary",
			avgNS:   1000,
			elapsed: 59 * time.Second,
			want:    1000,
		},
		{
			name:    "at_grace_boundary",
			avgNS:   1000,
			elapsed: grace,
			want:    1000,
		},
		{
			name:    "just_past_grace",
			avgNS:   1024,
			elapsed: grace + 1,
			want:    1023,
		},
		{
			name:    "one_half_life",
			avgNS:   1024,
			elapsed: grace + halfLife,
			want:    512,
		},
		{
			name:    "two_half_lives",
			avgNS:   1024,
			elapsed: grace + 2*halfLife,
			want:    256,
		},
		{
			name:    "three_half_lives",
			avgNS:   1024,
			elapsed: grace + 3*halfLife,
			want:    128,
		},
		{
			name:    "five_half_lives",
			avgNS:   1024,
			elapsed: grace + 5*halfLife,
			want:    32,
		},
		{
			name:    "ten_half_lives",
			avgNS:   1024,
			elapsed: grace + 10*halfLife,
			want:    1,
		},
		{
			name:    "eleven_half_lives_rounds_to_zero",
			avgNS:   1024,
			elapsed: grace + 11*halfLife,
			want:    0,
		},
		{
			name:    "thirty_half_lives_returns_zero",
			avgNS:   1 << 40,
			elapsed: grace + 60*halfLife,
			want:    0,
		},
		{
			name:    "large_value_single_half_life",
			avgNS:   1 << 30,
			elapsed: grace + halfLife,
			want:    1 << 29,
		},
		{
			name:    "odd_value_one_half_life",
			avgNS:   1001,
			elapsed: grace + halfLife,
			want:    500,
		},
		{
			name:    "odd_value_two_half_lives",
			avgNS:   1001,
			elapsed: grace + 2*halfLife,
			want:    250,
		},
		{
			name:    "one_returns_zero_after_one_half_life",
			avgNS:   1,
			elapsed: grace + halfLife,
			want:    0,
		},
		{
			name:    "elapsed_zero",
			avgNS:   500,
			elapsed: 0,
			want:    500,
		},
		{
			name:    "large_elapsed_60_half_lives",
			avgNS:   1 << 50,
			elapsed: grace + 60*halfLife,
			want:    0,
		},
		{
			name:    "partial_half_life",
			avgNS:   1024,
			elapsed: grace + halfLife/2,
			want:    int64(1024 * math.Pow(0.5, 0.5)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decayAvgNS(tt.avgNS, tt.elapsed)
			require.Equal(t, tt.want, got, "decayAvgNS(%d, %v)", tt.avgNS, tt.elapsed)
		})
	}
}

func TestGetAvgQueryDuration_Dispatch(t *testing.T) {
	pool := newConnPool("test", 10, 5, 2)
	ns := int64(4 * time.Second)
	now := time.Now().UnixNano()

	pool.avgQueryDurationNS.Store(ns)
	pool.lastPoolQueryTimeNS.Store(now)
	pool.shardAvgQueryNS[0].Store(ns * 2)
	pool.lastShardQueryTimeNS[0].Store(now)
	pool.shardAvgQueryNS[1].Store(ns * 3)
	pool.lastShardQueryTimeNS[1].Store(now)

	poolGot := pool.getAvgQueryDuration(-1)
	shard0Got := pool.getAvgQueryDuration(0)
	shard1Got := pool.getAvgQueryDuration(1)
	oobGot := pool.getAvgQueryDuration(99)

	require.Equal(t, time.Duration(ns), poolGot, "shard -1 should use pool average")
	require.Equal(t, time.Duration(ns*2), shard0Got, "shard 0 should use shard 0 average")
	require.Equal(t, time.Duration(ns*3), shard1Got, "shard 1 should use shard 1 average")
	require.Equal(t, time.Duration(ns), oobGot, "out-of-range shard should fall back to pool average")
}

func TestGetAvgQueryDuration_ZeroAverage(t *testing.T) {
	pool := newConnPool("test", 10, 5, 2)
	pool.avgQueryDurationNS.Store(0)
	pool.lastPoolQueryTimeNS.Store(time.Now().Add(-10 * time.Minute).UnixNano())

	got := pool.getAvgQueryDuration(-1)
	require.Equal(t, time.Duration(0), got, "zero pool average should return 0 regardless of timestamp")

	pool.shardAvgQueryNS[0].Store(0)
	pool.lastShardQueryTimeNS[0].Store(time.Now().Add(-10 * time.Minute).UnixNano())

	got = pool.getAvgQueryDuration(0)
	require.Equal(t, time.Duration(0), got, "zero shard average should return 0 regardless of timestamp")
}

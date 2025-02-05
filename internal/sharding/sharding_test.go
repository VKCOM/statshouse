package sharding

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

func TestShard(t *testing.T) {
	metric1 := data_model.Key{Timestamp: 1000, Metric: 1, Tags: [format.MaxTags]int32{1, 2, 3}}
	metric2 := data_model.Key{Timestamp: 1000, Metric: 2, Tags: [format.MaxTags]int32{5, 6}}
	metricBuiltin := data_model.Key{Timestamp: 1000, Metric: -1000}

	type args struct {
		key         data_model.Key
		meta        *format.MetricMetaValue
		numShards   int
		newSharding bool // renamed from builtinNewSharding to match new parameter name
	}
	tests := []struct {
		name          string
		args          args
		expectedShard uint32
	}{
		// Verify core sharding strategies
		{"ok-by-tags-hash-1", args{
			key:         metric1,
			meta:        &format.MetricMetaValue{ShardStrategy: format.ShardByTagsHash},
			numShards:   16,
			newSharding: true,
		}, 8},
		{"ok-by-tags-hash-2", args{
			key:         metric2,
			meta:        &format.MetricMetaValue{ShardStrategy: format.ShardByTagsHash},
			numShards:   16,
			newSharding: true,
		}, 15},
		{"ok-by-tags-builtin", args{
			key:         metricBuiltin,
			meta:        &format.MetricMetaValue{ShardStrategy: format.ShardByTagsHash},
			numShards:   16,
			newSharding: true,
		}, 5},

		{"ok-fixed-shard", args{
			key:         metric1,
			meta:        &format.MetricMetaValue{ShardStrategy: format.ShardFixed, ShardNum: 3},
			numShards:   16,
			newSharding: true,
		}, 3},
		{"ok-fixed-builtin", args{
			key:         metricBuiltin,
			meta:        &format.MetricMetaValue{ShardStrategy: format.ShardFixed, ShardNum: 0},
			numShards:   16,
			newSharding: true,
		}, 0},

		{"ok-by-metric-id", args{
			key:         metric1,
			meta:        &format.MetricMetaValue{ShardStrategy: format.ShardByMetric},
			numShards:   16,
			newSharding: true,
		}, 1},

		// Verify newSharding flag behavior
		{"new-sharding-false", args{
			key:         metric1,
			meta:        &format.MetricMetaValue{},
			numShards:   16,
			newSharding: false,
		}, 8},

		{"new-sharding-true-user-metric", args{
			key:         metric1,
			meta:        &format.MetricMetaValue{},
			numShards:   16,
			newSharding: true,
		}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotShard, newStrategy, gotHash := Shard(&tt.args.key, tt.args.meta, tt.args.numShards, uint32(tt.args.numShards), tt.args.newSharding)
			if gotShard != tt.expectedShard {
				t.Errorf("Sharding() = %v, want %v", gotShard, tt.expectedShard)
			}
			// hash calculated only for ShardByTagsHash
			if tt.args.meta.ShardStrategy == format.ShardByTagsHash || tt.args.meta.ShardStrategy == "" && !tt.args.newSharding {
				assert.False(t, newStrategy)
				assert.NotZero(t, gotHash) // this fails with 2^-64 probability
			} else {
				assert.True(t, newStrategy)
				assert.Zero(t, gotHash)
			}
		})
	}
}

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
		key               data_model.Key
		meta              *format.MetricMetaValue
		numShards         int
		newShardingByName string
		newStrategy       bool
	}
	tests := []struct {
		name          string
		args          args
		expectedShard uint32
	}{
		// Verify core sharding strategies
		{"ok-by-tags-hash-1", args{
			key:               metric1,
			meta:              &format.MetricMetaValue{Name: "a", ShardStrategy: format.ShardByTagsHash},
			numShards:         16,
			newShardingByName: "~",
			newStrategy:       false, // meta has priority
		}, 8},
		{"ok-by-tags-hash-2", args{
			key:               metric2,
			meta:              &format.MetricMetaValue{Name: "a", ShardStrategy: format.ShardByTagsHash},
			numShards:         16,
			newShardingByName: "~",
			newStrategy:       false, // meta has priority
		}, 15},
		{"ok-by-tags-builtin", args{
			key:               metricBuiltin,
			meta:              &format.MetricMetaValue{Name: "a", ShardStrategy: format.ShardByTagsHash},
			numShards:         16,
			newShardingByName: "~",
			newStrategy:       false, // meta has priority
		}, 5},

		{"ok-fixed-shard", args{
			key:               metric1,
			meta:              &format.MetricMetaValue{Name: "a", ShardStrategy: format.ShardFixed, ShardNum: 3},
			numShards:         16,
			newShardingByName: "~",
			newStrategy:       true,
		}, 3},
		{"ok-fixed-builtin", args{
			key:               metricBuiltin,
			meta:              &format.MetricMetaValue{Name: "a", ShardStrategy: format.ShardFixed, ShardNum: 0},
			numShards:         16,
			newShardingByName: "~",
			newStrategy:       true,
		}, 0},

		{"ok-by-metric-id", args{
			key:               metric1,
			meta:              &format.MetricMetaValue{Name: "a", ShardStrategy: format.ShardByMetric},
			numShards:         16,
			newShardingByName: "~",
			newStrategy:       true,
		}, 1},

		// Verify newSharding flag behavior
		{"new-sharding-false", args{
			key:         metric1,
			meta:        &format.MetricMetaValue{Name: "a"},
			numShards:   16,
			newStrategy: false,
		}, 8},

		{"new-sharding-true-user-metric", args{
			key:               metric1,
			meta:              &format.MetricMetaValue{Name: "a"},
			numShards:         16,
			newShardingByName: "~",
			newStrategy:       true,
		}, 1},

		{"builtin-sharding-by-name-enabled", args{
			key:               metric1,
			meta:              &format.MetricMetaValue{Name: "__a"},
			numShards:         16,
			newShardingByName: "a",
			newStrategy:       true,
		}, 1},
		{"user-sharding-by-name-enabled", args{
			key:               metric1,
			meta:              &format.MetricMetaValue{Name: "a"},
			numShards:         16,
			newShardingByName: "a",
			newStrategy:       true,
		}, 1},
		{"user-sharding-by-name-disabled", args{
			key:               metric1,
			meta:              &format.MetricMetaValue{Name: "b"},
			numShards:         16,
			newShardingByName: "a",
			newStrategy:       false,
		}, 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotShard, newStrategy, _, gotHash := Shard(&tt.args.key, tt.args.meta, tt.args.numShards, uint32(tt.args.numShards), tt.args.newShardingByName)
			if gotShard != tt.expectedShard {
				t.Errorf("Sharding() = %v, want %v", gotShard, tt.expectedShard)
			}
			assert.Equal(t, tt.args.newStrategy, newStrategy)
			if newStrategy {
				assert.Zero(t, gotHash)
			} else {
				assert.NotZero(t, gotHash)
			}
		})
	}
}

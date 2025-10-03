package sharding

import (
	"testing"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
)

func TestShard(t *testing.T) {
	metric1 := data_model.Key{Timestamp: 1000, Metric: 1, Tags: [format.MaxTags]int32{1, 2, 3}}
	metric2 := data_model.Key{Timestamp: 1000, Metric: 2, Tags: [format.MaxTags]int32{5, 6}}
	metricBuiltin := data_model.Key{Timestamp: 1000, Metric: -1000}

	type args struct {
		key       data_model.Key
		meta      *format.MetricMetaValue
		numShards int
	}
	tests := []struct {
		name          string
		args          args
		expectedShard uint32
	}{
		// Verify core sharding strategies
		{"ok-by-tags-hash-1", args{
			key:       metric1,
			meta:      &format.MetricMetaValue{Name: "a", ShardStrategy: format.ShardByTagsHash},
			numShards: 16,
		}, 13},
		{"ok-by-tags-hash-2", args{
			key:       metric2,
			meta:      &format.MetricMetaValue{Name: "a", ShardStrategy: format.ShardByTagsHash},
			numShards: 16,
		}, 1},
		{"ok-by-tags-builtin", args{
			key:       metricBuiltin,
			meta:      &format.MetricMetaValue{Name: "a", ShardStrategy: format.ShardByTagsHash},
			numShards: 16,
		}, 11},

		{"ok-fixed-shard", args{
			key:       metric1,
			meta:      &format.MetricMetaValue{Name: "a", ShardStrategy: format.ShardFixed, ShardNum: 3},
			numShards: 16,
		}, 3},
		{"ok-fixed-builtin", args{
			key:       metricBuiltin,
			meta:      &format.MetricMetaValue{Name: "a", ShardStrategy: format.ShardFixed, ShardNum: 0},
			numShards: 16,
		}, 0},

		{"ok-by-metric-id", args{
			key:       metric1,
			meta:      &format.MetricMetaValue{Name: "a", ShardStrategy: format.ShardByMetric},
			numShards: 16,
		}, 1},

		{"new-sharding-true-user-metric", args{
			key:       metric1,
			meta:      &format.MetricMetaValue{Name: "a"},
			numShards: 16,
		}, 1},
	}

	scratch := make([]byte, 0)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotShard := Shard(&tt.args.key, tt.args.meta, tt.args.numShards, uint32(tt.args.numShards), &scratch)
			if gotShard != tt.expectedShard {
				t.Errorf("Sharding() = %v, want %v", gotShard, tt.expectedShard)
			}
		})
	}
}

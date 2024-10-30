package sharding

import (
	"testing"

	"github.com/mailru/easyjson/opt"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

func TestShard(t *testing.T) {
	metric1 := data_model.Key{Timestamp: 1000, Metric: 1, Tags: [format.MaxTags]int32{1, 2, 3}}
	metric2 := data_model.Key{Timestamp: 1000, Metric: 2, Tags: [format.MaxTags]int32{5, 6}}
	metricBuiltin := data_model.Key{Timestamp: 1000, Metric: -1000}
	pastTs := uint32(900)
	futureTs := uint32(1100)

	type args struct {
		key                data_model.Key
		meta               *format.MetricMetaValue
		numShards          int
		builtinNewSharding bool
	}
	tests := []struct {
		name             string
		args             args
		expectedShard    uint32
		expectedStrategy int
		wantErr          bool
	}{
		// be careful, if this tests are failing then we changed shardig schema
		{"ok-by-tags-1", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardBy16MappedTagsHash}}}, numShards: 16, builtinNewSharding: true}, 8, format.ShardBy16MappedTagsHashId, false},
		{"ok-by-tags-2", args{key: metric2, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardBy16MappedTagsHash}}}, numShards: 16, builtinNewSharding: true}, 15, format.ShardBy16MappedTagsHashId, false},
		{"ok-by-tags-builtin", args{key: metricBuiltin, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardBy16MappedTagsHash}}}, numShards: 16, builtinNewSharding: true}, 5, format.ShardBy16MappedTagsHashId, false},

		{"ok-multiple-after", args{key: metricBuiltin, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{
			{Strategy: format.ShardBy16MappedTagsHash},
			{Strategy: format.ShardFixed, Shard: opt.OUint32(0), AfterTs: opt.OUint32(futureTs)},
		}}, numShards: 16, builtinNewSharding: true}, 5, format.ShardBy16MappedTagsHashId, false},
		{"ok-multiple-before", args{key: metricBuiltin, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{
			{Strategy: format.ShardFixed, Shard: opt.OUint32(0)},
			{Strategy: format.ShardBy16MappedTagsHash, AfterTs: opt.OUint32(pastTs)},
		}}, numShards: 16, builtinNewSharding: true}, 5, format.ShardBy16MappedTagsHashId, false},
		{"ok-multiple-no-ts", args{key: metricBuiltin, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{
			{Strategy: format.ShardFixed, Shard: opt.OUint32(0)},
			{Strategy: format.ShardBy16MappedTagsHash},
		}}, numShards: 16, builtinNewSharding: true}, 5, format.ShardBy16MappedTagsHashId, false},

		{"ok-fixed-1", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardFixed, Shard: opt.OUint32(3)}}}, numShards: 16, builtinNewSharding: true}, 3, format.ShardFixedId, false},
		{"ok-fixed-2", args{key: metric2, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardFixed, Shard: opt.OUint32(4)}}}, numShards: 16, builtinNewSharding: true}, 4, format.ShardFixedId, false},
		{"ok-fixed-builtin", args{key: metricBuiltin, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardFixed, Shard: opt.OUint32(0)}}}, numShards: 16, builtinNewSharding: true}, 0, format.ShardFixedId, false},

		{"ok-by-tag-1", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardByTag, TagId: opt.OUint32(1)}}}, numShards: 16, builtinNewSharding: true}, 2, format.ShardByTagId, false},
		{"ok-by-tag-2", args{key: metric2, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardByTag, TagId: opt.OUint32(1)}}}, numShards: 16, builtinNewSharding: true}, 6, format.ShardByTagId, false},
		{"ok-by-tag-builtin", args{key: metricBuiltin, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardByTag, TagId: opt.OUint32(0)}}}, numShards: 16, builtinNewSharding: true}, 0, format.ShardByTagId, false},

		{"ok-by-metric-id", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardByMetric}}}, numShards: 16, builtinNewSharding: true}, 1, format.ShardByMetricId, false},

		{"builing-flag-builtin", args{key: metricBuiltin, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardFixed, Shard: opt.OUint32(0)}}}, numShards: 16, builtinNewSharding: false}, 5, format.ShardBy16MappedTagsHashId, false},
		{"builing-flag-user-metric", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardFixed, Shard: opt.OUint32(3)}}}, numShards: 16, builtinNewSharding: false}, 3, format.ShardFixedId, false},

		// negative cases
		{"fail-fixed-no-shard", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardFixed}}}, numShards: 16}, 0, -1, true},
		{"fail-fixed-bad-shard", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardFixed, Shard: opt.OUint32(1000)}}}, numShards: 16}, 0, -1, true},
		{"fail-by-tag-no-id", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardByTag}}}, numShards: 16}, 0, -1, true},
		{"fail-by-tag-bad-id", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardByTag, TagId: opt.OUint32(1000)}}}, numShards: 16}, 0, -1, true},
	}
	for _, tt := range tests {
		for i, sh := range tt.args.meta.Sharding {
			tt.args.meta.Sharding[i].StrategyId, _ = format.ShardingStrategyId(sh)
		}
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotShard, gotStrategy, err := Shard(tt.args.key, tt.args.key.Hash(), tt.args.meta, tt.args.numShards, tt.args.builtinNewSharding)
			if (err != nil) != tt.wantErr {
				t.Errorf("Shard() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotShard != tt.expectedShard {
				t.Errorf("Shard() = %v, want %v", gotShard, tt.expectedShard)
			}
			if gotStrategy != tt.expectedStrategy {
				t.Errorf("Shard() = %v, want %v", gotStrategy, tt.expectedStrategy)
			}
		})
	}
}

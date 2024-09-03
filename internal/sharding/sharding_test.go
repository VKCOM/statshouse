package sharding

import (
	"testing"

	"github.com/mailru/easyjson/opt"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

func TestShard(t *testing.T) {
	metric1 := data_model.Key{Timestamp: 1000, Metric: 1, Keys: [format.MaxTags]int32{1, 2, 3}}
	metric2 := data_model.Key{Timestamp: 1000, Metric: 2, Keys: [format.MaxTags]int32{5, 6}}
	metricBuiltin := data_model.Key{Timestamp: 1000, Metric: -1000}
	pastTs := uint32(900)
	futureTs := uint32(1100)

	type args struct {
		key       data_model.Key
		meta      *format.MetricMetaValue
		numShards int
	}
	tests := []struct {
		name             string
		args             args
		expectedShard    uint32
		expectedStrategy string
		wantErr          bool
	}{
		// be careful, if this tests are failing then we changed shardig schema
		{"ok-by-tags-1", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardBy16MappedTagsHash}}}, numShards: 16}, 8, format.ShardBy16MappedTagsHash, false},
		{"ok-by-tags-2", args{key: metric2, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardBy16MappedTagsHash}}}, numShards: 16}, 15, format.ShardBy16MappedTagsHash, false},
		{"ok-by-tags-builtin", args{key: metricBuiltin, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardBy16MappedTagsHash}}}, numShards: 16}, 5, format.ShardBy16MappedTagsHash, false},

		{"ok-multiple-after", args{key: metricBuiltin, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{
			{Strategy: format.ShardBy16MappedTagsHash},
			{Strategy: format.ShardFixed, Shard: opt.OUint32(0), AfterTs: opt.OUint32(futureTs)},
		}}, numShards: 16}, 5, format.ShardBy16MappedTagsHash, false},
		{"ok-multiple-before", args{key: metricBuiltin, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{
			{Strategy: format.ShardFixed, Shard: opt.OUint32(0)},
			{Strategy: format.ShardBy16MappedTagsHash, AfterTs: opt.OUint32(pastTs)},
		}}, numShards: 16}, 5, format.ShardBy16MappedTagsHash, false},
		{"ok-multiple-no-ts", args{key: metricBuiltin, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{
			{Strategy: format.ShardFixed, Shard: opt.OUint32(0)},
			{Strategy: format.ShardBy16MappedTagsHash},
		}}, numShards: 16}, 5, format.ShardBy16MappedTagsHash, false},

		{"ok-fixed-1", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardFixed, Shard: opt.OUint32(3)}}}, numShards: 16}, 3, format.ShardFixed, false},
		{"ok-fixed-2", args{key: metric2, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardFixed, Shard: opt.OUint32(4)}}}, numShards: 16}, 4, format.ShardFixed, false},
		{"ok-fixed-builtin", args{key: metricBuiltin, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardFixed, Shard: opt.OUint32(0)}}}, numShards: 16}, 0, format.ShardFixed, false},

		{"ok-by-tag-1", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardByTag, TagId: opt.OUint32(1)}}}, numShards: 16}, 2, format.ShardByTag, false},
		{"ok-by-tag-2", args{key: metric2, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardByTag, TagId: opt.OUint32(1)}}}, numShards: 16}, 6, format.ShardByTag, false},
		{"ok-by-tag-builtin", args{key: metricBuiltin, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardByTag, TagId: opt.OUint32(0)}}}, numShards: 16}, 0, format.ShardByTag, false},

		// negative cases
		{"fail-fixed-no-shard", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardFixed}}}, numShards: 16}, 0, "", true},
		{"fail-fixed-bad-shard", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardFixed, Shard: opt.OUint32(1000)}}}, numShards: 16}, 0, "", true},
		{"fail-by-tag-no-id", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardByTag}}}, numShards: 16}, 0, "", true},
		{"fail-by-tag-bad-id", args{key: metric1, meta: &format.MetricMetaValue{Sharding: []format.MetricSharding{{Strategy: format.ShardByTag, TagId: opt.OUint32(1000)}}}, numShards: 16}, 0, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotShard, gotStrategy, err := Shard(tt.args.key, tt.args.meta, tt.args.numShards, false)
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

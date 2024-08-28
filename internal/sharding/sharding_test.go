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

	type args struct {
		key       data_model.Key
		sharding  format.MetricSharding
		numShards int
	}
	tests := []struct {
		name    string
		args    args
		want    uint32
		wantErr bool
	}{
		// be careful, if this tests are failing then we changed shardig schema
		{"ok-by-tags-1", args{key: metric1, sharding: format.MetricSharding{Strategy: format.MappedTags}, numShards: 16}, 8, false},
		{"ok-by-tags-2", args{key: metric2, sharding: format.MetricSharding{Strategy: format.MappedTags}, numShards: 16}, 15, false},
		{"ok-by-tags-builtin", args{key: metricBuiltin, sharding: format.MetricSharding{Strategy: format.MappedTags}, numShards: 16}, 5, false},

		{"ok-fixed-1", args{key: metric1, sharding: format.MetricSharding{Strategy: format.FixedShard, Shard: opt.OUint32(3)}, numShards: 16}, 3, false},
		{"ok-fixed-2", args{key: metric2, sharding: format.MetricSharding{Strategy: format.FixedShard, Shard: opt.OUint32(4)}, numShards: 16}, 4, false},
		{"ok-fixed-builtin", args{key: metricBuiltin, sharding: format.MetricSharding{Strategy: format.FixedShard, Shard: opt.OUint32(0)}, numShards: 16}, 0, false},

		{"ok-by-tag-1", args{key: metric1, sharding: format.MetricSharding{Strategy: format.Tag, TagId: opt.OUint32(1)}, numShards: 16}, 2, false},
		{"ok-by-tag-2", args{key: metric2, sharding: format.MetricSharding{Strategy: format.Tag, TagId: opt.OUint32(1)}, numShards: 16}, 6, false},
		{"ok-by-tag-builtin", args{key: metricBuiltin, sharding: format.MetricSharding{Strategy: format.Tag, TagId: opt.OUint32(0)}, numShards: 16}, 0, false},

		// negative cases
		{"fail-fixed-no-shard", args{key: metric1, sharding: format.MetricSharding{Strategy: format.FixedShard}, numShards: 16}, 0, true},
		{"fail-fixed-bad-shard", args{key: metric1, sharding: format.MetricSharding{Strategy: format.FixedShard, Shard: opt.OUint32(1000)}, numShards: 16}, 0, true},
		{"fail-by-tag-no-id", args{key: metric1, sharding: format.MetricSharding{Strategy: format.Tag}, numShards: 16}, 0, true},
		{"fail-by-tag-bad-id", args{key: metric1, sharding: format.MetricSharding{Strategy: format.Tag, TagId: opt.OUint32(1000)}, numShards: 16}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Shard(tt.args.key, tt.args.sharding, tt.args.numShards)
			if (err != nil) != tt.wantErr {
				t.Errorf("Shard() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Shard() = %v, want %v", got, tt.want)
			}
		})
	}
}

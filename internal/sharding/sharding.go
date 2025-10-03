package sharding

import (
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
)

// legacyKeyHash will be 0 for all new sharding strategies
func Shard(key *data_model.Key, meta *format.MetricMetaValue, numShards int, shardByMetricCount uint32, scratch *[]byte) (shardID uint32, weightMul int) {
	switch meta.ShardStrategy {
	case format.ShardFixed:
		return meta.ShardNum, numShards
	case "", format.ShardByMetric:
		shard := uint32(key.Metric) % shardByMetricCount
		return shard, numShards
	case format.ShardBuiltin:
		tagId := meta.MetricTagID
		// for builtin metrics we always use row values
		metric := key.Tags[tagId]
		shard := uint32(metric) % shardByMetricCount
		return shard, numShards
	default: // including format.ShardByTagsHsh
		var scr []byte
		if scratch != nil {
			scr = *scratch
		}
		var legacyKeyHash uint64
		scr, legacyKeyHash = key.XXHash(scr)
		if scratch != nil {
			*scratch = scr
		}
		shard := shardByMappedTags(legacyKeyHash, numShards)
		return shard, 1
	}
}

func shardByMappedTags(keyHash uint64, numShards int) uint32 {
	mul := (keyHash >> 32) * uint64(numShards) >> 32 // trunc([0..0.9999999] * numShards) in fixed point 32.32
	return uint32(mul)
}

package sharding

import (
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

// legacyKeyHash will be 0 for all new sharding strategies
func Shard(key *data_model.Key, meta *format.MetricMetaValue, numShards int, shardByMetricCount uint32, newShardingByName string) (shardID uint32, newStrategy bool, weightMul int, legacyKeyHash uint64) {
	s := meta.ShardStrategy
	if s == "" {
		if meta.Name <= newShardingByName {
			s = format.ShardByMetric
		} else {
			s = format.ShardByTagsHash
		}
	}

	switch s {
	case format.ShardFixed:
		return meta.ShardNum, true, numShards, 0
	case format.ShardByMetric:
		shard := uint32(key.Metric) % shardByMetricCount
		return shard, true, numShards, 0
	default: // including format.ShardByTagsHsh
		legacyKeyHash = key.Hash()
		shard := shardByMappedTags(legacyKeyHash, numShards)
		return shard, false, 1, legacyKeyHash
	}
}

func shardByMappedTags(keyHash uint64, numShards int) uint32 {
	mul := (keyHash >> 32) * uint64(numShards) >> 32 // trunc([0..0.9999999] * numShards) in fixed point 32.32
	return uint32(mul)
}

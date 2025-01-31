package sharding

import (
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

// legacyKeyHash will be 0 for all new sharding strategies
func Shard(key *data_model.Key, meta *format.MetricMetaValue, numShards int, newSharding bool) (shardID uint32, newStrategy bool, legacyKeyHash uint64) {
	sharding := meta.Sharding
	if sharding.Strategy == "" {
		if newSharding {
			sharding.Strategy = format.ShardByMetric
		} else {
			sharding.Strategy = format.ShardByTagsHash
		}
	}

	switch sharding.Strategy {
	case format.ShardFixed:
		return sharding.Shard, true, 0
	case format.ShardByMetric:
		shard := uint32(key.Metric) % uint32(numShards)
		return shard, true, 0
	default: // including format.ShardByTagsHsh
		legacyKeyHash = key.Hash()
		shard := shardByMappedTags(legacyKeyHash, numShards)
		return shard, false, legacyKeyHash
	}
}

func shardByMappedTags(keyHash uint64, numShards int) uint32 {
	mul := (keyHash >> 32) * uint64(numShards) >> 32 // trunc([0..0.9999999] * numShards) in fixed point 32.32
	return uint32(mul)
}

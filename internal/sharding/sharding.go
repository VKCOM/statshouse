package sharding

import (
	"fmt"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

func Shard(key data_model.Key, sharding format.MetricSharding, numShards int) (uint32, error) {
	switch sharding.Strategy {
	case format.ShardByFixedShard:
		if !sharding.Shard.IsDefined() {
			return 0, fmt.Errorf("invalid sharding config: shard is not defined")
		}
		if sharding.Shard.V >= uint32(numShards) {
			return 0, fmt.Errorf("invalid sharding config: shard >= numShards")
		}
		return sharding.Shard.V, nil
	case format.ShardByMappedTags:
		return shardByMappedTags(key, numShards), nil
	case format.ShardByTag:
		if !sharding.TagId.IsDefined() {
			return 0, fmt.Errorf("invalid sharding config: tag_id is not defined")
		}
		if sharding.TagId.V >= format.MaxTags {
			return 0, fmt.Errorf("invalid sharding config: tag_id >= MaxTags")
		}
		return shardByTag(key, sharding.TagId.V, numShards), nil
	}
	return 0, fmt.Errorf("invalid sharding config: unknown strategy")
}

func shardByMappedTags(key data_model.Key, numShards int) uint32 {
	hash := key.Hash()
	mul := (hash >> 32) * uint64(numShards) >> 32 // trunc([0..0.9999999] * numShards) in fixed point 32.32
	return uint32(mul)
}

func shardByTag(key data_model.Key, tagId uint32, numShards int) uint32 {
	return uint32(key.Keys[tagId]) % uint32(numShards)
}

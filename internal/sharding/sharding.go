package sharding

import (
	"fmt"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

func Shard(key *data_model.Key, meta *format.MetricMetaValue, numShards int, builtinNewSharding bool) (shardID uint32, strategy int, legacyKeyHash uint64, err error) {
	sh := choseShardingStrategy(key, meta)
	if key.Metric < 0 && !builtinNewSharding {
		// fallback to legacy format
		sh = format.MetricSharding{Strategy: format.ShardBy16MappedTagsHash, StrategyId: format.ShardBy16MappedTagsHashId}
	}

	switch sh.StrategyId {
	case format.ShardFixedId:
		if !sh.Shard.IsDefined() {
			return 0, -1, 0, fmt.Errorf("invalid sharding config: shard is not defined")
		}
		if sh.Shard.V >= uint32(numShards) {
			return 0, -1, 0, fmt.Errorf("invalid sharding config: shard >= numShards")
		}
		return sh.Shard.V, sh.StrategyId, 0, nil
	case format.ShardBy16MappedTagsHashId:
		legacyHash = key.Hash()
		shard := shardByMappedTags(legacyHash, numShards)
		return shard, sh.StrategyId, legacyHash, nil
	case format.ShardByTagId:
		if !sh.TagId.IsDefined() {
			return 0, -1, 0, fmt.Errorf("invalid sharding config: tag_id is not defined")
		}
		if sh.TagId.V >= format.MaxTags {
			return 0, -1, 0, fmt.Errorf("invalid sharding config: tag_id >= MaxTags")
		}
		return shardByTag(key, sh.TagId.V, numShards), sh.StrategyId, 0, nil
	case format.ShardByMetricId:
		return shardByMetricId(key, numShards), sh.StrategyId, 0, nil
	}
	return 0, -1, 0, fmt.Errorf("invalid sharding config: unknown strategy")
}

func shardByMappedTags(keyHash uint64, numShards int) uint32 {
	mul := (keyHash >> 32) * uint64(numShards) >> 32 // trunc([0..0.9999999] * numShards) in fixed point 32.32
	return uint32(mul)
}

func shardByTag(key *data_model.Key, tagId uint32, numShards int) uint32 {
	return uint32(key.Tags[tagId]) % uint32(numShards)
}

func shardByMetricId(key *data_model.Key, numShards int) uint32 {
	return uint32(key.Metric) % uint32(numShards)
}

func choseShardingStrategy(key *data_model.Key, meta *format.MetricMetaValue) (sh format.MetricSharding) {
	if len(meta.Sharding) == 0 {
		return format.MetricSharding{Strategy: format.ShardBy16MappedTagsHash, StrategyId: format.ShardBy16MappedTagsHashId}
	}
	ts := key.Timestamp
	if ts == 0 {
		ts = uint32(time.Now().Unix())
	}
	for i := len(meta.Sharding) - 1; i >= 0; i-- {
		sh = meta.Sharding[i]
		if !sh.AfterTs.IsDefined() || sh.AfterTs.V < ts {
			break
		}
	}
	return sh
}

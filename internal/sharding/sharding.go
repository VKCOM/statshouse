package sharding

import (
	"fmt"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

func Shard(key data_model.Key, meta *format.MetricMetaValue, numShards int) (uint32, string, error) {
	if len(meta.Sharding) == 0 {
		return 0, "", fmt.Errorf("bad metric meta, no sharding defined")
	}
	ts := key.Timestamp
	if ts == 0 {
		ts = uint32(time.Now().Unix())
	}
	var sh format.MetricSharding
	for i := len(meta.Sharding) - 1; i >= 0; i-- {
		sh = meta.Sharding[i]
		if !sh.AfterTs.IsDefined() || sh.AfterTs.V < ts {
			break
		}
	}

	switch sh.Strategy {
	case format.ShardFixed:
		if !sh.Shard.IsDefined() {
			return 0, "", fmt.Errorf("invalid sharding config: shard is not defined")
		}
		if sh.Shard.V >= uint32(numShards) {
			return 0, "", fmt.Errorf("invalid sharding config: shard >= numShards")
		}
		return sh.Shard.V, sh.Strategy, nil
	case format.ShardBy16MappedTagsHash:
		return shardByMappedTags(key, numShards), sh.Strategy, nil
	case format.ShardByTag:
		if !sh.TagId.IsDefined() {
			return 0, "", fmt.Errorf("invalid sharding config: tag_id is not defined")
		}
		if sh.TagId.V >= format.MaxTags {
			return 0, "", fmt.Errorf("invalid sharding config: tag_id >= MaxTags")
		}
		return shardByTag(key, sh.TagId.V, numShards), sh.Strategy, nil
	}
	return 0, "", fmt.Errorf("invalid sharding config: unknown strategy")
}

func shardByMappedTags(key data_model.Key, numShards int) uint32 {
	hash := key.Hash()
	mul := (hash >> 32) * uint64(numShards) >> 32 // trunc([0..0.9999999] * numShards) in fixed point 32.32
	return uint32(mul)
}

func shardByTag(key data_model.Key, tagId uint32, numShards int) uint32 {
	return uint32(key.Keys[tagId]) % uint32(numShards)
}

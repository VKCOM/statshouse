package sharding

import (
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
)

// shard overflow is checked by the caller
func Shard(key *data_model.Key, meta *format.MetricMetaValue, shardByMetricCount uint32, scratch *[]byte) (shardNum uint32, ok bool) {
	switch meta.ShardStrategy {
	case format.ShardFixed:
		return meta.ShardNum, true
	case format.ShardByMetricID:
		shard := uint32(key.Metric) % shardByMetricCount
		return shard, true
	case format.ShardByTagsHash:
		var scr []byte
		if scratch != nil {
			scr = *scratch
		}
		var legacyKeyHash uint64
		scr, legacyKeyHash = key.XXHash(scr)
		if scratch != nil {
			*scratch = scr
		}
		shard := shardByMappedTags(legacyKeyHash, shardByMetricCount)
		return shard, true
	default: // including format.ShardBuiltinDist, which should be written directly into insert batch data, so never by this function
		return 0, false
	}
}

func shardByMappedTags(keyHash uint64, numShards uint32) uint32 {
	mul := (keyHash >> 32) * uint64(numShards) >> 32 // trunc([0..0.9999999] * numShards) in fixed point 32.32
	return uint32(mul)
}

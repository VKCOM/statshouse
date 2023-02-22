// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"errors"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

func TillStartOfNextSecond(now time.Time) time.Duration { // helper, TODO - move somewhere
	return now.Truncate(time.Second).Add(time.Second).Sub(now)
}

const (
	// Aggregator aggregates data in shards
	// Clients pre calculate shard ID and puts into lower byte of metricID in TL
	// Also clients shuffle shards, so that aggregator is less likely to stop on shard mutex
	// LogAggregationShardsPerSecond should be 8 (Cannot be larger, no point in being smaller)
	LogAggregationShardsPerSecond = 8
	AggregationShardsPerSecond    = 1 << LogAggregationShardsPerSecond

	MaxHistorySendStreams      = 10      // Do not change, unless you understand how exactly new conveyor works
	MaxHistoryInsertBatch      = 4       // Should be < MaxHistorySendStreams/2 so that we have enough historic buckets received when we finish previous insert
	HistoryInsertBodySizeLimit = 1 << 20 // We will batch several historic buckets together if they fit in this limit

	MaxLivenessResponsesWindowLength = 60
	MaxHistoricBucketsMemorySize     = 50 << 20

	MaxUncompressedBucketSize = 10 << 20 // limits memory for uncompressed data buffer in aggregator, Still dangerous

	MinStringTopCapacity        = 20
	MinStringTopSend            = 5
	MinStringTopInsert          = 5
	AggregatorStringTopCapacity = 1000 // big, but reasonable

	AgentPercentileCompression      = 40 // TODO - will typically have 20-30 centroids for compression 40
	AggregatorPercentileCompression = 80 // TODO - clickhouse has compression of 256 by default

	MaxShortWindow       = 5     // Must be >= 2, 5 seconds to send recent data, if too late - send as historic
	FutureWindow         = 2     // Allow a couple of seconds clocks difference on clients
	MaxHistoricWindow    = 86400 // 1 day to send historic data, then drop
	MaxHistoricWindowLag = 100   // Clients try to delete old data themselves, we allow some lag for those who already sent us data

	BelieveTimestampWindow = 3600 * 3 / 2 // Hour plus some margin, for crons running once per hour
	// TODO - in the next release, increase to 24 hours plus margin. Parts are quickly merged, so all timestamps in
	// [-day..0] will be quickly and thoroughly optimized.

	MinCardinalityWindow = 300 // Our estimators GC depends on this not being too small
	MinMaxCardinality    = 100

	InsertBudgetFixed = 50000
	// fixed budget for BuiltinMetricIDAggKeepAlive and potentially other metrics which can be added with 0 contributors
	// Also helps when # of contributors is very small

	AgentAggregatorDelay = 5  // Typical max
	InsertDelay          = 10 // Typical max

	MaxConveyorDelay = MaxShortWindow + FutureWindow + InsertDelay + AgentAggregatorDelay

	MaxMissedSecondsIntoContributors = 60 // If source sends more MissedSeconds, they will be truncated. Do not make large. We scan 4 arrays of this size on each insert.

	ClientRPCPongTimeout = 30 * time.Second

	AgentMappingTimeout1 = 10 * time.Second
	AgentMappingTimeout2 = 30 * time.Second
	AutoConfigTimeout    = 30 * time.Second

	MaxJournalItemsSent = 1000 // TODO - increase, but limit response size in bytes
	MaxJournalBytesSent = 800 * 1024

	ClickHouseTimeoutConfig   = time.Second * 10 // either quickly autoconfig or quickly exit
	ClickhouseConfigRetries   = 5
	ClickHouseTimeout         = time.Minute
	ClickHouseTimeoutHistoric = 5 * time.Minute // reduces chance of duplicates via historic conveyor
	NoHistoricBucketsDelay    = 3 * time.Second
	ClickHouseErrorDelay      = 10 * time.Second

	KeepAliveMaxBackoff          = 30 * time.Second // for cases when aggregators quickly return error
	JournalDDOSProtectionTimeout = 50 * time.Millisecond

	InternalLogInsertInterval = 5 * time.Second

	RPCErrorMissedRecentConveyor   = -5001 // just send again through historic
	RPCErrorInsertRecentConveyor   = -5002 // just send again through historic
	RPCErrorInsertHistoricConveyor = -5003 // just send again after delay
	RPCErrorNoAutoCreate           = -5004 // just live with it, this is normal
	RPCErrorTerminateLongpoll      = -5005 // we terminate long polls because rpc.Server does not inform us about hctx disconnects. TODO - remove as soon as Server is updated
	RPCErrorScrapeAgentIP          = -5006 // scrape agent must have IP address

	JournalDiskNamespace        = "metric_journal_v5:"
	TagValueDiskNamespace       = "tag_value_v3:"
	TagValueInvertDiskNamespace = "tag_value_invert_v3:"
	BootstrapDiskNamespace      = "bootstrap:" // stored in aggregator only

	MappingMaxMetricsInQueue = 1000
	MappingMaxMemCacheSize   = 2_000_000
	MappingCacheTTLMinimum   = 7 * 24 * time.Hour
	MappingNegativeCacheTTL  = 5 * time.Second
	MappingMinInterval       = 1 * time.Millisecond

	SimulatorMetricPrefix = "simulator_metric_"
)

func NextBackoffDuration(backoffTimeout time.Duration) time.Duration {
	if backoffTimeout < 0 {
		backoffTimeout = 0
	}
	backoffTimeout = (backoffTimeout + time.Second) * 5 / 4 // exact curve shape is not important
	if backoffTimeout > KeepAliveMaxBackoff {
		backoffTimeout = KeepAliveMaxBackoff
	}
	return backoffTimeout
}

// those can seriously fill our logs, we want to avoid it in a consistent manner
func SilentRPCError(err error) bool {
	var rpcError rpc.Error
	if !errors.As(err, &rpcError) {
		return false
	}
	switch rpcError.Code {
	case RPCErrorMissedRecentConveyor, RPCErrorInsertRecentConveyor,
		RPCErrorInsertHistoricConveyor, RPCErrorNoAutoCreate, RPCErrorTerminateLongpoll:
		return true
	default:
		return false
	}
}

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package format

const (
	TagValueIDBadgeAgentSamplingFactor = -1
	TagValueIDBadgeAggSamplingFactor   = -10
	TagValueIDBadgeIngestionErrors     = 1
	TagValueIDBadgeAggMappingErrors    = 2
	TagValueIDBadgeContributors        = 3 // # of agents who sent this second. Hyper important to distinguish between holes in your data and problems with agents (no connectivity, etc.).
	TagValueIDBadgeIngestionWarnings   = 4

	TagValueIDRPCRequestsStatusOK          = 1
	TagValueIDRPCRequestsStatusErrLocal    = 2
	TagValueIDRPCRequestsStatusErrUpstream = 3
	TagValueIDRPCRequestsStatusHijack      = 4
	TagValueIDRPCRequestsStatusNoHandler   = 5
	TagValueIDRPCRequestsStatusErrCancel   = 6 // on proxy, agent request was cancelled before response from aggregator arrived

	TagValueIDProduction  = 1
	TagValueIDStaging1    = 2
	TagValueIDStaging2    = 3
	TagValueIDDevelopment = 4

	TagValueIDAPILaneFastLight    = 1
	TagValueIDAPILaneFastHeavy    = 2
	TagValueIDAPILaneSlowLight    = 3
	TagValueIDAPILaneSlowHeavy    = 4
	TagValueIDAPILaneFastHardware = 5
	TagValueIDAPILaneSlowHardware = 6

	TagValueIDAPILaneFastLightv2    = 0
	TagValueIDAPILaneFastHeavyv2    = 1
	TagValueIDAPILaneSlowLightv2    = 2
	TagValueIDAPILaneSlowHeavyv2    = 3
	TagValueIDAPILaneSlowHardwarev2 = 4
	TagValueIDAPILaneFastHardwarev2 = 5

	TagValueIDConveyorRecent   = 1
	TagValueIDConveyorHistoric = 2
)

var conveyorToValue = map[int32]string{
	TagValueIDConveyorRecent:   "recent",
	TagValueIDConveyorHistoric: "historic",
}

const (
	TagValueIDAggregatorOriginal = 1
	TagValueIDAggregatorSpare    = 2
)

var aggregatorRoleToValue = map[int32]string{
	TagValueIDAggregatorOriginal: "original",
	TagValueIDAggregatorSpare:    "spare",
}

const (
	TagValueIDTimingFutureBucketRecent              = 1
	TagValueIDTimingFutureBucketHistoric            = 2
	TagValueIDTimingLateRecent                      = 3
	TagValueIDTimingLongWindowThrownAgent           = 4
	TagValueIDTimingLongWindowThrownAggregator      = 5
	TagValueIDTimingMissedSeconds                   = 6 // TODO - remove after everyone uses TagValueIDTimingMissedSecondsAgents
	TagValueIDTimingLongWindowThrownAggregatorLater = 7
	TagValueIDTimingDiskOverflowThrownAgent         = 8
	TagValueIDTimingMissedSecondsAgent              = 9  // separate to prevent mix of old and new way to write missed seconds
	TagValueIDTimingThrownDueToMemory               = 10 // if second could not be saved to disk, but later thrown out due to memory pressure

	TagValueIDRouteDirect       = 1
	TagValueIDRouteIngressProxy = 2
)

var routeToValue = map[int32]string{
	TagValueIDRouteDirect:       "direct",
	TagValueIDRouteIngressProxy: "ingress_proxy",
}

const (
	TagValueIDSecondReal    = 1
	TagValueIDSecondPhantom = 2 // We do not add phantom seconds anymore

	TagValueIDStatusOK    = 1
	TagValueIDStatusError = 2

	TagValueIDHistoricQueueMemory     = 1
	TagValueIDHistoricQueueDiskUnsent = 2
	TagValueIDHistoricQueueDiskSent   = 3
	TagValueIDHistoricQueueEmpty      = 4

	TagValueIDDiskCacheErrorWrite             = 1
	TagValueIDDiskCacheErrorRead              = 2
	TagValueIDDiskCacheErrorDelete            = 3
	TagValueIDDiskCacheErrorReadNotConfigured = 5
	TagValueIDDiskCacheErrorCompressFailed    = 6

	TagValueIDSrcIngestionStatusOKCached                     = 10
	TagValueIDSrcIngestionStatusOKUncached                   = 11
	TagValueIDSrcIngestionStatusErrMetricNotFound            = 21
	TagValueIDSrcIngestionStatusErrNanInfValue               = 23
	TagValueIDSrcIngestionStatusErrNanInfCounter             = 24
	TagValueIDSrcIngestionStatusErrNegativeCounter           = 25
	TagValueIDSrcIngestionStatusErrMapOther                  = 30 // never written, for historic data
	TagValueIDSrcIngestionStatusWarnMapTagNameNotFound       = 33
	TagValueIDSrcIngestionStatusErrMapInvalidRawTagValue     = 34 // warning now, for historic data
	TagValueIDSrcIngestionStatusErrMapTagValueCached         = 35
	TagValueIDSrcIngestionStatusErrMapTagValue               = 36
	TagValueIDSrcIngestionStatusErrMapGlobalQueueOverload    = 37
	TagValueIDSrcIngestionStatusErrMapPerMetricQueueOverload = 38
	TagValueIDSrcIngestionStatusErrMapTagValueEncoding       = 39
	TagValueIDSrcIngestionStatusOKLegacy                     = 40 // never written, for historic data
	TagValueIDSrcIngestionStatusErrMetricNonCanonical        = 41 // never written, for historic data
	TagValueIDSrcIngestionStatusErrMetricDisabled            = 42
	TagValueIDSrcIngestionStatusErrLegacyProtocol            = 43 // we stopped adding metrics to conveyor via legacy protocol
	TagValueIDSrcIngestionStatusWarnDeprecatedT              = 44 // never written, for historic data
	TagValueIDSrcIngestionStatusWarnDeprecatedStop           = 45 // never written, for historic data
	TagValueIDSrcIngestionStatusWarnMapTagSetTwice           = 46
	TagValueIDSrcIngestionStatusWarnDeprecatedKeyName        = 47
	TagValueIDSrcIngestionStatusErrMetricNameEncoding        = 48
	TagValueIDSrcIngestionStatusErrMapTagNameEncoding        = 49
	TagValueIDSrcIngestionStatusErrValueUniqueBothSet        = 50
	TagValueIDSrcIngestionStatusWarnOldCounterSemantic       = 51 // never written, for historic data
	TagValueIDSrcIngestionStatusWarnMapInvalidRawTagValue    = 52
	TagValueIDSrcIngestionStatusWarnMapTagNameFoundDraft     = 53
	TagValueIDSrcIngestionStatusErrShardingFailed            = 54
	TagValueIDSrcIngestionStatusWarnTimestampClampedPast     = 55
	TagValueIDSrcIngestionStatusWarnTimestampClampedFuture   = 56
	TagValueIDSrcIngestionStatusErrMetricBuiltin             = 57
	TagValueIDSrcIngestionStatusOKDup                        = 58 // TODO - remove after removing duplication code

	TagValueIDPacketFormatLegacy   = 1
	TagValueIDPacketFormatTL       = 2
	TagValueIDPacketFormatMsgPack  = 3
	TagValueIDPacketFormatJSON     = 4
	TagValueIDPacketFormatProtobuf = 5
	TagValueIDPacketFormatRPC      = 6
	TagValueIDPacketFormatEmpty    = 7

	TagValueIDMetaJournalVersionsKindLegacySHA1  = 1
	TagValueIDMetaJournalVersionsKindLegacyXXH3  = 2
	TagValueIDMetaJournalVersionsKindNormalXXH3  = 3
	TagValueIDMetaJournalVersionsKindCompactXXH3 = 4
)
const (
	TagValueIDPacketProtocolUDP      = 1
	TagValueIDPacketProtocolUnixGram = 2
	TagValueIDPacketProtocolTCP      = 3
	TagValueIDPacketProtocolVKRPC    = 4
	TagValueIDPacketProtocolHTTP     = 5
)
const (
	TagValueIDOldMetricForm6hTo1d = 2
	TagValueIDOldMetricForm1dTo2d = 3
	TagValueIDOldMetricForm2d     = 4
)

var packetProtocolToValue = map[int32]string{
	TagValueIDPacketProtocolUDP:      "udp",
	TagValueIDPacketProtocolUnixGram: "unixgram",
	TagValueIDPacketProtocolTCP:      "tcp",
	TagValueIDPacketProtocolVKRPC:    "vkrpc",
	TagValueIDPacketProtocolHTTP:     "http",
}

const (
	TagValueIDAgentReceiveStatusOK           = 1
	TagValueIDAgentReceiveStatusError        = 2
	TagValueIDAgentReceiveStatusConnect      = 3
	TagValueIDAgentReceiveStatusDisconnect   = 4
	TagValueIDAgentReceiveStatusNetworkError = 5
	TagValueIDAgentReceiveStatusFramingError = 6

	TagValueIDAggMappingDolphinLegacy = 1 // unused, remains for historic purpose
	TagValueIDAggMappingTags          = 2
	TagValueIDAggMappingMetaMetrics   = 3
	TagValueIDAggMappingJournalUpdate = 4

	TagValueIDAggSamplingFactorReasonInsertSize = 2 // we have no other reasons yet

	TagValueIDAggMappingStatusOKCached     = 1
	TagValueIDAggMappingStatusOKUncached   = 2
	TagValueIDAggMappingStatusErrUncached  = 3
	TagValueIDAggMappingStatusNotFound     = 4
	TagValueIDAggMappingStatusImmediateOK  = 5
	TagValueIDAggMappingStatusImmediateErr = 6
	TagValueIDAggMappingStatusEnqueued     = 7
	TagValueIDAggMappingStatusDelayedOK    = 8
	TagValueIDAggMappingStatusDelayedErr   = 9

	TagValueIDGroupSizeSamplingFit     = 1
	TagValueIDGroupSizeSamplingSampled = 2

	TagValueIDAgentMappingStatusAllDead   = 1
	TagValueIDAgentMappingStatusOKFirst   = 2
	TagValueIDAgentMappingStatusOKSecond  = 3
	TagValueIDAgentMappingStatusErrSingle = 4
	TagValueIDAgentMappingStatusErrBoth   = 5

	TagValueIDAggMappingCreatedStatusOK                    = 1
	TagValueIDAggMappingCreatedStatusCreated               = 2
	TagValueIDAggMappingCreatedStatusFlood                 = 3
	TagValueIDAggMappingCreatedStatusErrorPMC              = 4
	TagValueIDAggMappingCreatedStatusErrorInvariant        = 5
	TagValueIDAggMappingCreatedStatusErrorNotAskedToCreate = 6
	TagValueIDAggMappingCreatedStatusErrorInvalidValue     = 7

	TagValueIDAggMappingCreatedConveyorOld = 1
	TagValueIDAggMappingCreatedConveyorNew = 2

	TagValueIDAgentTimingGroupPipeline = 1
	TagValueIDAgentTimingGroupSend     = 2

	// pipeline
	TagValueIDAgentTimingMapping     = 1
	TagValueIDAgentTimingMappingSlow = 2
	TagValueIDAgentTimingApplyMetric = 3
	TagValueIDAgentTimingFlush       = 4
	TagValueIDAgentTimingPreprocess  = 5
	// send
	TagValueIDAgentTimingSendRecent   = 101
	TagValueIDAgentTimingSendHistoric = 102

	TagValueIDAggInsertV3 = 1
	TagValueIDAggInsertV2 = 2
)

var tableFormatToValue = map[int32]string{
	TagValueIDAggInsertV3: "v3",
	TagValueIDAggInsertV2: "v2",
}

var successOrFailure = map[int32]string{
	0: "success",
	1: "failure",
}

const (
	TagValueIDComponentAgent        = 1
	TagValueIDComponentAggregator   = 2
	TagValueIDComponentIngressProxy = 3
	TagValueIDComponentAPI          = 4
	TagValueIDComponentMetadata     = 5
)

var componentToValue = map[int32]string{
	TagValueIDComponentAgent:        "agent",
	TagValueIDComponentAggregator:   "aggregator",
	TagValueIDComponentIngressProxy: "ingress_proxy",
	TagValueIDComponentAPI:          "api",
	TagValueIDComponentMetadata:     "metadata",
}
var packetFormatToValue = map[int32]string{
	TagValueIDPacketFormatLegacy:   "legacy",
	TagValueIDPacketFormatTL:       "tl",
	TagValueIDPacketFormatMsgPack:  "msgpack",
	TagValueIDPacketFormatJSON:     "json",
	TagValueIDPacketFormatProtobuf: "protobuf",
	TagValueIDPacketFormatRPC:      "rpc",
	TagValueIDPacketFormatEmpty:    "empty",
}

const (
	TagValueIDAutoConfigOK             = 1
	TagValueIDAutoConfigErrorSend      = 2
	TagValueIDAutoConfigErrorKeepAlive = 3
	TagValueIDAutoConfigWrongCluster   = 4

	TagValueIDSizeCounter           = 1
	TagValueIDSizeValue             = 2
	TagValueIDSizeSingleValue       = 3
	TagValueIDSizePercentiles       = 4
	TagValueIDSizeUnique            = 5
	TagValueIDSizeStringTop         = 6
	TagValueIDSizeSampleFactors     = 7
	TagValueIDSizeIngestionStatusOK = 8
	TagValueIDSizeBuiltIn           = 9
)

var insertKindToValue = map[int32]string{
	TagValueIDSizeCounter:           "counter",
	TagValueIDSizeValue:             "value",
	TagValueIDSizeSingleValue:       "single_value", // used only by agent
	TagValueIDSizePercentiles:       "percentile",
	TagValueIDSizeUnique:            "unique",
	TagValueIDSizeStringTop:         "string_top",
	TagValueIDSizeSampleFactors:     "sample_factor",    // used only by agent
	TagValueIDSizeIngestionStatusOK: "ingestion_status", // used only by agent
	TagValueIDSizeBuiltIn:           "builtin",          // used only by aggregator
}

const (
	TagValueIDScrapeError = 1
	TagValueIDScrapeOK    = 2

	TagValueIDCPUUsageUser = 1
	TagValueIDCPUUsageSys  = 2

	TagValueIDHeartbeatEventStart     = 1
	TagValueIDHeartbeatEventHeartbeat = 2

	TagValueIDBuildArchAMD64 = 1
	TagValueIDBuildArch386   = 2
	TagValueIDBuildArchARM64 = 3
	TagValueIDBuildArchARM   = 4
)

var buildArchToValue = map[int32]string{
	TagValueIDBuildArchAMD64: "amd64",
	TagValueIDBuildArch386:   "386",
	TagValueIDBuildArchARM64: "arm64",
	TagValueIDBuildArchARM:   "arm",
}

const (
	TagValueIDSystemMetricCPU       = 1
	TagValueIDSystemMetricDisk      = 2
	TagValueIDSystemMetricMemory    = 3
	TagValueIDSystemMetricNet       = 4
	TagValueIDSystemMetricPSI       = 5
	TagValueIDSystemMetricSocksStat = 6
	TagValueIDSystemMetricProtocols = 7
	TagValueIDSystemMetricVMStat    = 8
	TagValueIDSystemMetricDMesgStat = 9
	TagValueIDSystemMetricGCStats   = 10
	TagValueIDSystemMetricNetClass  = 11

	TagValueIDRPC  = 1
	TagValueIDHTTP = 2

	TagOKConnection = 1
	TagNoConnection = 2
	TagOtherError   = 3
	TagRPCError     = 4
	TagTimeoutError = 5

	TagValueIDSamplingDecisionKeep    = -1
	TagValueIDSamplingDecisionDiscard = -2

	TagValueIDDMESGParseError = 1
	TagValueIDAPIPanicError   = 2

	TagValueIDRestartTimingsPhaseInactive          = 1
	TagValueIDRestartTimingsPhaseStartDiskCache    = 2
	TagValueIDRestartTimingsPhaseStartReceivers    = 3
	TagValueIDRestartTimingsPhaseStartService      = 4
	TagValueIDRestartTimingsPhaseTotal             = 100 // we want average of sum
	TagValueIDRestartTimingsPhaseStopRecentSenders = 101
	TagValueIDRestartTimingsPhaseStopReceivers     = 102
	TagValueIDRestartTimingsPhaseStopFlusher       = 103
	TagValueIDRestartTimingsPhaseStopFlushing      = 104
	TagValueIDRestartTimingsPhaseStopPreprocessor  = 105
	TagValueIDRestartTimingsPhaseStopInserters     = 106
	TagValueIDRestartTimingsPhaseStopRPCServer     = 107
	TagValueIDRestartTimingsPhaseSaveMappings      = 108
	TagValueIDRestartTimingsPhaseSaveJournal       = 109

	TagValueIDAggBucketInfoRows               = 1
	TagValueIDAggBucketInfoIntKeys            = 2
	TagValueIDAggBucketInfoStringKeys         = 3
	TagValueIDAggBucketInfoMappingHits        = 4
	TagValueIDAggBucketInfoMappingMisses      = 5
	TagValueIDAggBucketInfoMappingUnknownKeys = 6
	TagValueIDAggBucketInfoMappingLocks       = 7
	TagValueIDAggBucketInfoCentroids          = 8
	TagValueIDAggBucketInfoUniqueBytes        = 9
	TagValueIDAggBucketInfoStringTops         = 10
	TagValueIDAggBucketInfoIntTops            = 11
	TagValueIDAggBucketInfoNewKeys            = 12
	TagValueIDAggBucketInfoMetrics            = 13
	TagValueIDAggBucketInfoOutdatedRows       = 14

	TagValueIDMappingCacheEventHit                 = 1
	TagValueIDMappingCacheEventMiss                = 2
	TagValueIDMappingCacheEventTimestampUpdate     = 3
	TagValueIDMappingCacheEventTimestampUpdateSkip = 4
	TagValueIDMappingCacheEventAdd                 = 5
	TagValueIDMappingCacheEventEvict               = 6

	TagValueIDMappingQueueEventUnknownMapRemove  = 1
	TagValueIDMappingQueueEventUnknownMapAdd     = 2
	TagValueIDMappingQueueEventUnknownListRemove = 3
	TagValueIDMappingQueueEventUnknownListAdd    = 4
	TagValueIDMappingQueueEventCreateMapAdd      = 5
	TagValueIDMappingQueueEventCreateMapRemove   = 6
)

var (
	apiBufferKind = map[int32]string{
		1: "pool", // "sync.Pool", allocated buffer is subject for reuse (good)
		2: "heap", // large buffer won't be reused (bad, should not happen)
	}
)

var (
	// insert additional names from here
	// https://github.com/ClickHouse/clickhouse-java/blob/2417ae4c286dd1f26e2eac0e7752033fe1aac8b3/src/main/java/ru/yandex/clickhouse/except/ClickHouseErrorCode.java
	clickhouseExceptions = map[int32]string{
		// 0:      "OK", - do not add, we have exception 0 set for timeouts when we get no response
		115:    "UNKNOWN_SETTING",
		164:    "READONLY",
		192:    "UNKNOWN_USER",
		193:    "WRONG_PASSWORD",
		194:    "REQUIRED_PASSWORD",
		202:    "TOO_MUCH_SIMULTANEOUS_QUERIES",
		225:    "NO_ZOOKEEPER",
		241:    "MEMORY_LIMIT_EXCEEDED",
		242:    "TABLE_IS_READ_ONLY",
		252:    "TOO_MUCH_PARTS",
		100252: "TOO_MUCH_PARTS_SYNTAX_ERROR", // this is semantically different, parsed from exception text
		394:    "QUERY_WAS_CANCELLED",
	}
)

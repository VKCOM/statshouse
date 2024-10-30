// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package format

import (
	"log"
	"math"
	"strconv"

	"github.com/mailru/easyjson/opt"
)

const (
	TagIDShift       = 100 // "0" is set to 100, "1" to 101, "_s" to 99, "_h" to 98, leaving space in both directions
	TagIDShiftLegacy = 2   // in the past we used this shift, so it got into DB

	tagStringForUI = "tag"

	// TODO - rename all "Src" names to "Agent", rename "__src" to "__agent" also

	BuiltinGroupIDDefault = -4 // for all metrics with group not known. We want to edit it in the future, so not 0
	BuiltinGroupIDBuiltin = -2 // for all built in metrics except host
	BuiltinGroupIDHost    = -3 // host built in metrics
	BuiltinGroupIDMissing = -5 // indicates missing metadata while running sampling, should not happen

	BuiltinNamespaceIDDefault = -5
	BuiltinNamespaceIDMissing = -6 // indicates missing metadata while running sampling, should not happen

	BuiltinMetricIDAgentSamplingFactor       = -1
	BuiltinMetricIDAggBucketReceiveDelaySec  = -2 // Also approximates insert delay, interesting for historic buckets
	BuiltinMetricIDAggInsertSize             = -3 // If all contributors come on time, count will be 1 (per shard). If some come through historic conveyor, can be larger.
	BuiltinMetricIDTLByteSizePerInflightType = -4
	BuiltinMetricIDAggKeepAlive              = -5 // How many keep-alive were among contributors
	BuiltinMetricIDAggSizeCompressed         = -6
	BuiltinMetricIDAggSizeUncompressed       = -7
	BuiltinMetricIDAggAdditionsToEstimator   = -8 // How many new tags were inserted into bucket
	BuiltinMetricIDAggHourCardinality        = -9
	BuiltinMetricIDAggSamplingFactor         = -10
	BuiltinMetricIDIngestionStatus           = -11
	BuiltinMetricIDAggInsertTime             = -12
	BuiltinMetricIDAggHistoricBucketsWaiting = -13
	BuiltinMetricIDAggBucketAggregateTimeSec = -14
	BuiltinMetricIDAggActiveSenders          = -15
	BuiltinMetricIDAggOutdatedAgents         = -16
	BuiltinMetricIDAgentDiskCacheErrors      = -18
	BuiltinMetricIDTimingErrors              = -20
	BuiltinMetricIDAgentReceivedBatchSize    = -21
	BuiltinMetricIDAggMapping                = -23
	BuiltinMetricIDAggInsertTimeReal         = -24
	BuiltinMetricIDAgentHistoricQueueSize    = -25
	BuiltinMetricIDAggHistoricSecondsWaiting = -26
	BuiltinMetricIDAggInsertSizeReal         = -27
	BuiltinMetricIDAgentMapping              = -30
	BuiltinMetricIDAgentReceivedPacketSize   = -31
	BuiltinMetricIDAggMappingCreated         = -33
	BuiltinMetricIDVersions                  = -34
	BuiltinMetricIDBadges                    = -35 // copy of some other metrics for efficient show of errors and warnings
	BuiltinMetricIDAutoConfig                = -36
	BuiltinMetricIDJournalVersions           = -37 // has smart custom sending logic
	BuiltinMetricIDPromScrapeTime            = -38
	BuiltinMetricIDAgentHeartbeatVersion     = -41 // TODO - remove
	BuiltinMetricIDAgentHeartbeatArgs        = -42 // TODO - remove, this metric was writing larger than allowed strings to DB in the past
	BuiltinMetricIDUsageMemory               = -43
	BuiltinMetricIDUsageCPU                  = -44
	BuiltinMetricIDGeneratorConstCounter     = -45
	BuiltinMetricIDGeneratorSinCounter       = -46
	BuiltinMetricIDHeartbeatVersion          = -47
	BuiltinMetricIDHeartbeatArgs             = -48 // this metric was writing larger than allowed strings to DB in the past
	// BuiltinMetricIDAPIRPCServiceTime       = -49 // deprecated, replaced by "__api_service_time"
	BuiltinMetricIDAPIBRS = -50
	// BuiltinMetricIDAPIEndpointResponseTime = -51 // deprecated, replaced by "__api_response_time"
	// BuiltinMetricIDAPIEndpointServiceTime  = -52 // deprecated, replaced by "__api_service_time"
	BuiltinMetricIDBudgetHost           = -53 // these 2 metrics are invisible, but host mapping is flood-protected by their names
	BuiltinMetricIDBudgetAggregatorHost = -54 // we want to see limits properly credited in flood meta metric tags
	BuiltinMetricIDAPIActiveQueries     = -55
	BuiltinMetricIDRPCRequests          = -56
	BuiltinMetricIDBudgetUnknownMetric  = -57
	// BuiltinMetricIDHeartbeatArgs2             = -58 // not recorded any more
	// BuiltinMetricIDHeartbeatArgs3             = -59 // not recorded any more
	// BuiltinMetricIDHeartbeatArgs4             = -60 // not recorded any more
	BuiltinMetricIDContributorsLog            = -61
	BuiltinMetricIDContributorsLogRev         = -62
	BuiltinMetricIDGeneratorGapsCounter       = -63
	BuiltinMetricIDGroupSizeBeforeSampling    = -64
	BuiltinMetricIDGroupSizeAfterSampling     = -65
	BuiltinMetricIDAPISelectBytes             = -66
	BuiltinMetricIDAPISelectRows              = -67
	BuiltinMetricIDAPISelectDuration          = -68
	BuiltinMetricIDAgentHistoricQueueSizeSum  = -69
	BuiltinMetricIDAPISourceSelectRows        = -70
	BuiltinMetricIDSystemMetricScrapeDuration = -71
	BuiltinMetricIDMetaServiceTime            = -72
	BuiltinMetricIDMetaClientWaits            = -73
	BuiltinMetricIDAgentUDPReceiveBufferSize  = -74
	BuiltinMetricIDAPIMetricUsage             = -75
	BuiltinMetricIDAPIServiceTime             = -76
	BuiltinMetricIDAPIResponseTime            = -77
	BuiltinMetricIDSrcTestConnection          = -78
	BuiltinMetricIDAgentAggregatorTimeDiff    = -79
	BuiltinMetricIDSrcSamplingMetricCount     = -80
	BuiltinMetricIDAggSamplingMetricCount     = -81
	BuiltinMetricIDSrcSamplingSizeBytes       = -82
	BuiltinMetricIDAggSamplingSizeBytes       = -83
	BuiltinMetricIDUIErrors                   = -84
	BuiltinMetricIDStatsHouseErrors           = -85
	BuiltinMetricIDSrcSamplingBudget          = -86
	BuiltinMetricIDAggSamplingBudget          = -87
	BuiltinMetricIDSrcSamplingGroupBudget     = -88
	BuiltinMetricIDAggSamplingGroupBudget     = -89
	BuiltinMetricIDPromQLEngineTime           = -90
	BuiltinMetricIDAPICacheHit                = -91
	BuiltinMetricIDAggScrapeTargetDispatch    = -92
	BuiltinMetricIDAggScrapeTargetDiscovery   = -93
	BuiltinMetricIDAggScrapeConfigHash        = -94
	BuiltinMetricIDAggSamplingTime            = -95
	BuiltinMetricIDAgentDiskCacheSize         = -96
	BuiltinMetricIDAggContributors            = -97
	// BuiltinMetricIDAggAgentSharding           = -98  // deprecated
	BuiltinMetricIDAPICacheBytesAlloc        = -99
	BuiltinMetricIDAPICacheBytesFree         = -100
	BuiltinMetricIDAPICacheBytesTotal        = -101
	BuiltinMetricIDAPICacheAgeEvict          = -102
	BuiltinMetricIDAPICacheAgeTotal          = -103
	BuiltinMetricIDAPIBufferBytesAlloc       = -104
	BuiltinMetricIDAPIBufferBytesFree        = -105
	BuiltinMetricIDAPIBufferBytesTotal       = -106
	BuiltinMetricIDAutoCreateMetric          = -107
	BuiltinMetricIDRestartTimings            = -108
	BuiltinMetricIDGCDuration                = -109
	BuiltinMetricIDAggHistoricHostsWaiting   = -110
	BuiltinMetricIDAggSamplingEngineTime     = -111
	BuiltinMetricIDAggSamplingEngineKeys     = -112
	BuiltinMetricIDProxyAcceptHandshakeError = -113
	BuiltinMetricIDProxyVmSize               = -114
	BuiltinMetricIDProxyVmRSS                = -115
	BuiltinMetricIDProxyHeapAlloc            = -116
	BuiltinMetricIDProxyHeapSys              = -117
	BuiltinMetricIDProxyHeapIdle             = -118
	BuiltinMetricIDProxyHeapInuse            = -119
	BuiltinMetricIDApiVmSize                 = -120
	BuiltinMetricIDApiVmRSS                  = -121
	BuiltinMetricIDApiHeapAlloc              = -122
	BuiltinMetricIDApiHeapSys                = -123
	BuiltinMetricIDApiHeapIdle               = -124
	BuiltinMetricIDApiHeapInuse              = -125
	BuiltinMetricIDClientWriteError          = -126

	// [-1000..-2000] reserved by host system metrics
	// [-10000..-12000] reserved by builtin dashboard
	// [-20000..-22000] reserved by well known configuration IDs
	PrometheusConfigID          = -20000
	PrometheusGeneratedConfigID = -20001
	KnownTagsConfigID           = -20002

	// metric names used in code directly
	BuiltinMetricNameAggBucketReceiveDelaySec   = "__agg_bucket_receive_delay_sec"
	BuiltinMetricNameAgentSamplingFactor        = "__src_sampling_factor"
	BuiltinMetricNameAggSamplingFactor          = "__agg_sampling_factor"
	BuiltinMetricNameIngestionStatus            = "__src_ingestion_status"
	BuiltinMetricNameAggMappingCreated          = "__agg_mapping_created"
	BuiltinMetricNameBadges                     = "__badges"
	BuiltinMetricNamePromScrapeTime             = "__prom_scrape_time"
	BuiltinMetricNameMetaServiceTime            = "__meta_rpc_service_time"
	BuiltinMetricNameMetaClientWaits            = "__meta_load_journal_client_waits"
	BuiltinMetricNameUsageMemory                = "__usage_mem"
	BuiltinMetricNameUsageCPU                   = "__usage_cpu"
	BuiltinMetricNameAPIBRS                     = "__api_big_response_storage_size"
	BuiltinMetricNameAPISelectBytes             = "__api_ch_select_bytes"
	BuiltinMetricNameAPISelectRows              = "__api_ch_select_rows"
	BuiltinMetricNameAPISourceSelectRows        = "__api_ch_source_select_rows"
	BuiltinMetricNameAPISelectDuration          = "__api_ch_select_duration"
	BuiltinMetricNameBudgetHost                 = "__budget_host"
	BuiltinMetricNameBudgetOwner                = "__budget_owner"
	BuiltinMetricNameBudgetAggregatorHost       = "__budget_aggregator_host"
	BuiltinMetricNameAPIActiveQueries           = "__api_active_queries"
	BuiltinMetricNameBudgetUnknownMetric        = "__budget_unknown_metric"
	BuiltinMetricNameSystemMetricScrapeDuration = "__system_metrics_duration"
	BuiltinMetricNameAPIMetricUsage             = "__api_metric_usage"
	BuiltinMetricNameAPIServiceTime             = "__api_service_time"
	BuiltinMetricNameAPIResponseTime            = "__api_response_time"
	BuiltinMetricNameHeartbeatVersion           = "__heartbeat_version"
	BuiltinMetricNameStatsHouseErrors           = "__statshouse_errors"
	BuiltinMetricNamePromQLEngineTime           = "__promql_engine_time"
	BuiltinMetricNameAPICacheHit                = "__api_cache_hit_rate"
	BuiltinMetricNameIDUIErrors                 = "__ui_errors"
	BuiltinMetricAPICacheBytesAlloc             = "__api_cache_bytes_alloc"
	BuiltinMetricAPICacheBytesFree              = "__api_cache_bytes_free"
	BuiltinMetricAPICacheBytesTotal             = "__api_cache_bytes_total"
	BuiltinMetricAPICacheAgeTotal               = "__api_cache_age_total"
	BuiltinMetricAPICacheAgeEvict               = "__api_cache_age_evict"
	BuiltinMetricAPIBufferBytesAlloc            = "__api_buffer_bytes_alloc"
	BuiltinMetricAPIBufferBytesFree             = "__api_buffer_bytes_free"
	BuiltinMetricAPIBufferBytesTotal            = "__api_buffer_bytes_total"
	BuiltinMetricNameGCDuration                 = "__gc_duration"
	BuiltinMetricNameProxyAcceptHandshakeError  = "__igp_accept_handshake_error"
	BuiltinMetricNameApiVmSize                  = "__api_vm_size"
	BuiltinMetricNameApiVmRSS                   = "__api_vm_rss"
	BuiltinMetricNameApiHeapAlloc               = "__api_heap_alloc"
	BuiltinMetricNameApiHeapSys                 = "__api_heap_sys"
	BuiltinMetricNameApiHeapIdle                = "__api_heap_idle"
	BuiltinMetricNameApiHeapInuse               = "__api_heap_inuse"
	BuiltinMetricNameProxyVmSize                = "__igp_vm_size"
	BuiltinMetricNameProxyVmRSS                 = "__igp_vm_rss"
	BuiltinMetricNameProxyHeapAlloc             = "__igp_heap_alloc"
	BuiltinMetricNameProxyHeapSys               = "__igp_heap_sys"
	BuiltinMetricNameProxyHeapIdle              = "__igp_heap_idle"
	BuiltinMetricNameProxyHeapInuse             = "__igp_heap_inuse"
	BuiltinMetricNameClientWriteError           = "__src_client_write_err"

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

	TagValueIDProduction = 1
	TagValueIDStaging1   = 2
	TagValueIDStaging2   = 3
	TagValueIDStaging3   = 4

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

	TagValueIDAggregatorOriginal = 1
	TagValueIDAggregatorSpare    = 2

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

	TagValueIDSecondReal    = 1
	TagValueIDSecondPhantom = 2 // We do not add phantom seconds anymore

	TagValueIDSharingByMappedTags = 0
	TagValueIDSharingByMetricId   = 1

	TagValueIDInsertTimeOK    = 1
	TagValueIDInsertTimeError = 2

	TagValueIDHistoricQueueMemory     = 1
	TagValueIDHistoricQueueDiskUnsent = 2
	TagValueIDHistoricQueueDiskSent   = 3

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
	TagValueIDSrcIngestionStatusErrMetricInvisible           = 42
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

	TagValueIDPacketFormatLegacy   = 1
	TagValueIDPacketFormatTL       = 2
	TagValueIDPacketFormatMsgPack  = 3
	TagValueIDPacketFormatJSON     = 4
	TagValueIDPacketFormatProtobuf = 5
	TagValueIDPacketFormatRPC      = 6
	TagValueIDPacketFormatEmpty    = 7

	TagValueIDPacketProtocolUDP      = 1
	TagValueIDPacketProtocolUnixGram = 2
	TagValueIDPacketProtocolTCP      = 3
	TagValueIDPacketProtocolVKRPC    = 4
	TagValueIDPacketProtocolHTTP     = 5

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

	TagValueIDAgentFirstSampledMetricBudgetPerMetric = 1
	TagValueIDAgentFirstSampledMetricBudgetUnused    = 2

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

	TagValueIDComponentAgent        = 1
	TagValueIDComponentAggregator   = 2
	TagValueIDComponentIngressProxy = 3
	TagValueIDComponentAPI          = 4
	TagValueIDComponentMetadata     = 5

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
)

var (
	BuiltinMetricByName           map[string]*MetricMetaValue
	BuiltinMetricAllowedToReceive map[string]*MetricMetaValue

	// list of built-in metrics which can be sent as normal metric. Used by API and prometheus exporter
	// Description: "-" marks tags used in incompatible way in the past. We should not reuse such tags, because there would be garbage in historic data.

	BuiltinMetrics = map[int32]*MetricMetaValue{
		BuiltinMetricIDAgentSamplingFactor: {
			Name: BuiltinMetricNameAgentSamplingFactor,
			Kind: MetricKindValue,
			Description: `Sample factor selected by agent.
Calculated by agent from scratch every second to fit all collected data into network budget.
Count of this metric is proportional to # of clients who set it in particular second. 
Set only if greater than 1.`,
			Tags: []MetricMetaTag{{
				Description: "metric",
				IsMetric:    true,
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}},
			PreKeyTagID: "1",
		},
		BuiltinMetricIDAggBucketReceiveDelaySec: {
			Name: BuiltinMetricNameAggBucketReceiveDelaySec,
			Kind: MetricKindValue,
			Description: `Difference between timestamp of received bucket and aggregator wall clock.
Count of this metric is # of agents who sent this second (per replica*shard), and they do it every second to keep this metric stable.
Set by aggregator.`,
			MetricType: MetricSecond,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}, {
				Description:   "aggregator_role",
				ValueComments: convertToValueComments(aggregatorRoleToValue),
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDSecondReal:    "real",
					TagValueIDSecondPhantom: "phantom",
				}),
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDAggInsertSize: {
			Name:        "__agg_insert_size",
			Kind:        MetricKindValue,
			Description: "Size of aggregated bucket inserted into clickhouse. Written when second is inserted, which can be much later.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}, {
				Description:   "type",
				ValueComments: convertToValueComments(insertKindToValue),
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDTLByteSizePerInflightType: {
			Name:        "__src_tl_byte_size_per_inflight_type",
			Kind:        MetricKindValue,
			Description: "Approximate uncompressed byte size of various parts of TL representation of time bucket.\nSet by agent.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description:   "inflight_type",
				ValueComments: convertToValueComments(insertKindToValue),
			}},
		},
		BuiltinMetricIDAggKeepAlive: {
			Name:        "__agg_keep_alive",
			Kind:        MetricKindCounter,
			Description: "Number of keep-alive empty inserts (which follow normal insert conveyor) in aggregated bucket.\nSet by aggregator.",
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDAggSizeCompressed: {
			Name:        "__agg_size_compressed",
			Kind:        MetricKindValue,
			Description: "Compressed size of bucket received from agent (size of raw TL request).\nSet by aggregator.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}, {
				Description:   "aggregator_role",
				ValueComments: convertToValueComments(aggregatorRoleToValue),
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDAggSizeUncompressed: {
			Name:        "__agg_size_uncompressed",
			Kind:        MetricKindValue,
			Description: "Uncompressed size of bucket received from agent.\nSet by aggregator.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}, {
				Description:   "aggregator_role",
				ValueComments: convertToValueComments(aggregatorRoleToValue),
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDAggAdditionsToEstimator: {
			Name: "__agg_additions_to_estimator",
			Kind: MetricKindValue,
			Description: `How many unique metric-tag combinations were inserted into aggregation bucket.
Set by aggregator. Max(value)@host shows host responsible for most combinations, and is very order-dependent.`,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}, {
				Description:   "aggregator_role",
				ValueComments: convertToValueComments(aggregatorRoleToValue),
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDAggHourCardinality: {
			Name: "__agg_hour_cardinality",
			Kind: MetricKindValue,
			Description: `Estimated unique metric-tag combinations per hour.
Linear interpolation between previous hour and value collected so far for this hour.
Steps of interpolation can be visible on graph.
Each aggregator writes value on every insert to particular second, multiplied by # of aggregator shards.
So avg() of this metric shows estimated full cardinality with or without grouping by aggregator.`,
			Resolution: 60,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "metric",
				IsMetric:    true,
			}},
			PreKeyTagID: "4",
		},
		BuiltinMetricIDAggSamplingFactor: {
			Name: BuiltinMetricNameAggSamplingFactor,
			Kind: MetricKindValue,
			Description: `Sample factor selected by aggregator.
Calculated by aggregator from scratch every second to fit all collected data into clickhouse insert budget.
Set only if greater than 1.`,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "metric",
				IsMetric:    true,
			}, {
				Description: "-", // we do not show sampling reason for now because there is single reason. We write it, though.
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAggSamplingFactorReasonInsertSize: "insert_size",
				}),
			}},
			PreKeyTagID: "4",
		},
		BuiltinMetricIDIngestionStatus: {
			Name: BuiltinMetricNameIngestionStatus,
			Kind: MetricKindCounter,
			Description: `Status of receiving metrics by agent.
Most errors are due to various data format violation.
Some, like 'err_map_per_metric_queue_overload', 'err_map_tag_value', 'err_map_tag_value_cached' indicate tag mapping subsystem slowdowns or errors.
This metric uses sampling budgets of metric it refers to, so flooding by errors cannot affect other metrics.
'err_*_utf8'' statuses store original string value in hex.`,
			StringTopDescription: "string_value",
			Tags: []MetricMetaTag{{
				Description: "metric",
				IsMetric:    true,
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDSrcIngestionStatusOKCached:                     "ok_cached",
					TagValueIDSrcIngestionStatusOKUncached:                   "ok_uncached",
					TagValueIDSrcIngestionStatusErrMetricNotFound:            "err_metric_not_found",
					TagValueIDSrcIngestionStatusErrNanInfValue:               "err_nan_inf_value",
					TagValueIDSrcIngestionStatusErrNanInfCounter:             "err_nan_inf_counter",
					TagValueIDSrcIngestionStatusErrNegativeCounter:           "err_negative_counter",
					TagValueIDSrcIngestionStatusErrMapOther:                  "err_map_other",
					TagValueIDSrcIngestionStatusWarnMapTagNameNotFound:       "warn_tag_not_found",
					TagValueIDSrcIngestionStatusErrMapInvalidRawTagValue:     "err_map_invalid_raw_tag_value",
					TagValueIDSrcIngestionStatusErrMapTagValueCached:         "err_map_tag_value_cached",
					TagValueIDSrcIngestionStatusErrMapTagValue:               "err_map_tag_value",
					TagValueIDSrcIngestionStatusErrMapGlobalQueueOverload:    "err_map_global_queue_overload",
					TagValueIDSrcIngestionStatusErrMapPerMetricQueueOverload: "err_map_per_metric_queue_overload",
					TagValueIDSrcIngestionStatusErrMapTagValueEncoding:       "err_validate_tag_value_utf8",
					TagValueIDSrcIngestionStatusOKLegacy:                     "ok_legacy_protocol",
					TagValueIDSrcIngestionStatusErrMetricNonCanonical:        "non_canonical_name",
					TagValueIDSrcIngestionStatusErrMetricInvisible:           "err_metric_disabled",
					TagValueIDSrcIngestionStatusErrLegacyProtocol:            "err_legacy_protocol",
					TagValueIDSrcIngestionStatusWarnDeprecatedT:              "warn_deprecated_field_t",
					TagValueIDSrcIngestionStatusWarnDeprecatedStop:           "warn_deprecated_field_stop",
					TagValueIDSrcIngestionStatusWarnMapTagSetTwice:           "warn_map_tag_set_twice",
					TagValueIDSrcIngestionStatusWarnDeprecatedKeyName:        "warn_deprecated_tag_name",
					TagValueIDSrcIngestionStatusErrMetricNameEncoding:        "err_validate_metric_utf8",
					TagValueIDSrcIngestionStatusErrMapTagNameEncoding:        "err_validate_tag_name_utf8",
					TagValueIDSrcIngestionStatusErrValueUniqueBothSet:        "err_value_unique_both_set",
					TagValueIDSrcIngestionStatusWarnOldCounterSemantic:       "warn_deprecated_counter_semantic",
					TagValueIDSrcIngestionStatusWarnMapInvalidRawTagValue:    "warn_map_invalid_raw_tag_value",
					TagValueIDSrcIngestionStatusWarnMapTagNameFoundDraft:     "warn_tag_draft_found",
					TagValueIDSrcIngestionStatusErrShardingFailed:            "err_sharding_failed",
				}),
			}, {
				Description: "tag_id",
			}},
			PreKeyTagID: "1",
			Sharding: []MetricSharding{{
				Strategy: ShardByTag,
				TagId:    opt.OUint32(1),
			}},
		},
		BuiltinMetricIDAggInsertTime: {
			Name:        "__agg_insert_time",
			Kind:        MetricKindValue,
			Description: "Time inserting this second into clickhouse took. Written when second is inserted, which can be much later.",
			MetricType:  MetricSecond,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDInsertTimeOK:    "ok",
					TagValueIDInsertTimeError: "error",
				}),
			}},
		},
		BuiltinMetricIDAggHistoricBucketsWaiting: {
			Name: "__agg_historic_buckets_waiting",
			Kind: MetricKindValue,
			Description: `Time difference of historic seconds (several per contributor) waiting to be inserted via historic conveyor.
Count is number of such seconds waiting.`,
			MetricType: MetricSecond,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "aggregator_role",
				ValueComments: convertToValueComments(aggregatorRoleToValue),
			}, {
				Description:   "route",
				ValueComments: convertToValueComments(routeToValue),
			}},
		},
		BuiltinMetricIDAggBucketAggregateTimeSec: {
			Name: "__agg_bucket_aggregate_time_sec",
			Kind: MetricKindValue,
			Description: `Time between agent bucket is received and fully aggregated into aggregator bucket.
Set by aggregator. Max(value)@host shows agent responsible for longest aggregation.`,
			MetricType: MetricSecond,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}, {
				Description:   "aggregator_role",
				ValueComments: convertToValueComments(aggregatorRoleToValue),
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDAggActiveSenders: {
			Name:        "__agg_active_senders",
			Kind:        MetricKindValue,
			Description: "Number of insert lines between aggregator and clickhouse busy with insertion.",
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}},
		},
		BuiltinMetricIDAggOutdatedAgents: {
			Name:        "__agg_outdated_agents",
			Kind:        MetricKindCounter,
			Resolution:  60,
			Description: "Number of outdated agents.",
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "owner",
			}, {
				Description: "host",
			}, {
				Description: "remote_ip",
				RawKind:     "ip",
			}},
		},
		BuiltinMetricIDAgentDiskCacheErrors: {
			Name:        "__src_disc_cache_errors",
			Kind:        MetricKindCounter,
			Description: "Disk cache errors. Written by agent.",
			Tags: []MetricMetaTag{{
				Description: "kind",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDDiskCacheErrorWrite:             "err_write",
					TagValueIDDiskCacheErrorRead:              "err_read",
					TagValueIDDiskCacheErrorDelete:            "err_delete",
					TagValueIDDiskCacheErrorReadNotConfigured: "err_read_not_configured",
					TagValueIDDiskCacheErrorCompressFailed:    "err_compress",
				}),
			}},
		},
		BuiltinMetricIDTimingErrors: {
			Name: "__timing_errors",
			Kind: MetricKindValue,
			Description: `Timing errors - sending data too early or too late.
Set by either agent or aggregator, depending on status.`,
			Tags: []MetricMetaTag{{
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDTimingFutureBucketRecent:              "clock_future_recent",
					TagValueIDTimingFutureBucketHistoric:            "clock_future_historic",
					TagValueIDTimingLateRecent:                      "late_recent",
					TagValueIDTimingLongWindowThrownAgent:           "out_of_window_agent",
					TagValueIDTimingLongWindowThrownAggregator:      "out_of_window_aggregator",
					TagValueIDTimingMissedSeconds:                   "missed_seconds",
					TagValueIDTimingLongWindowThrownAggregatorLater: "out_of_window_aggregator_later",
					TagValueIDTimingDiskOverflowThrownAgent:         "out_of_disk_space_agent",
					TagValueIDTimingMissedSecondsAgent:              "missed_seconds_agent",
					TagValueIDTimingThrownDueToMemory:               "out_of_memory_space_agent",
				}),
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDAgentReceivedBatchSize: {
			Name:        "__src_ingested_metric_batch_size",
			Kind:        MetricKindValue,
			Description: "Size in bytes of metric batches received by agent.\nCount is # of such batches.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description:   "format",
				ValueComments: convertToValueComments(packetFormatToValue),
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAgentReceiveStatusOK:    "ok",
					TagValueIDAgentReceiveStatusError: "error",
				}),
			}, {
				Description:   "protocol",
				ValueComments: convertToValueComments(packetProtocolToValue),
			}},
		},
		BuiltinMetricIDAggMapping: {
			Name:        "__agg_mapping_status",
			Kind:        MetricKindCounter,
			Description: "Status of mapping on aggregator side.",
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "mapper",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAggMappingDolphinLegacy: "dolphin_legacy",
					TagValueIDAggMappingTags:          "client_pmc_legacy",
					TagValueIDAggMappingMetaMetrics:   "client_meta_metric",
					TagValueIDAggMappingJournalUpdate: "journal_update",
				}),
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAggMappingStatusOKCached:     "ok_cached",
					TagValueIDAggMappingStatusOKUncached:   "ok_uncached",
					TagValueIDAggMappingStatusErrUncached:  "err_uncached",
					TagValueIDAggMappingStatusNotFound:     "not_found",
					TagValueIDAggMappingStatusImmediateOK:  "ok_immediate",
					TagValueIDAggMappingStatusImmediateErr: "err_immediate",
					TagValueIDAggMappingStatusEnqueued:     "enqueued",
					TagValueIDAggMappingStatusDelayedOK:    "ok_delayed",
					TagValueIDAggMappingStatusDelayedErr:   "err_delayed",
				}),
			}},
		},
		BuiltinMetricIDAggInsertTimeReal: {
			Name:        "__agg_insert_time_real",
			Kind:        MetricKindValue,
			Description: "Time of aggregated bucket inserting into clickhouse took in this second.\nactual seconds inserted can be from the past.",
			MetricType:  MetricSecond,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDInsertTimeOK:    "ok",
					TagValueIDInsertTimeError: "error",
				}),
			}, {
				Description: "http_status",
				Raw:         true,
			}, {
				Description: "clickhouse_exception",
				Raw:         true, // TODO - ValueComments with popular clickhouse exceptions
			}, {
				Description: "experiment",
				ValueComments: convertToValueComments(map[int32]string{
					0: "main",
					1: "experiment",
				}),
			}},
		},
		BuiltinMetricIDAgentHistoricQueueSize: {
			Name:        "__src_historic_queue_size_bytes",
			Kind:        MetricKindValue,
			Description: "Historic queue size in memory and on disk.\nDisk size increases when second is written, decreases when file is deleted.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description: "storage",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDHistoricQueueMemory:     "memory",
					TagValueIDHistoricQueueDiskUnsent: "disk_unsent",
					TagValueIDHistoricQueueDiskSent:   "disk_sent",
				}),
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDAgentHistoricQueueSizeSum: {
			Name:        "__src_historic_queue_size_sum_bytes",
			Kind:        MetricKindValue,
			Description: "Historic queue size in memory and on disk, sum for shards sent to every shard.\nCan be compared with __src_historic_queue_size_bytes to find if subset of aggregators is inaccessible.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description: "storage",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDHistoricQueueMemory:     "memory",
					TagValueIDHistoricQueueDiskUnsent: "disk_unsent",
					TagValueIDHistoricQueueDiskSent:   "disk_sent",
				}),
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDAggHistoricSecondsWaiting: {
			Name:        "__agg_historic_seconds_waiting",
			Kind:        MetricKindValue,
			Description: "Time difference of aggregated historic seconds waiting to be inserted via historic conveyor. Count is number of unique seconds waiting.",
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "aggregator_role",
				ValueComments: convertToValueComments(aggregatorRoleToValue),
			}, {
				Description:   "route",
				ValueComments: convertToValueComments(routeToValue),
			}},
		},
		BuiltinMetricIDAggInsertSizeReal: {
			Name:        "__agg_insert_size_real",
			Kind:        MetricKindValue,
			Description: "Size of aggregated bucket inserted into clickhouse in this second (actual seconds inserted can be from the past).",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDInsertTimeOK:    "ok",
					TagValueIDInsertTimeError: "error",
				}),
			}, {
				Description: "http_status",
				Raw:         true,
			}, {
				Description: "clickhouse_exception",
				Raw:         true, // TODO - ValueComments with popular clickhouse exceptions
			}, {
				Description: "experiment",
				ValueComments: convertToValueComments(map[int32]string{
					0: "main",
					1: "experiment",
				}),
			}},
		},
		BuiltinMetricIDAgentMapping: {
			Name:        "__src_mapping_time",
			Kind:        MetricKindValue,
			Description: "Time and status of mapping request.\nWritten by agent.",
			MetricType:  MetricSecond,
			Tags: []MetricMetaTag{{
				Description: "mapper",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAggMappingDolphinLegacy: "dolphin_legacy",
					TagValueIDAggMappingTags:          "pmc_legacy",
					TagValueIDAggMappingMetaMetrics:   "meta_metric",
				}),
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAgentMappingStatusAllDead:   "error_all_dead",
					TagValueIDAgentMappingStatusOKFirst:   "ok_first",
					TagValueIDAgentMappingStatusOKSecond:  "ok_second",
					TagValueIDAgentMappingStatusErrSingle: "error_single_alive",
					TagValueIDAgentMappingStatusErrBoth:   "error_both_alive",
				}),
			}},
		},
		BuiltinMetricIDAgentReceivedPacketSize: {
			Name:        "__src_ingested_packet_size",
			Kind:        MetricKindValue,
			Description: "Size in bytes of packets received by agent. Also count is # of received packets.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description:   "format",
				ValueComments: convertToValueComments(packetFormatToValue),
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAgentReceiveStatusOK:           "ok",
					TagValueIDAgentReceiveStatusError:        "error",
					TagValueIDAgentReceiveStatusConnect:      "connect",
					TagValueIDAgentReceiveStatusDisconnect:   "disconnect",
					TagValueIDAgentReceiveStatusNetworkError: "network_error",
					TagValueIDAgentReceiveStatusFramingError: "framing_error",
				}),
			}, {
				Description:   "protocol",
				ValueComments: convertToValueComments(packetProtocolToValue),
			}},
		},
		BuiltinMetricIDAgentUDPReceiveBufferSize: {
			Name:        "__src_udp_receive_buffer_size",
			Kind:        MetricKindValue,
			Resolution:  60,
			Description: "Size in bytes of agent UDP receive buffer.",
			MetricType:  MetricByte,
		},
		BuiltinMetricIDAggMappingCreated: {
			Name: BuiltinMetricNameAggMappingCreated,
			Kind: MetricKindValue,
			Description: `Status of mapping string tags to integer values.
Value is actual integer value created (by incrementing global counter).
Set by aggregator.`,
			StringTopDescription: "Tag Values",
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "metric",
				IsMetric:    true,
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAggMappingCreatedStatusOK:                    "ok",
					TagValueIDAggMappingCreatedStatusCreated:               "created",
					TagValueIDAggMappingCreatedStatusFlood:                 "mapping_flood",
					TagValueIDAggMappingCreatedStatusErrorPMC:              "err_pmc",
					TagValueIDAggMappingCreatedStatusErrorInvariant:        "err_pmc_invariant",
					TagValueIDAggMappingCreatedStatusErrorNotAskedToCreate: "err_not_asked_to_create",
					TagValueIDAggMappingCreatedStatusErrorInvalidValue:     "err_invalid_value",
				}),
			}, {
				Description: "tag_id",
			}},
			PreKeyTagID: "4",
		},
		BuiltinMetricIDVersions: {
			Name:                 "__build_version",
			Kind:                 MetricKindCounter,
			Description:          "Build Version (commit) of statshouse components.",
			StringTopDescription: "Build Commit",
			Resolution:           60,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description:   "component",
				ValueComments: convertToValueComments(componentToValue),
			}, {
				Description: "commit_date",
				Raw:         true,
			}, {
				Description: "commit_timestamp",
				RawKind:     "timestamp",
			}, {
				Description: "commit_hash",
				RawKind:     "hex",
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDBadges: {
			Name:        BuiltinMetricNameBadges,
			Kind:        MetricKindValue,
			Description: "System metric used to display UI badges above plot. Stores stripped copy of some other builtin metrics.",
			Resolution:  5,
			Tags: []MetricMetaTag{{
				Description: "badge",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDBadgeAgentSamplingFactor: "agent_sampling_factor",
					TagValueIDBadgeAggSamplingFactor:   "aggregator_sampling_factor",
					TagValueIDBadgeIngestionErrors:     "ingestion_errors",
					TagValueIDBadgeIngestionWarnings:   "ingestion_warnings",
					TagValueIDBadgeAggMappingErrors:    "mapping_errors",
					TagValueIDBadgeContributors:        "contributors",
				}),
			}, {
				Description: "metric",
				IsMetric:    true,
			}},
			PreKeyTagID: "2",
			Sharding: []MetricSharding{{
				Strategy: ShardAggInternal,
			}},
		},
		BuiltinMetricIDAutoConfig: {
			Name: "__autoconfig",
			Kind: MetricKindCounter,
			Description: `Status of agent getConfig RPC message, used to configure sharding on agents.
Set by aggregator, max host shows actual host of agent who connected.
Ingress proxies first proxy request (to record host and IP of agent), then replace response with their own addresses.'`,
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAutoConfigOK:             "ok_config",
					TagValueIDAutoConfigErrorSend:      "err_send",
					TagValueIDAutoConfigErrorKeepAlive: "err_keep_alive",
					TagValueIDAutoConfigWrongCluster:   "err_config_cluster",
				}),
			}, {
				Description: "shard_replica",
				Raw:         true,
			}, {
				Description: "total_shard_replicas",
				Raw:         true,
			}},
		},
		BuiltinMetricIDJournalVersions: {
			Name:                 "__metric_journal_version",
			Kind:                 MetricKindCounter,
			Description:          "Metadata journal version plus stable hash of journal state.",
			StringTopDescription: "Journal Hash",
			Resolution:           60,
			Tags: []MetricMetaTag{{
				Description:   "component",
				ValueComments: convertToValueComments(componentToValue),
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "version",
				Raw:         true,
			}, {
				Description: "journal_hash",
				RawKind:     "hex",
			}},
		},
		BuiltinMetricIDPromScrapeTime: {
			Name:        BuiltinMetricNamePromScrapeTime,
			Kind:        MetricKindValue,
			Description: "Time of scraping prom metrics",
			MetricType:  MetricSecond,
			Tags: []MetricMetaTag{
				{
					Description: "-",
				}, {
					Description: "job",
				}, {
					Description: "host", // Legacy, see comment in pushScrapeTimeMetric
				}, {
					Description: "port", // Legacy, see comment in pushScrapeTimeMetric
				}, {
					Description: "scrape_status",
					ValueComments: convertToValueComments(map[int32]string{
						TagValueIDScrapeError: "error",
						TagValueIDScrapeOK:    "ok",
					}),
				},
			},
		},
		BuiltinMetricIDUsageMemory: {
			Name:        BuiltinMetricNameUsageMemory,
			Kind:        MetricKindValue,
			Description: "Memory usage of statshouse components.",
			MetricType:  MetricByte,
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description:   "component",
				ValueComments: convertToValueComments(componentToValue),
			}},
		},
		BuiltinMetricIDUsageCPU: {
			Name:        BuiltinMetricNameUsageCPU,
			Kind:        MetricKindValue,
			Description: "CPU usage of statshouse components, CPU seconds per second.",
			MetricType:  MetricSecond,
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description:   "component",
				ValueComments: convertToValueComments(componentToValue),
			}, {
				Description: "sys/user",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDCPUUsageSys:  "sys",
					TagValueIDCPUUsageUser: "user"}),
			}},
		},
		BuiltinMetricIDHeartbeatVersion: {
			Name:                 BuiltinMetricNameHeartbeatVersion,
			Kind:                 MetricKindValue,
			Description:          "Heartbeat value is uptime",
			MetricType:           MetricSecond,
			StringTopDescription: "Build Commit",
			Resolution:           60,
			Tags: []MetricMetaTag{{
				Description:   "component",
				ValueComments: convertToValueComments(componentToValue),
			}, {
				Description: "event_type",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDHeartbeatEventStart:     "start",
					TagValueIDHeartbeatEventHeartbeat: "heartbeat"}),
			}, {
				Description: "-",
			}, {
				Description: "commit_hash",
				RawKind:     "hex",
			}, {
				Description: "commit_date",
				Raw:         true,
			}, {
				Description: "commit_timestamp",
				RawKind:     "timestamp",
			}, {
				Description: "host",
			}, {
				Description: "remote_ip",
				RawKind:     "ip",
			}, {
				Description: "owner",
			}},
		},
		BuiltinMetricIDHeartbeatArgs: {
			Name:                 "__heartbeat_args",
			Kind:                 MetricKindValue,
			Description:          "Commandline of statshouse components.\nHeartbeat value is uptime.",
			MetricType:           MetricSecond,
			StringTopDescription: "Arguments",
			Resolution:           60,
			Tags: []MetricMetaTag{{
				Description:   "component",
				ValueComments: convertToValueComments(componentToValue),
			}, {
				Description: "event_type",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDHeartbeatEventStart:     "start",
					TagValueIDHeartbeatEventHeartbeat: "heartbeat"}),
			}, {
				Description: "arguments_hash",
				RawKind:     "hex",
			}, {
				Description: "commit_hash", // this is unrelated to metric keys, this is ingress key ID
				RawKind:     "hex",
			}, {
				Description: "commit_date",
				Raw:         true,
			}, {
				Description: "commit_timestamp",
				RawKind:     "timestamp",
			}, {
				Description: "host",
			}, {
				Description: "remote_ip",
				RawKind:     "ip",
			}, {
				Description: "arguments_length",
				RawKind:     "int",
			}},
		},
		BuiltinMetricIDGeneratorConstCounter: {
			Name:        "__fn_const_counter",
			Kind:        MetricKindCounter,
			Description: "Counter generated on the fly by constant function",
			Tags:        []MetricMetaTag{},
		},
		BuiltinMetricIDGeneratorSinCounter: {
			Name:        "__fn_sin_counter",
			Kind:        MetricKindCounter,
			Description: "Test counter generated on the fly by sine function",
			Tags:        []MetricMetaTag{},
		},
		BuiltinMetricIDAPIServiceTime: {
			Name:        BuiltinMetricNameAPIServiceTime,
			Kind:        MetricKindValue,
			Description: "Time to handle API query.",
			MetricType:  MetricSecond,
			Tags: []MetricMetaTag{{
				Description: "endpoint",
			}, {
				Description: "protocol",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDRPC:  "RPC",
					TagValueIDHTTP: "HTTP",
				}),
			}, {
				Description: "method",
			}, {
				Description: "data_format",
			}, {
				Description: "lane",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAPILaneFastLightv2:    "fast_light",
					TagValueIDAPILaneFastHeavyv2:    "fast_heavy",
					TagValueIDAPILaneSlowLightv2:    "slow_light",
					TagValueIDAPILaneSlowHeavyv2:    "slow_heavy",
					TagValueIDAPILaneSlowHardwarev2: "slow_hardware",
					TagValueIDAPILaneFastHardwarev2: "fast_hardware",
				}),
			}, {
				Description: "host",
			}, {
				Description: "token_name",
			}, {
				Description: "response_code",
				Raw:         true,
			}, {
				Description: "metric",
				IsMetric:    true,
			}, {
				Description: "priority",
				Raw:         true,
			}},
		},
		BuiltinMetricIDAPIResponseTime: {
			Name:        BuiltinMetricNameAPIResponseTime,
			Kind:        MetricKindValue,
			Description: "Time to handle and respond to query by API",
			MetricType:  MetricSecond,
			Tags: []MetricMetaTag{{
				Description: "endpoint",
			}, {
				Description: "protocol",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDRPC:  "RPC",
					TagValueIDHTTP: "HTTP",
				}),
			}, {
				Description: "method",
			}, {
				Description: "data_format",
			}, {
				Description: "lane",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAPILaneFastLightv2: "fastlight",
					TagValueIDAPILaneFastHeavyv2: "fastheavy",
					TagValueIDAPILaneSlowLightv2: "slowlight",
					TagValueIDAPILaneSlowHeavyv2: "slowheavy"}),
			}, {
				Description: "host",
			}, {
				Description: "token_name",
			}, {
				Description: "response_code",
				Raw:         true,
			}, {
				Description: "metric",
				IsMetric:    true,
			}, {
				Description: "priority",
				Raw:         true,
			}},
		},
		BuiltinMetricIDPromQLEngineTime: {
			Name:        BuiltinMetricNamePromQLEngineTime,
			Kind:        MetricKindValue,
			Description: "Time spent in PromQL engine",
			MetricType:  MetricSecond,
			Tags: []MetricMetaTag{{
				Name:        "host",
				Description: "API host",
			}, {
				Name:        "interval",
				Description: "Time interval requested",
				ValueComments: convertToValueComments(map[int32]string{
					1:  "1 second",
					2:  "5 minutes",
					3:  "15 minutes",
					4:  "1 hour",
					5:  "2 hours",
					6:  "6 hours",
					7:  "12 hours",
					8:  "1 day",
					9:  "2 days",
					10: "3 days",
					11: "1 week",
					12: "2 weeks",
					13: "1 month",
					14: "3 months",
					15: "6 months",
					16: "1 year",
					17: "2 years",
					18: "inf",
				}),
			}, {
				Name:        "points",
				Description: "Resulting number of points",
				ValueComments: convertToValueComments(map[int32]string{
					1:  "1",
					2:  "1K",
					3:  "2K",
					4:  "3K",
					5:  "4K",
					6:  "5K",
					7:  "6K",
					8:  "7K",
					9:  "8K",
					10: "inf",
				}),
			}, {
				Name:        "work",
				Description: "Type of work performed",
				ValueComments: convertToValueComments(map[int32]string{
					1: "query_parsing",
					2: "data_access",
					3: "data_processing",
				}),
			}},
		}, BuiltinMetricIDMetaServiceTime: { // TODO - harmonize
			Name:        BuiltinMetricNameMetaServiceTime,
			Kind:        MetricKindValue,
			Description: "Time to handle RPC query by meta.",
			MetricType:  MetricSecond,
			Tags: []MetricMetaTag{{
				Description: "host",
			}, {
				Description: "method",
			}, {
				Description: "query_type",
			}, {
				Description: "status",
			}},
		},
		BuiltinMetricIDMetaClientWaits: { // TODO - harmonize
			Name:        BuiltinMetricNameMetaClientWaits,
			Kind:        MetricKindValue,
			Description: "Number of clients waiting journal updates",
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDAPIBRS: { // TODO - harmonize
			Name:        BuiltinMetricNameAPIBRS,
			Kind:        MetricKindValue,
			Description: "Size of storage inside API of big response chunks",
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDAPISelectBytes: {
			Name: BuiltinMetricNameAPISelectBytes,
			Kind: MetricKindValue,
			// TODO replace with logs
			StringTopDescription: "error",
			Description:          "Number of bytes was handled by ClickHouse SELECT query",
			Tags: []MetricMetaTag{{
				Description: "query type",
			}},
		},
		BuiltinMetricIDAPISelectRows: {
			Name: BuiltinMetricNameAPISelectRows,
			Kind: MetricKindValue,
			// TODO replace with logs
			StringTopDescription: "error",
			Description:          "Number of rows was handled by ClickHouse SELECT query",
			Tags: []MetricMetaTag{{
				Description: "query type",
			}},
		},
		BuiltinMetricIDAPISelectDuration: {
			Name:        BuiltinMetricNameAPISelectDuration,
			Kind:        MetricKindValue,
			MetricType:  MetricSecond,
			Description: "Duration of clickhouse query",
			Tags: []MetricMetaTag{
				{
					Description: "query type",
				},
				{
					Description: "metric",
					IsMetric:    true,
				},
				{
					Description: "table",
				},
				{
					Description: "kind",
				},
				{
					Description: "status",
				},
				{
					Description: "token-short",
				},
				{
					Description: "token-long",
				},
				{
					Description: "shard", // metric % 16 for now, experimental
					Raw:         true,
				},
			},
		},
		BuiltinMetricIDAPISourceSelectRows: {
			Name:        BuiltinMetricNameAPISourceSelectRows,
			Kind:        MetricKindValue,
			Description: "Value of this metric number of rows was selected from DB or cache",
			Tags: []MetricMetaTag{
				{
					Description: "source type",
				},
				{
					Description: "metric",
					IsMetric:    true,
				},
				{
					Description: "table",
				},
				{
					Description: "kind",
				},
			},
		},
		BuiltinMetricIDBudgetUnknownMetric: {
			Name:        BuiltinMetricNameBudgetUnknownMetric,
			Kind:        MetricKindCounter,
			Description: "Invisible metric used only for accounting budget to create mappings with metric not found",
			Tags:        []MetricMetaTag{},
		},
		BuiltinMetricIDBudgetHost: {
			Name:        BuiltinMetricNameBudgetHost,
			Kind:        MetricKindCounter,
			Description: "Invisible metric used only for accounting budget to create host mappings",
			Tags:        []MetricMetaTag{},
		},
		BuiltinMetricIDBudgetAggregatorHost: {
			Name:        BuiltinMetricNameBudgetAggregatorHost,
			Kind:        MetricKindCounter,
			Description: "Invisible metric used only for accounting budget to create host mappings of aggregators themselves",
			Tags:        []MetricMetaTag{},
		},
		BuiltinMetricIDAPIActiveQueries: {
			Name:        BuiltinMetricNameAPIActiveQueries,
			Kind:        MetricKindValue,
			Description: "Active queries to clickhouse by API.\nRequests are assigned to lanes by estimated processing time.",
			Tags: []MetricMetaTag{{
				Description: "-", // if we need another component
			}, {
				Description: "version",
			}, {
				Description: "lane",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAPILaneFastLight:    "fast_light",
					TagValueIDAPILaneFastHeavy:    "fast_heavy",
					TagValueIDAPILaneSlowLight:    "slow_light",
					TagValueIDAPILaneSlowHeavy:    "slow_heavy",
					TagValueIDAPILaneFastHardware: "fast_hardware",
					TagValueIDAPILaneSlowHardware: "slow_hardware",
				}),
			}, {
				Description: "host",
			}},
		},
		BuiltinMetricIDRPCRequests: {
			Name:        "__rpc_request_size",
			Kind:        MetricKindValue,
			Description: "Size of RPC request bodies.\nFor ingress proxy, key_id can be used to identify senders.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description:   "component",
				ValueComments: convertToValueComments(componentToValue),
			}, {
				Description: "tag",
				RawKind:     "hex",
				ValueComments: convertToValueComments(map[int32]string{
					0x28bea524: "statshouse.autoCreate",
					0x4285ff57: "statshouse.getConfig2",
					0x42855554: "statshouse.getMetrics3",
					0x4285ff56: "statshouse.getTagMapping2",
					0x75a7f68e: "statshouse.getTagMappingBootstrap",
					0x41df72a3: "statshouse.getTargets2",
					0x4285ff53: "statshouse.sendKeepAlive2",
					0x44575940: "statshouse.sendSourceBucket2",
					0x4285ff58: "statshouse.testConnection2",
				}),
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDRPCRequestsStatusOK:          "ok",
					TagValueIDRPCRequestsStatusErrLocal:    "err_local",
					TagValueIDRPCRequestsStatusErrUpstream: "err_upstream",
					TagValueIDRPCRequestsStatusHijack:      "hijacked",
					TagValueIDRPCRequestsStatusNoHandler:   "err_no_handler"}),
			}, {
				Description: "-", // in the future - error code
			}, {
				Description: "-", // in the future - something
			}, {
				Description: "key_id", // this is unrelated to metric keys, this is ingress key ID
				RawKind:     "hex",
			}, {
				Description: "host", // filled by aggregator for ingress proxy
			}, {
				Description: "protocol",
				Raw:         true,
			}},
		},
		BuiltinMetricIDContributorsLog: {
			Name: "__contributors_log",
			Kind: MetricKindValue,
			Description: `Used to invalidate API caches.
Timestamps of all inserted seconds per second are recorded here in key1.
Value is delta between second value and time it was inserted.
To see which seconds change when, use __contributors_log_rev`,
			Tags: []MetricMetaTag{{
				Description: "timestamp",
				RawKind:     "timestamp",
			}},
		},
		BuiltinMetricIDContributorsLogRev: {
			Name: "__contributors_log_rev",
			Kind: MetricKindValue,
			Description: `Reverse index of __contributors_log, used to invalidate API caches.
key1 is UNIX timestamp of second when this second was changed.
Value is delta between second value and time it was inserted.`,
			Tags: []MetricMetaTag{{
				Description: "insert_timestamp",
				RawKind:     "timestamp",
			}},
		},
		BuiltinMetricIDGeneratorGapsCounter: {
			Name:        "__fn_gaps_counter",
			Kind:        MetricKindCounter,
			Description: "Test counter with constant value, but with multiple gaps",
			Tags:        []MetricMetaTag{},
		},
		BuiltinMetricIDGroupSizeBeforeSampling: {
			Name:        "__group_size_before_sampling",
			Kind:        MetricKindValue,
			Description: "Group size before sampling, bytes.",
			Tags: []MetricMetaTag{{
				Description:   "component",
				ValueComments: convertToValueComments(componentToValue),
			}, {
				Description: "group",
				Raw:         true,
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDGroupSizeSamplingFit:     "fit",
					TagValueIDGroupSizeSamplingSampled: "sampled",
				}),
			}},
		},
		BuiltinMetricIDGroupSizeAfterSampling: {
			Name:        "__group_size_after_sampling",
			Kind:        MetricKindValue,
			Description: "Group size after sampling, bytes.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description:   "component",
				ValueComments: convertToValueComments(componentToValue),
			}, {
				Description: "group",
				Raw:         true,
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDGroupSizeSamplingFit:     "fit",
					TagValueIDGroupSizeSamplingSampled: "sampled",
				}),
			}},
		},
		BuiltinMetricIDSystemMetricScrapeDuration: {
			Name:        BuiltinMetricNameSystemMetricScrapeDuration,
			Kind:        MetricKindValue,
			Description: "System metrics scrape duration in seconds",
			MetricType:  MetricSecond,
			Tags: []MetricMetaTag{{
				Description: "collector",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDSystemMetricCPU:       "cpu",
					TagValueIDSystemMetricDisk:      "disk",
					TagValueIDSystemMetricMemory:    "memory",
					TagValueIDSystemMetricNet:       "net",
					TagValueIDSystemMetricPSI:       "psi",
					TagValueIDSystemMetricSocksStat: "socks",
					TagValueIDSystemMetricProtocols: "protocols",
					TagValueIDSystemMetricVMStat:    "vmstat",
					TagValueIDSystemMetricDMesgStat: "dmesg",
					TagValueIDSystemMetricGCStats:   "gc",
					TagValueIDSystemMetricNetClass:  "netclass",
				}),
			}},
		},
		BuiltinMetricIDAgentAggregatorTimeDiff: {
			Name:        "__src_agg_time_diff",
			Kind:        MetricKindValue,
			Resolution:  60,
			Description: "Aggregator time - agent time when start testConnection",
			MetricType:  MetricSecond,
			Tags: []MetricMetaTag{{
				Description:   "component",
				ValueComments: convertToValueComments(componentToValue),
			}},
		},
		BuiltinMetricIDSrcTestConnection: {
			Name:        "__src_test_connection",
			Kind:        MetricKindValue,
			Resolution:  60,
			Description: "Duration of call test connection rpc method",
			MetricType:  MetricSecond,
			Tags: []MetricMetaTag{{
				Description:   "component",
				ValueComments: convertToValueComments(componentToValue),
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagOKConnection: "ok",
					TagOtherError:   "other",
					TagRPCError:     "rpc-error",
					TagNoConnection: "no-connection",
					TagTimeoutError: "timeout",
				}),
			}},
		},
		BuiltinMetricIDAPIMetricUsage: {
			Name:        BuiltinMetricNameAPIMetricUsage,
			Resolution:  60,
			Kind:        MetricKindCounter,
			Description: "Metric usage",
			Tags: []MetricMetaTag{
				{
					Description: "type",
					ValueComments: convertToValueComments(map[int32]string{
						TagValueIDRPC:  "RPC",
						TagValueIDHTTP: "http",
					}),
				},
				{
					Description: "user",
				},
				{
					Description: "metric",
					IsMetric:    true,
				},
			},
		},
		BuiltinMetricIDSrcSamplingMetricCount: {
			Name:        "__src_sampling_metric_count",
			Kind:        MetricKindValue,
			Description: `Metric count processed by sampler on agent.`,
			Tags: []MetricMetaTag{{
				Name:          "component",
				ValueComments: convertToValueComments(componentToValue),
			}},
		},
		BuiltinMetricIDAggSamplingMetricCount: {
			Name:        "__agg_sampling_metric_count",
			Kind:        MetricKindValue,
			Description: `Metric count processed by sampler on aggregator.`,
			Tags: []MetricMetaTag{{
				Name:          "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}},
		},
		BuiltinMetricIDSrcSamplingSizeBytes: {
			Name:        "__src_sampling_size_bytes",
			Kind:        MetricKindValue,
			MetricType:  MetricByte,
			Description: `Size in bytes processed by sampler on agent.`,
			Tags: []MetricMetaTag{{
				Name:          "component",
				ValueComments: convertToValueComments(componentToValue),
			}, {
				Name: "sampling_decision",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDSamplingDecisionKeep:    "keep",
					TagValueIDSamplingDecisionDiscard: "discard",
				}),
			}, {
				Name:        "namespace",
				IsNamespace: true,
				ValueComments: convertToValueComments(map[int32]string{
					BuiltinNamespaceIDDefault: "default",
					BuiltinNamespaceIDMissing: "missing",
				}),
			}, {
				Name:    "group",
				IsGroup: true,
				ValueComments: convertToValueComments(map[int32]string{
					BuiltinGroupIDDefault: "default",
					BuiltinGroupIDBuiltin: "builtin",
					BuiltinGroupIDHost:    "host",
					BuiltinGroupIDMissing: "missing",
				}),
			}},
		},
		BuiltinMetricIDAggSamplingSizeBytes: {
			Name:        "__agg_sampling_size_bytes",
			Kind:        MetricKindValue,
			MetricType:  MetricByte,
			Description: `Size in bytes processed by sampler on aggregator.`,
			Tags: []MetricMetaTag{{
				Name:          "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}, {
				Name: "sampling_decision",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDSamplingDecisionKeep:    "keep",
					TagValueIDSamplingDecisionDiscard: "discard",
				}),
			}, {
				Name:        "namespace",
				IsNamespace: true,
				ValueComments: convertToValueComments(map[int32]string{
					BuiltinNamespaceIDDefault: "default",
					BuiltinNamespaceIDMissing: "missing",
				}),
			}, {
				Name:    "group",
				IsGroup: true,
				ValueComments: convertToValueComments(map[int32]string{
					BuiltinGroupIDDefault: "default",
					BuiltinGroupIDBuiltin: "builtin",
					BuiltinGroupIDHost:    "host",
					BuiltinGroupIDMissing: "missing",
				}),
			}},
		},
		BuiltinMetricIDSrcSamplingBudget: {
			Name:        "__src_sampling_budget",
			Kind:        MetricKindValue,
			MetricType:  MetricByte,
			Description: `Budget allocated on agent.`,
			Tags: []MetricMetaTag{{
				Name:          "component",
				ValueComments: convertToValueComments(componentToValue),
			}},
		},
		BuiltinMetricIDAggSamplingBudget: {
			Name:        "__agg_sampling_budget",
			Kind:        MetricKindValue,
			MetricType:  MetricByte,
			Description: `Budget allocated on aggregator.`,
			Tags: []MetricMetaTag{{
				Name:          "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}},
		},
		BuiltinMetricIDSrcSamplingGroupBudget: {
			Name:        "__src_sampling_group_budget",
			Kind:        MetricKindValue,
			MetricType:  MetricByte,
			Description: `Group budget allocated on agent.`,
			Tags: []MetricMetaTag{{
				Name:          "component",
				ValueComments: convertToValueComments(componentToValue),
			}, {
				Name:        "namespace",
				IsNamespace: true,
				ValueComments: convertToValueComments(map[int32]string{
					BuiltinNamespaceIDDefault: "default",
					BuiltinNamespaceIDMissing: "missing",
				}),
			}, {
				Name:    "group",
				IsGroup: true,
				ValueComments: convertToValueComments(map[int32]string{
					BuiltinGroupIDDefault: "default",
					BuiltinGroupIDBuiltin: "builtin",
					BuiltinGroupIDHost:    "host",
					BuiltinGroupIDMissing: "missing",
				}),
			}},
		},
		BuiltinMetricIDAggSamplingGroupBudget: {
			Name:        "__agg_sampling_group_budget",
			Kind:        MetricKindValue,
			MetricType:  MetricByte,
			Description: `Group budget allocated on aggregator.`,
			Tags: []MetricMetaTag{{
				Name:          "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}, {
				Name:        "namespace",
				IsNamespace: true,
				ValueComments: convertToValueComments(map[int32]string{
					BuiltinNamespaceIDDefault: "default",
					BuiltinNamespaceIDMissing: "missing",
				}),
			}, {
				Name:    "group",
				IsGroup: true,
				ValueComments: convertToValueComments(map[int32]string{
					BuiltinGroupIDDefault: "default",
					BuiltinGroupIDBuiltin: "builtin",
					BuiltinGroupIDHost:    "host",
					BuiltinGroupIDMissing: "missing",
				}),
			}},
		},
		BuiltinMetricIDUIErrors: {
			Name:                 BuiltinMetricNameIDUIErrors,
			Kind:                 MetricKindValue,
			Description:          `Errors on the frontend.`,
			StringTopDescription: "error_string",
			Tags:                 []MetricMetaTag{{Description: "environment"}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}},
		},
		BuiltinMetricIDStatsHouseErrors: {
			Name:                 BuiltinMetricNameStatsHouseErrors,
			Kind:                 MetricKindCounter,
			Description:          `Always empty metric because SH don't have errors'`,
			StringTopDescription: "error_string",
			Tags: []MetricMetaTag{
				{
					Description: "error_type",
					Raw:         true,
					ValueComments: convertToValueComments(map[int32]string{
						TagValueIDDMESGParseError: "dmesg_parse",
						TagValueIDAPIPanicError:   "api_panic",
					}),
				}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}},
		},
		BuiltinMetricIDAPICacheHit: {
			Name:        BuiltinMetricNameAPICacheHit,
			Kind:        MetricKindValue,
			Description: `API cache hit rate`,
			Tags: []MetricMetaTag{{
				Description: "source",
			}, {
				Description: "metric",
				IsMetric:    true,
				Raw:         true,
			}, {
				Description: "table",
			}, {
				Description: "kind",
			}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}},
		},
		BuiltinMetricIDAggScrapeTargetDispatch: {
			Name:                 "__agg_scrape_target_dispatch",
			Kind:                 MetricKindCounter,
			Description:          "Scrape target-to-agent assigment events",
			StringTopDescription: "agent_host",
			Tags: []MetricMetaTag{
				{
					Description: "status",
					Raw:         true,
					ValueComments: convertToValueComments(map[int32]string{
						0: "success",
						1: "failure",
					}),
				},
				{
					Description: "event_type",
					Raw:         true,
					ValueComments: convertToValueComments(map[int32]string{
						1: "targets_ready",
						2: "targets_sent",
					}),
				},
			},
		},
		BuiltinMetricIDAggScrapeTargetDiscovery: {
			Name:                 "__agg_scrape_target_discovery",
			Kind:                 MetricKindCounter,
			Description:          "Scrape targets found by service discovery",
			StringTopDescription: "scrape_target",
		},
		BuiltinMetricIDAggScrapeConfigHash: {
			Name:        "__agg_scrape_config_hash",
			Kind:        MetricKindCounter,
			Description: "Scrape configuration string SHA1 hash",
			Tags: []MetricMetaTag{
				{
					Description: "config_hash",
					Raw:         true,
					RawKind:     "hex",
				},
			},
		},
		BuiltinMetricIDAggSamplingTime: {
			Name:        "__agg_sampling_time",
			Kind:        MetricKindValue,
			MetricType:  MetricSecond,
			Description: "Time sampling this second took. Written when second is inserted, which can be much later.",
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}},
		},
		BuiltinMetricIDAggSamplingEngineTime: {
			Name:        "__agg_sampling_engine_time",
			Kind:        MetricKindValue,
			MetricType:  MetricSecond,
			Description: "Time spent in sampling engine",
			Tags: []MetricMetaTag{{
				Description: "phase",
				ValueComments: map[string]string{
					" 1": "append",
					" 2": "partition",
					" 3": "budgeting",
					" 4": "sampling",
					" 5": "meta",
				},
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}},
		},
		BuiltinMetricIDAggSamplingEngineKeys: {
			Name:        "__agg_sampling_engine_keys",
			Kind:        MetricKindCounter,
			Description: "Number of series went through sampling engine",
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "conveyor",
				ValueComments: convertToValueComments(conveyorToValue),
			}},
		},
		BuiltinMetricIDAgentDiskCacheSize: {
			Name:        "__src_disk_cache_size",
			Kind:        MetricKindValue,
			MetricType:  MetricByte,
			Description: "Size of agent mapping cache",
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDAggContributors: {
			Name:        "__agg_contributors",
			Kind:        MetricKindValue,
			Description: "Number of contributors used to calculate sampling budget.",
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDAPICacheBytesAlloc: {
			Name:        BuiltinMetricAPICacheBytesAlloc,
			Kind:        MetricKindValue,
			Description: "API cache memory allocation in bytes.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description: "host",
			}, {
				Description:   "version",
				ValueComments: versionToValue,
			}, {
				Description:   "step",
				ValueComments: secondsToValue,
			}},
		},
		BuiltinMetricIDAPICacheBytesFree: {
			Name:        BuiltinMetricAPICacheBytesFree,
			Kind:        MetricKindValue,
			Description: "API cache memory deallocation in bytes.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description: "host",
			}, {
				Description:   "version",
				ValueComments: versionToValue,
			}, {
				Description:   "step",
				ValueComments: secondsToValue,
			}, {
				Description:   "reason",
				ValueComments: apiCacheEvictionReason,
			}},
		},
		BuiltinMetricIDAPICacheBytesTotal: {
			Name:        BuiltinMetricAPICacheBytesTotal,
			Kind:        MetricKindValue,
			Resolution:  15,
			Description: "API cache size in bytes.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description: "host",
			}, {
				Description:   "version",
				ValueComments: versionToValue,
			}, {
				Description:   "step",
				ValueComments: secondsToValue,
			}},
		},
		BuiltinMetricIDAPICacheAgeEvict: {
			Name:        BuiltinMetricAPICacheAgeEvict,
			Kind:        MetricKindValue,
			Description: "API cache entry age when evicted in seconds.",
			MetricType:  MetricSecond,
			Tags: []MetricMetaTag{{
				Description: "host",
			}, {
				Description:   "version",
				ValueComments: versionToValue,
			}, {
				Description:   "step",
				ValueComments: secondsToValue,
			}, {
				Description:   "reason",
				ValueComments: apiCacheEvictionReason,
			}},
		},
		BuiltinMetricIDAPICacheAgeTotal: {
			Name:        BuiltinMetricAPICacheAgeTotal,
			Kind:        MetricKindValue,
			Resolution:  15,
			Description: "API cache age in seconds.",
			MetricType:  MetricSecond,
			Tags: []MetricMetaTag{{
				Description: "host",
			}, {
				Description:   "version",
				ValueComments: versionToValue,
			}, {
				Description:   "step",
				ValueComments: secondsToValue,
			}},
		},
		BuiltinMetricIDAPIBufferBytesAlloc: {
			Name:        BuiltinMetricAPIBufferBytesAlloc,
			Kind:        MetricKindValue,
			Description: "API buffer allocation in bytes.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description: "host",
			}, {
				Description:   "kind",
				ValueComments: apiBufferKind,
			}},
		},
		BuiltinMetricIDAPIBufferBytesFree: {
			Name:        BuiltinMetricAPIBufferBytesFree,
			Kind:        MetricKindValue,
			Description: "API buffer deallocation in bytes.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description: "host",
			}, {
				Description:   "kind",
				ValueComments: apiBufferKind,
			}},
		},
		BuiltinMetricIDAPIBufferBytesTotal: {
			Name:        BuiltinMetricAPIBufferBytesTotal,
			Kind:        MetricKindValue,
			Description: "API buffer pool size in bytes.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDAutoCreateMetric: {
			Name:        "__agg_autocreate_metric",
			Kind:        MetricKindCounter,
			Description: "Event of automatically created metrics.",
			MetricType:  MetricByte,
			Tags: []MetricMetaTag{{
				Description: "action",
				ValueComments: map[string]string{
					" 1": "create",
					" 2": "edit",
				},
			}, {
				Description: "status",
				ValueComments: map[string]string{
					" 1": "success",
					" 2": "failure",
				},
			}},
		},
		BuiltinMetricIDRestartTimings: {
			Name:        "__src_restart_timings",
			Kind:        MetricKindValue,
			MetricType:  MetricSecond,
			Description: "Time of various restart phases (inactive is time between process stop and start)",
			Tags: []MetricMetaTag{{
				Description:   "component",
				ValueComments: convertToValueComments(componentToValue),
			}, {
				Description: "phase",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDRestartTimingsPhaseInactive:          "inactive",
					TagValueIDRestartTimingsPhaseStartDiskCache:    "start_disk_cache",
					TagValueIDRestartTimingsPhaseStartReceivers:    "start_receivers",
					TagValueIDRestartTimingsPhaseStartService:      "start_service",
					TagValueIDRestartTimingsPhaseTotal:             "total",
					TagValueIDRestartTimingsPhaseStopRecentSenders: "stop_recent_senders",
					TagValueIDRestartTimingsPhaseStopReceivers:     "stop_receivers",
					TagValueIDRestartTimingsPhaseStopFlusher:       "stop_flusher",
					TagValueIDRestartTimingsPhaseStopFlushing:      "stop_flushing",
					TagValueIDRestartTimingsPhaseStopPreprocessor:  "stop_preprocessor",
					TagValueIDRestartTimingsPhaseStopInserters:     "stop_inserters",
					TagValueIDRestartTimingsPhaseStopRPCServer:     "stop_rpc_server",
				}),
			}, {
				Description: "-",
			}, {
				Description: "-",
			}},
		},
		BuiltinMetricIDGCDuration: {
			Name:        BuiltinMetricNameGCDuration,
			Kind:        MetricKindValue,
			MetricType:  MetricSecond,
			Description: "Count - number of GC, Value - time spent to gc",
			Tags: []MetricMetaTag{{
				Description: "-", // reserved for host
			},
				{
					Description:   "component",
					ValueComments: convertToValueComments(componentToValue),
				}},
		},
		BuiltinMetricIDAggHistoricHostsWaiting: {
			Name:        "__agg_historic_hosts_waiting",
			Kind:        MetricKindValue,
			Description: "Approximate number of different hosts waiting with historic data.",
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "aggregator_role",
				ValueComments: convertToValueComments(aggregatorRoleToValue),
			}, {
				Description:   "route",
				ValueComments: convertToValueComments(routeToValue),
			}},
		},
		BuiltinMetricIDProxyAcceptHandshakeError: {
			Name:                 BuiltinMetricNameProxyAcceptHandshakeError,
			Kind:                 MetricKindCounter,
			Description:          "Proxy refused to accept incoming connection because of failed  handshake.",
			StringTopDescription: "remote_ip",
			Tags: []MetricMetaTag{{
				Description: "host",
			}, {
				Description: "magic_head",
			}, {
				Description: "error",
			}},
		},
		BuiltinMetricIDApiVmSize: {
			Name:        BuiltinMetricNameApiVmSize,
			Kind:        MetricKindValue,
			Description: "StatsHouse API virtual memory size.",
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDApiVmRSS: {
			Name:        BuiltinMetricNameApiVmRSS,
			Kind:        MetricKindValue,
			Description: "StatsHouse API resident set size.",
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDApiHeapAlloc: {
			Name:        BuiltinMetricNameApiHeapAlloc,
			Kind:        MetricKindValue,
			Description: "StatsHouse API bytes of allocated heap objects.",
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDApiHeapSys: {
			Name:        BuiltinMetricNameApiHeapSys,
			Kind:        MetricKindValue,
			Description: "StatsHouse API bytes of heap memory obtained from the OS.",
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDApiHeapIdle: {
			Name:        BuiltinMetricNameApiHeapIdle,
			Kind:        MetricKindValue,
			Description: "StatsHouse API bytes in idle (unused) spans.",
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDApiHeapInuse: {
			Name:        BuiltinMetricNameApiHeapInuse,
			Kind:        MetricKindValue,
			Description: "StatsHouse API bytes in in-use spans.",
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDProxyVmSize: {
			Name:        BuiltinMetricNameProxyVmSize,
			Kind:        MetricKindValue,
			Description: "StatsHouse proxy virtual memory size.",
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDProxyVmRSS: {
			Name:        BuiltinMetricNameProxyVmRSS,
			Kind:        MetricKindValue,
			Description: "StatsHouse proxy resident set size.",
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDProxyHeapAlloc: {
			Name:        BuiltinMetricNameProxyHeapAlloc,
			Kind:        MetricKindValue,
			Description: "StatsHouse proxy bytes of allocated heap objects.",
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDProxyHeapSys: {
			Name:        BuiltinMetricNameProxyHeapSys,
			Kind:        MetricKindValue,
			Description: "StatsHouse proxy bytes of heap memory obtained from the OS.",
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDProxyHeapIdle: {
			Name:        BuiltinMetricNameProxyHeapIdle,
			Kind:        MetricKindValue,
			Description: "StatsHouse proxy bytes in idle (unused) spans.",
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDProxyHeapInuse: {
			Name:        BuiltinMetricNameProxyHeapInuse,
			Kind:        MetricKindValue,
			Description: "StatsHouse proxy bytes in in-use spans.",
			Resolution:  60,
			Tags: []MetricMetaTag{{
				Description: "host",
			}},
		},
		BuiltinMetricIDClientWriteError: {
			Name:        BuiltinMetricNameClientWriteError,
			Kind:        MetricKindValue,
			MetricType:  MetricByte,
			Description: "Bytes lost on StatsHouse clients.",
			Tags: []MetricMetaTag{{
				Description: "lang",
				ValueComments: convertToValueComments(map[int32]string{
					1: "golang",
				})}, {
				Description: "cause",
				ValueComments: convertToValueComments(map[int32]string{
					1: "would_block",
				})},
			},
		},
	}

	builtinMetricsInvisible = map[int32]bool{
		BuiltinMetricIDBudgetHost:           true,
		BuiltinMetricIDBudgetAggregatorHost: true,
		BuiltinMetricIDBudgetUnknownMetric:  true,
	}

	// API and metadata sends this metrics via local statshouse instance
	builtinMetricsAllowedToReceive = map[int32]bool{
		BuiltinMetricIDTimingErrors:               true,
		BuiltinMetricIDPromScrapeTime:             true,
		BuiltinMetricIDAPIBRS:                     true,
		BuiltinMetricIDAPIServiceTime:             true,
		BuiltinMetricIDAPIResponseTime:            true,
		BuiltinMetricIDUsageMemory:                true,
		BuiltinMetricIDUsageCPU:                   true,
		BuiltinMetricIDAPIActiveQueries:           true,
		BuiltinMetricIDAPISelectRows:              true,
		BuiltinMetricIDAPISelectBytes:             true,
		BuiltinMetricIDAPISelectDuration:          true,
		BuiltinMetricIDSystemMetricScrapeDuration: true,
		BuiltinMetricIDMetaServiceTime:            true,
		BuiltinMetricIDMetaClientWaits:            true,
		BuiltinMetricIDAPIMetricUsage:             true,
		BuiltinMetricIDHeartbeatVersion:           true,
		BuiltinMetricIDUIErrors:                   true,
		BuiltinMetricIDStatsHouseErrors:           true,
		BuiltinMetricIDPromQLEngineTime:           true,
		BuiltinMetricIDAPICacheHit:                true,
		BuiltinMetricIDAPICacheBytesAlloc:         true,
		BuiltinMetricIDAPICacheBytesFree:          true,
		BuiltinMetricIDAPICacheBytesTotal:         true,
		BuiltinMetricIDAPICacheAgeEvict:           true,
		BuiltinMetricIDAPICacheAgeTotal:           true,
		BuiltinMetricIDAPIBufferBytesAlloc:        true,
		BuiltinMetricIDAPIBufferBytesFree:         true,
		BuiltinMetricIDAPIBufferBytesTotal:        true,
		BuiltinMetricIDAutoCreateMetric:           true,
		BuiltinMetricIDGCDuration:                 true,
		BuiltinMetricIDProxyAcceptHandshakeError:  true,
		BuiltinMetricIDProxyVmSize:                true,
		BuiltinMetricIDProxyVmRSS:                 true,
		BuiltinMetricIDProxyHeapAlloc:             true,
		BuiltinMetricIDProxyHeapSys:               true,
		BuiltinMetricIDProxyHeapIdle:              true,
		BuiltinMetricIDProxyHeapInuse:             true,
		BuiltinMetricIDApiVmSize:                  true,
		BuiltinMetricIDApiVmRSS:                   true,
		BuiltinMetricIDApiHeapAlloc:               true,
		BuiltinMetricIDApiHeapSys:                 true,
		BuiltinMetricIDApiHeapIdle:                true,
		BuiltinMetricIDApiHeapInuse:               true,
		BuiltinMetricIDClientWriteError:           true,
	}

	builtinMetricsNoSamplingAgent = map[int32]bool{
		BuiltinMetricIDAgentMapping:            true,
		BuiltinMetricIDJournalVersions:         true,
		BuiltinMetricIDAgentReceivedPacketSize: true,
		BuiltinMetricIDAgentReceivedBatchSize:  true,
		BuiltinMetricIDHeartbeatVersion:        true,
		BuiltinMetricIDHeartbeatArgs:           true,
		BuiltinMetricIDAgentDiskCacheErrors:    true,
		BuiltinMetricIDTimingErrors:            true,
		BuiltinMetricIDUsageMemory:             true,
		BuiltinMetricIDUsageCPU:                true,

		BuiltinMetricIDCPUUsage:        true,
		BuiltinMetricIDMemUsage:        true,
		BuiltinMetricIDProcessCreated:  true,
		BuiltinMetricIDProcessRunning:  true,
		BuiltinMetricIDSystemUptime:    true,
		BuiltinMetricIDPSICPU:          true,
		BuiltinMetricIDPSIMem:          true,
		BuiltinMetricIDPSIIO:           true,
		BuiltinMetricIDNetBandwidth:    true,
		BuiltinMetricIDNetPacket:       true,
		BuiltinMetricIDNetError:        true,
		BuiltinMetricIDDiskUsage:       true,
		BuiltinMetricIDINodeUsage:      true,
		BuiltinMetricIDTCPSocketStatus: true,
		BuiltinMetricIDTCPSocketMemory: true,
		BuiltinMetricIDSocketMemory:    true,
		BuiltinMetricIDSocketUsed:      true,
		BuiltinMetricIDSoftIRQ:         true,
		BuiltinMetricIDIRQ:             true,
		BuiltinMetricIDContextSwitch:   true,
		BuiltinMetricIDWriteback:       true,
	}

	MetricsWithAgentEnvRouteArch = map[int32]bool{
		BuiltinMetricIDAgentDiskCacheErrors:      true,
		BuiltinMetricIDTimingErrors:              true,
		BuiltinMetricIDAgentMapping:              true,
		BuiltinMetricIDAutoConfig:                true, // also passed through ingress proxies
		BuiltinMetricIDJournalVersions:           true,
		BuiltinMetricIDTLByteSizePerInflightType: true,
		BuiltinMetricIDIngestionStatus:           true,
		BuiltinMetricIDAgentReceivedBatchSize:    true,
		BuiltinMetricIDAgentReceivedPacketSize:   true,
		BuiltinMetricIDAggSizeCompressed:         true,
		BuiltinMetricIDAggSizeUncompressed:       true,
		BuiltinMetricIDAggBucketReceiveDelaySec:  true,
		BuiltinMetricIDAggBucketAggregateTimeSec: true,
		BuiltinMetricIDAggAdditionsToEstimator:   true,
		BuiltinMetricIDAgentHistoricQueueSize:    true,
		BuiltinMetricIDVersions:                  true,
		BuiltinMetricIDAggKeepAlive:              true,
		BuiltinMetricIDAggMappingCreated:         true,
		BuiltinMetricIDUsageMemory:               true,
		BuiltinMetricIDUsageCPU:                  true,
		BuiltinMetricIDHeartbeatVersion:          true,
		BuiltinMetricIDHeartbeatArgs:             true,
		BuiltinMetricIDAgentUDPReceiveBufferSize: true,
		BuiltinMetricIDAgentDiskCacheSize:        true,
		BuiltinMetricIDSrcTestConnection:         true,
		BuiltinMetricIDAgentAggregatorTimeDiff:   true,
		BuiltinMetricIDSrcSamplingMetricCount:    true,
		BuiltinMetricIDSrcSamplingSizeBytes:      true,
		BuiltinMetricIDStatsHouseErrors:          true,
		BuiltinMetricIDSrcSamplingBudget:         true,
		BuiltinMetricIDSrcSamplingGroupBudget:    true,
		BuiltinMetricIDRestartTimings:            true,
		BuiltinMetricIDGCDuration:                true,
	}

	metricsWithoutAggregatorID = map[int32]bool{
		BuiltinMetricIDTLByteSizePerInflightType:  true,
		BuiltinMetricIDIngestionStatus:            true,
		BuiltinMetricIDAgentDiskCacheErrors:       true,
		BuiltinMetricIDAgentReceivedBatchSize:     true,
		BuiltinMetricIDAgentMapping:               true,
		BuiltinMetricIDAgentReceivedPacketSize:    true,
		BuiltinMetricIDBadges:                     true,
		BuiltinMetricIDPromScrapeTime:             true,
		BuiltinMetricIDGeneratorConstCounter:      true,
		BuiltinMetricIDGeneratorSinCounter:        true,
		BuiltinMetricIDAPIBRS:                     true,
		BuiltinMetricIDAPISelectRows:              true,
		BuiltinMetricIDAPISelectBytes:             true,
		BuiltinMetricIDAPISelectDuration:          true,
		BuiltinMetricIDAPIServiceTime:             true,
		BuiltinMetricIDAPIResponseTime:            true,
		BuiltinMetricIDAPIActiveQueries:           true,
		BuiltinMetricIDBudgetHost:                 true,
		BuiltinMetricIDBudgetAggregatorHost:       true,
		BuiltinMetricIDSystemMetricScrapeDuration: true,
		BuiltinMetricIDAgentUDPReceiveBufferSize:  true,
		BuiltinMetricIDAgentDiskCacheSize:         true,
		BuiltinMetricIDAPIMetricUsage:             true,
		BuiltinMetricIDSrcSamplingMetricCount:     true,
		BuiltinMetricIDSrcSamplingSizeBytes:       true,
		BuiltinMetricIDSrcSamplingBudget:          true,
		BuiltinMetricIDSrcSamplingGroupBudget:     true,
		BuiltinMetricIDUIErrors:                   true,
		BuiltinMetricIDStatsHouseErrors:           true,
		BuiltinMetricIDPromQLEngineTime:           true,
		BuiltinMetricIDAPICacheBytesAlloc:         true,
		BuiltinMetricIDAPICacheBytesFree:          true,
		BuiltinMetricIDAPICacheBytesTotal:         true,
		BuiltinMetricIDAPICacheAgeEvict:           true,
		BuiltinMetricIDAPICacheAgeTotal:           true,
		BuiltinMetricIDAPIBufferBytesAlloc:        true,
		BuiltinMetricIDAPIBufferBytesFree:         true,
		BuiltinMetricIDAPIBufferBytesTotal:        true,
		BuiltinMetricIDRestartTimings:             true,
		BuiltinMetricIDGCDuration:                 true,
		BuiltinMetricIDProxyAcceptHandshakeError:  true,
		BuiltinMetricIDProxyVmSize:                true,
		BuiltinMetricIDProxyVmRSS:                 true,
		BuiltinMetricIDProxyHeapAlloc:             true,
		BuiltinMetricIDProxyHeapSys:               true,
		BuiltinMetricIDProxyHeapIdle:              true,
		BuiltinMetricIDProxyHeapInuse:             true,
		BuiltinMetricIDApiVmSize:                  true,
		BuiltinMetricIDApiVmRSS:                   true,
		BuiltinMetricIDApiHeapAlloc:               true,
		BuiltinMetricIDApiHeapSys:                 true,
		BuiltinMetricIDApiHeapIdle:                true,
		BuiltinMetricIDApiHeapInuse:               true,
		BuiltinMetricIDClientWriteError:           true,
	}

	insertKindToValue = map[int32]string{
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

	conveyorToValue = map[int32]string{
		TagValueIDConveyorRecent:   "recent",
		TagValueIDConveyorHistoric: "historic",
	}

	componentToValue = map[int32]string{
		TagValueIDComponentAgent:        "agent",
		TagValueIDComponentAggregator:   "aggregator",
		TagValueIDComponentIngressProxy: "ingress_proxy",
		TagValueIDComponentAPI:          "api",
		TagValueIDComponentMetadata:     "metadata",
	}

	packetFormatToValue = map[int32]string{
		TagValueIDPacketFormatLegacy:   "legacy",
		TagValueIDPacketFormatTL:       "tl",
		TagValueIDPacketFormatMsgPack:  "msgpack",
		TagValueIDPacketFormatJSON:     "json",
		TagValueIDPacketFormatProtobuf: "protobuf",
		TagValueIDPacketFormatRPC:      "rpc",
		TagValueIDPacketFormatEmpty:    "empty",
	}

	packetProtocolToValue = map[int32]string{
		TagValueIDPacketProtocolUDP:      "udp",
		TagValueIDPacketProtocolUnixGram: "unixgram",
		TagValueIDPacketProtocolTCP:      "tcp",
		TagValueIDPacketProtocolVKRPC:    "vkrpc",
		TagValueIDPacketProtocolHTTP:     "http",
	}

	aggregatorRoleToValue = map[int32]string{
		TagValueIDAggregatorOriginal: "original",
		TagValueIDAggregatorSpare:    "spare",
	}

	routeToValue = map[int32]string{
		TagValueIDRouteDirect:       "direct",
		TagValueIDRouteIngressProxy: "ingress_proxy",
	}

	buildArchToValue = map[int32]string{
		TagValueIDBuildArchAMD64: "amd64",
		TagValueIDBuildArch386:   "386",
		TagValueIDBuildArchARM64: "arm64",
		TagValueIDBuildArchARM:   "arm",
	}
	// BuiltInGroupDefault can be overridden by journal, don't use directly
	BuiltInGroupDefault = map[int32]*MetricsGroup{
		BuiltinGroupIDDefault: {
			ID:     BuiltinGroupIDDefault,
			Name:   "__default",
			Weight: 1,
		},
		BuiltinGroupIDBuiltin: {
			ID:     BuiltinGroupIDBuiltin,
			Name:   "__builtin",
			Weight: 1,
		},
		BuiltinGroupIDHost: {
			ID:     BuiltinGroupIDHost,
			Name:   "__host",
			Weight: 1,
		},
	}
	// BuiltInNamespaceDefault can be overridden by journal, don't use directly
	BuiltInNamespaceDefault = map[int32]*NamespaceMeta{
		BuiltinNamespaceIDDefault: {
			ID:     BuiltinNamespaceIDDefault,
			Name:   "__default",
			Weight: 1,
		},
	}

	versionToValue = map[string]string{
		" 1": "v1",
		" 2": "v2",
	}

	secondsToValue = map[string]string{
		" 1":       "1s",
		" 5":       "5s",
		" 15":      "15s",
		" 60":      "1m",
		" 300":     "5m",
		" 900":     "15m",
		" 3600":    "1h",
		" 14400":   "4h",
		" 86400":   "24h",
		" 604800":  "7d",
		" 2678400": "1M",
	}

	apiCacheEvictionReason = map[string]string{
		" 1": "stale",  // known to be stale
		" 2": "LRU",    // evicted to free up memory
		" 3": "update", // evicted by more recent load
	}

	apiBufferKind = map[string]string{
		" 1": "pool", // "sync.Pool", allocated buffer is subject for reuse (good)
		" 2": "heap", // large buffer won't be reused (bad, should not happen)
	}

	// metic metas for builtin metrics are accessible directly without search in map
	// they are initialized from BuiltinMetrics map in init() function
	BuiltinMetricMetaAgentSamplingFactor        *MetricMetaValue
	BuiltinMetricMetaAggBucketReceiveDelaySec   *MetricMetaValue
	BuiltinMetricMetaAggInsertSize              *MetricMetaValue
	BuiltinMetricMetaTLByteSizePerInflightType  *MetricMetaValue
	BuiltinMetricMetaAggKeepAlive               *MetricMetaValue
	BuiltinMetricMetaAggSizeCompressed          *MetricMetaValue
	BuiltinMetricMetaAggSizeUncompressed        *MetricMetaValue
	BuiltinMetricMetaAggAdditionsToEstimator    *MetricMetaValue
	BuiltinMetricMetaAggHourCardinality         *MetricMetaValue
	BuiltinMetricMetaAggSamplingFactor          *MetricMetaValue
	BuiltinMetricMetaIngestionStatus            *MetricMetaValue
	BuiltinMetricMetaAggInsertTime              *MetricMetaValue
	BuiltinMetricMetaAggHistoricBucketsWaiting  *MetricMetaValue
	BuiltinMetricMetaAggBucketAggregateTimeSec  *MetricMetaValue
	BuiltinMetricMetaAggActiveSenders           *MetricMetaValue
	BuiltinMetricMetaAggOutdatedAgents          *MetricMetaValue
	BuiltinMetricMetaAgentDiskCacheErrors       *MetricMetaValue
	BuiltinMetricMetaTimingErrors               *MetricMetaValue
	BuiltinMetricMetaAgentReceivedBatchSize     *MetricMetaValue
	BuiltinMetricMetaAggMapping                 *MetricMetaValue
	BuiltinMetricMetaAggInsertTimeReal          *MetricMetaValue
	BuiltinMetricMetaAgentHistoricQueueSize     *MetricMetaValue
	BuiltinMetricMetaAggHistoricSecondsWaiting  *MetricMetaValue
	BuiltinMetricMetaAggInsertSizeReal          *MetricMetaValue
	BuiltinMetricMetaAgentMapping               *MetricMetaValue
	BuiltinMetricMetaAgentReceivedPacketSize    *MetricMetaValue
	BuiltinMetricMetaAggMappingCreated          *MetricMetaValue
	BuiltinMetricMetaVersions                   *MetricMetaValue
	BuiltinMetricMetaBadges                     *MetricMetaValue
	BuiltinMetricMetaAutoConfig                 *MetricMetaValue
	BuiltinMetricMetaJournalVersions            *MetricMetaValue
	BuiltinMetricMetaPromScrapeTime             *MetricMetaValue
	BuiltinMetricMetaAgentHeartbeatVersion      *MetricMetaValue
	BuiltinMetricMetaAgentHeartbeatArgs         *MetricMetaValue
	BuiltinMetricMetaUsageMemory                *MetricMetaValue
	BuiltinMetricMetaUsageCPU                   *MetricMetaValue
	BuiltinMetricMetaGeneratorConstCounter      *MetricMetaValue
	BuiltinMetricMetaGeneratorSinCounter        *MetricMetaValue
	BuiltinMetricMetaHeartbeatVersion           *MetricMetaValue
	BuiltinMetricMetaHeartbeatArgs              *MetricMetaValue
	BuiltinMetricMetaAPIBRS                     *MetricMetaValue
	BuiltinMetricMetaBudgetHost                 *MetricMetaValue
	BuiltinMetricMetaBudgetAggregatorHost       *MetricMetaValue
	BuiltinMetricMetaAPIActiveQueries           *MetricMetaValue
	BuiltinMetricMetaRPCRequests                *MetricMetaValue
	BuiltinMetricMetaBudgetUnknownMetric        *MetricMetaValue
	BuiltinMetricMetaContributorsLog            *MetricMetaValue
	BuiltinMetricMetaContributorsLogRev         *MetricMetaValue
	BuiltinMetricMetaGeneratorGapsCounter       *MetricMetaValue
	BuiltinMetricMetaGroupSizeBeforeSampling    *MetricMetaValue
	BuiltinMetricMetaGroupSizeAfterSampling     *MetricMetaValue
	BuiltinMetricMetaAPISelectBytes             *MetricMetaValue
	BuiltinMetricMetaAPISelectRows              *MetricMetaValue
	BuiltinMetricMetaAPISelectDuration          *MetricMetaValue
	BuiltinMetricMetaAgentHistoricQueueSizeSum  *MetricMetaValue
	BuiltinMetricMetaAPISourceSelectRows        *MetricMetaValue
	BuiltinMetricMetaSystemMetricScrapeDuration *MetricMetaValue
	BuiltinMetricMetaMetaServiceTime            *MetricMetaValue
	BuiltinMetricMetaMetaClientWaits            *MetricMetaValue
	BuiltinMetricMetaAgentUDPReceiveBufferSize  *MetricMetaValue
	BuiltinMetricMetaAPIMetricUsage             *MetricMetaValue
	BuiltinMetricMetaAPIServiceTime             *MetricMetaValue
	BuiltinMetricMetaAPIResponseTime            *MetricMetaValue
	BuiltinMetricMetaSrcTestConnection          *MetricMetaValue
	BuiltinMetricMetaAgentAggregatorTimeDiff    *MetricMetaValue
	BuiltinMetricMetaSrcSamplingMetricCount     *MetricMetaValue
	BuiltinMetricMetaAggSamplingMetricCount     *MetricMetaValue
	BuiltinMetricMetaSrcSamplingSizeBytes       *MetricMetaValue
	BuiltinMetricMetaAggSamplingSizeBytes       *MetricMetaValue
	BuiltinMetricMetaUIErrors                   *MetricMetaValue
	BuiltinMetricMetaStatsHouseErrors           *MetricMetaValue
	BuiltinMetricMetaSrcSamplingBudget          *MetricMetaValue
	BuiltinMetricMetaAggSamplingBudget          *MetricMetaValue
	BuiltinMetricMetaSrcSamplingGroupBudget     *MetricMetaValue
	BuiltinMetricMetaAggSamplingGroupBudget     *MetricMetaValue
	BuiltinMetricMetaPromQLEngineTime           *MetricMetaValue
	BuiltinMetricMetaAPICacheHit                *MetricMetaValue
	BuiltinMetricMetaAggScrapeTargetDispatch    *MetricMetaValue
	BuiltinMetricMetaAggScrapeTargetDiscovery   *MetricMetaValue
	BuiltinMetricMetaAggScrapeConfigHash        *MetricMetaValue
	BuiltinMetricMetaAggSamplingTime            *MetricMetaValue
	BuiltinMetricMetaAgentDiskCacheSize         *MetricMetaValue
	BuiltinMetricMetaAggContributors            *MetricMetaValue
	BuiltinMetricMetaAPICacheBytesAlloc         *MetricMetaValue
	BuiltinMetricMetaAPICacheBytesFree          *MetricMetaValue
	BuiltinMetricMetaAPICacheBytesTotal         *MetricMetaValue
	BuiltinMetricMetaAPICacheAgeEvict           *MetricMetaValue
	BuiltinMetricMetaAPICacheAgeTotal           *MetricMetaValue
	BuiltinMetricMetaAPIBufferBytesAlloc        *MetricMetaValue
	BuiltinMetricMetaAPIBufferBytesFree         *MetricMetaValue
	BuiltinMetricMetaAPIBufferBytesTotal        *MetricMetaValue
	BuiltinMetricMetaAutoCreateMetric           *MetricMetaValue
	BuiltinMetricMetaRestartTimings             *MetricMetaValue
	BuiltinMetricMetaGCDuration                 *MetricMetaValue
	BuiltinMetricMetaAggHistoricHostsWaiting    *MetricMetaValue
	BuiltinMetricMetaAggSamplingEngineTime      *MetricMetaValue
	BuiltinMetricMetaAggSamplingEngineKeys      *MetricMetaValue
)

func TagIDTagToTagID(tagIDTag int32) string {
	return tagIDTag2TagID[tagIDTag]
}

func init() {
	for _, g := range BuiltInGroupDefault {
		err := g.RestoreCachedInfo(true)
		if err != nil {
			log.Printf("error to RestoreCachedInfo of %v", *g)
		}
	}
	for _, n := range BuiltInNamespaceDefault {
		err := n.RestoreCachedInfo(true)
		if err != nil {
			log.Printf("error to RestoreCachedInfo of %v", *n)
		}
	}
	for k, v := range hostMetrics {
		v.Tags = append([]MetricMetaTag{{Name: "hostname"}}, v.Tags...)
		if slowHostMetricID[k] {
			v.Resolution = 15
		} else {
			v.Resolution = 15
		}
		v.GroupID = BuiltinGroupIDHost
		v.Group = BuiltInGroupDefault[BuiltinGroupIDHost]
		v.Sharding = []MetricSharding{{Strategy: ShardByMetric}}
		BuiltinMetrics[k] = v
		builtinMetricsAllowedToReceive[k] = true
		metricsWithoutAggregatorID[k] = true
	}
	for i := 0; i < MaxTags; i++ {
		name := strconv.Itoa(i)
		legacyName := legacyTagIDPrefix + name
		tagIDsLegacy = append(tagIDsLegacy, legacyName)
		tagIDs = append(tagIDs, name)
		tagIDToIndex[name] = i
		apiCompatTagID[name] = name
		apiCompatTagID[legacyName] = name
		tagIDTag2TagID[int32(i+TagIDShiftLegacy)] = legacyName
		tagIDTag2TagID[int32(i+TagIDShift)] = tagStringForUI + " " + strconv.Itoa(i) // for UI only
	}
	apiCompatTagID[StringTopTagID] = StringTopTagID
	apiCompatTagID[LegacyStringTopTagID] = StringTopTagID
	tagIDTag2TagID[TagIDShiftLegacy-1] = StringTopTagID
	tagIDTag2TagID[TagIDShift-1] = tagStringForUI + " " + StringTopTagID // for UI only
	tagIDTag2TagID[TagIDShift-2] = tagStringForUI + " " + HostTagID      // for UI only

	BuiltinMetricByName = make(map[string]*MetricMetaValue, len(BuiltinMetrics))
	BuiltinMetricAllowedToReceive = make(map[string]*MetricMetaValue, len(BuiltinMetrics))
	for id, m := range BuiltinMetrics {
		m.MetricID = id
		if m.GroupID == 0 {
			m.GroupID = BuiltinGroupIDBuiltin
			m.Group = BuiltInGroupDefault[BuiltinGroupIDBuiltin]
		}
		m.Visible = !builtinMetricsInvisible[id]
		m.PreKeyFrom = math.MaxInt32 // allow writing, but not yet selecting
		m.Weight = 1

		BuiltinMetricByName[m.Name] = m

		if builtinMetricsAllowedToReceive[m.MetricID] {
			BuiltinMetricAllowedToReceive[m.Name] = m
		}
		if id == BuiltinMetricIDIngestionStatus || id == BuiltinMetricIDAggMappingCreated {
			m.Tags = append([]MetricMetaTag{{Description: "environment"}}, m.Tags...)
		} else {
			m.Tags = append([]MetricMetaTag{{Description: "-"}}, m.Tags...)
		}
		for len(m.Tags) < MaxTags {
			m.Tags = append(m.Tags, MetricMetaTag{Description: "-"})
		}
		if !metricsWithoutAggregatorID[id] {
			m.Tags[AggHostTag] = MetricMetaTag{Description: "aggregator_host"}
			m.Tags[AggShardTag] = MetricMetaTag{Description: "aggregator_shard", Raw: true}
			m.Tags[AggReplicaTag] = MetricMetaTag{Description: "aggregator_replica", Raw: true}
		}
		if _, ok := hostMetrics[id]; ok {
			m.Tags[0] = MetricMetaTag{Description: "env"}
			m.Tags[HostDCTag] = MetricMetaTag{Description: "dc"}
			m.Tags[HostGroupTag] = MetricMetaTag{Description: "group"}
			m.Tags[HostRegionTag] = MetricMetaTag{Description: "region"}
			m.Tags[HostOwnerTag] = MetricMetaTag{Description: "owner"}

		}
		if MetricsWithAgentEnvRouteArch[id] {
			m.Tags[RouteTag] = MetricMetaTag{Description: "route", ValueComments: convertToValueComments(routeToValue)}
			m.Tags[AgentEnvTag] = MetricMetaTag{
				Description: "statshouse_env",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDProduction: "statshouse.production",
					TagValueIDStaging1:   "statshouse.staging1",
					TagValueIDStaging2:   "statshouse.staging2",
					TagValueIDStaging3:   "statshouse.staging3",
				}),
			}
			m.Tags[BuildArchTag] = MetricMetaTag{
				Description:   "build_arch",
				ValueComments: convertToValueComments(buildArchToValue),
			}
		}

		for i, t := range m.Tags {
			if t.Description == "tag_id" {
				m.Tags[i].ValueComments = convertToValueComments(tagIDTag2TagID)
				m.Tags[i].Raw = true
				continue
			}
			if i == 0 { // env is not raw
				continue
			}
			if m.Tags[i].RawKind != "" {
				m.Tags[i].Raw = true
				continue
			}
			if m.Tags[i].Description == "-" && m.Tags[i].Name == "" {
				m.Tags[i].Raw = true
				continue
			}
			if m.Tags[i].IsMetric || m.Tags[i].ValueComments != nil {
				m.Tags[i].Raw = true
			}
			// Also some tags are simply marked with Raw:true above
		}

		// init sharding strategy if it's not explicitly defined
		if len(m.Sharding) == 0 {
			m.Sharding = []MetricSharding{{
				Strategy: ShardFixed,
				Shard:    opt.OUint32(0),
			}}
		}
		_ = m.RestoreCachedInfo()
	}

	BuiltinMetricMetaAgentSamplingFactor = BuiltinMetrics[BuiltinMetricIDAgentSamplingFactor]
	BuiltinMetricMetaAggBucketReceiveDelaySec = BuiltinMetrics[BuiltinMetricIDAggBucketReceiveDelaySec]
	BuiltinMetricMetaAggInsertSize = BuiltinMetrics[BuiltinMetricIDAggInsertSize]
	BuiltinMetricMetaTLByteSizePerInflightType = BuiltinMetrics[BuiltinMetricIDTLByteSizePerInflightType]
	BuiltinMetricMetaAggKeepAlive = BuiltinMetrics[BuiltinMetricIDAggKeepAlive]
	BuiltinMetricMetaAggSizeCompressed = BuiltinMetrics[BuiltinMetricIDAggSizeCompressed]
	BuiltinMetricMetaAggSizeUncompressed = BuiltinMetrics[BuiltinMetricIDAggSizeUncompressed]
	BuiltinMetricMetaAggAdditionsToEstimator = BuiltinMetrics[BuiltinMetricIDAggAdditionsToEstimator]
	BuiltinMetricMetaAggHourCardinality = BuiltinMetrics[BuiltinMetricIDAggHourCardinality]
	BuiltinMetricMetaAggSamplingFactor = BuiltinMetrics[BuiltinMetricIDAggSamplingFactor]
	BuiltinMetricMetaIngestionStatus = BuiltinMetrics[BuiltinMetricIDIngestionStatus]
	BuiltinMetricMetaAggInsertTime = BuiltinMetrics[BuiltinMetricIDAggInsertTime]
	BuiltinMetricMetaAggHistoricBucketsWaiting = BuiltinMetrics[BuiltinMetricIDAggHistoricBucketsWaiting]
	BuiltinMetricMetaAggBucketAggregateTimeSec = BuiltinMetrics[BuiltinMetricIDAggBucketAggregateTimeSec]
	BuiltinMetricMetaAggActiveSenders = BuiltinMetrics[BuiltinMetricIDAggActiveSenders]
	BuiltinMetricMetaAggOutdatedAgents = BuiltinMetrics[BuiltinMetricIDAggOutdatedAgents]
	BuiltinMetricMetaAgentDiskCacheErrors = BuiltinMetrics[BuiltinMetricIDAgentDiskCacheErrors]
	BuiltinMetricMetaTimingErrors = BuiltinMetrics[BuiltinMetricIDTimingErrors]
	BuiltinMetricMetaAgentReceivedBatchSize = BuiltinMetrics[BuiltinMetricIDAgentReceivedBatchSize]
	BuiltinMetricMetaAggMapping = BuiltinMetrics[BuiltinMetricIDAggMapping]
	BuiltinMetricMetaAggInsertTimeReal = BuiltinMetrics[BuiltinMetricIDAggInsertTimeReal]
	BuiltinMetricMetaAgentHistoricQueueSize = BuiltinMetrics[BuiltinMetricIDAgentHistoricQueueSize]
	BuiltinMetricMetaAggHistoricSecondsWaiting = BuiltinMetrics[BuiltinMetricIDAggHistoricSecondsWaiting]
	BuiltinMetricMetaAggInsertSizeReal = BuiltinMetrics[BuiltinMetricIDAggInsertSizeReal]
	BuiltinMetricMetaAgentMapping = BuiltinMetrics[BuiltinMetricIDAgentMapping]
	BuiltinMetricMetaAgentReceivedPacketSize = BuiltinMetrics[BuiltinMetricIDAgentReceivedPacketSize]
	BuiltinMetricMetaAggMappingCreated = BuiltinMetrics[BuiltinMetricIDAggMappingCreated]
	BuiltinMetricMetaVersions = BuiltinMetrics[BuiltinMetricIDVersions]
	BuiltinMetricMetaBadges = BuiltinMetrics[BuiltinMetricIDBadges]
	BuiltinMetricMetaAutoConfig = BuiltinMetrics[BuiltinMetricIDAutoConfig]
	BuiltinMetricMetaJournalVersions = BuiltinMetrics[BuiltinMetricIDJournalVersions]
	BuiltinMetricMetaPromScrapeTime = BuiltinMetrics[BuiltinMetricIDPromScrapeTime]
	BuiltinMetricMetaAgentHeartbeatVersion = BuiltinMetrics[BuiltinMetricIDAgentHeartbeatVersion]
	BuiltinMetricMetaAgentHeartbeatArgs = BuiltinMetrics[BuiltinMetricIDAgentHeartbeatArgs]
	BuiltinMetricMetaUsageMemory = BuiltinMetrics[BuiltinMetricIDUsageMemory]
	BuiltinMetricMetaUsageCPU = BuiltinMetrics[BuiltinMetricIDUsageCPU]
	BuiltinMetricMetaGeneratorConstCounter = BuiltinMetrics[BuiltinMetricIDGeneratorConstCounter]
	BuiltinMetricMetaGeneratorSinCounter = BuiltinMetrics[BuiltinMetricIDGeneratorSinCounter]
	BuiltinMetricMetaHeartbeatVersion = BuiltinMetrics[BuiltinMetricIDHeartbeatVersion]
	BuiltinMetricMetaHeartbeatArgs = BuiltinMetrics[BuiltinMetricIDHeartbeatArgs]
	BuiltinMetricMetaAPIBRS = BuiltinMetrics[BuiltinMetricIDAPIBRS]
	BuiltinMetricMetaBudgetHost = BuiltinMetrics[BuiltinMetricIDBudgetHost]
	BuiltinMetricMetaBudgetAggregatorHost = BuiltinMetrics[BuiltinMetricIDBudgetAggregatorHost]
	BuiltinMetricMetaAPIActiveQueries = BuiltinMetrics[BuiltinMetricIDAPIActiveQueries]
	BuiltinMetricMetaRPCRequests = BuiltinMetrics[BuiltinMetricIDRPCRequests]
	BuiltinMetricMetaBudgetUnknownMetric = BuiltinMetrics[BuiltinMetricIDBudgetUnknownMetric]
	BuiltinMetricMetaContributorsLog = BuiltinMetrics[BuiltinMetricIDContributorsLog]
	BuiltinMetricMetaContributorsLogRev = BuiltinMetrics[BuiltinMetricIDContributorsLogRev]
	BuiltinMetricMetaGeneratorGapsCounter = BuiltinMetrics[BuiltinMetricIDGeneratorGapsCounter]
	BuiltinMetricMetaGroupSizeBeforeSampling = BuiltinMetrics[BuiltinMetricIDGroupSizeBeforeSampling]
	BuiltinMetricMetaGroupSizeAfterSampling = BuiltinMetrics[BuiltinMetricIDGroupSizeAfterSampling]
	BuiltinMetricMetaAPISelectBytes = BuiltinMetrics[BuiltinMetricIDAPISelectBytes]
	BuiltinMetricMetaAPISelectRows = BuiltinMetrics[BuiltinMetricIDAPISelectRows]
	BuiltinMetricMetaAPISelectDuration = BuiltinMetrics[BuiltinMetricIDAPISelectDuration]
	BuiltinMetricMetaAgentHistoricQueueSizeSum = BuiltinMetrics[BuiltinMetricIDAgentHistoricQueueSizeSum]
	BuiltinMetricMetaAPISourceSelectRows = BuiltinMetrics[BuiltinMetricIDAPISourceSelectRows]
	BuiltinMetricMetaSystemMetricScrapeDuration = BuiltinMetrics[BuiltinMetricIDSystemMetricScrapeDuration]
	BuiltinMetricMetaMetaServiceTime = BuiltinMetrics[BuiltinMetricIDMetaServiceTime]
	BuiltinMetricMetaMetaClientWaits = BuiltinMetrics[BuiltinMetricIDMetaClientWaits]
	BuiltinMetricMetaAgentUDPReceiveBufferSize = BuiltinMetrics[BuiltinMetricIDAgentUDPReceiveBufferSize]
	BuiltinMetricMetaAPIMetricUsage = BuiltinMetrics[BuiltinMetricIDAPIMetricUsage]
	BuiltinMetricMetaAPIServiceTime = BuiltinMetrics[BuiltinMetricIDAPIServiceTime]
	BuiltinMetricMetaAPIResponseTime = BuiltinMetrics[BuiltinMetricIDAPIResponseTime]
	BuiltinMetricMetaSrcTestConnection = BuiltinMetrics[BuiltinMetricIDSrcTestConnection]
	BuiltinMetricMetaAgentAggregatorTimeDiff = BuiltinMetrics[BuiltinMetricIDAgentAggregatorTimeDiff]
	BuiltinMetricMetaSrcSamplingMetricCount = BuiltinMetrics[BuiltinMetricIDSrcSamplingMetricCount]
	BuiltinMetricMetaAggSamplingMetricCount = BuiltinMetrics[BuiltinMetricIDAggSamplingMetricCount]
	BuiltinMetricMetaSrcSamplingSizeBytes = BuiltinMetrics[BuiltinMetricIDSrcSamplingSizeBytes]
	BuiltinMetricMetaAggSamplingSizeBytes = BuiltinMetrics[BuiltinMetricIDAggSamplingSizeBytes]
	BuiltinMetricMetaUIErrors = BuiltinMetrics[BuiltinMetricIDUIErrors]
	BuiltinMetricMetaStatsHouseErrors = BuiltinMetrics[BuiltinMetricIDStatsHouseErrors]
	BuiltinMetricMetaSrcSamplingBudget = BuiltinMetrics[BuiltinMetricIDSrcSamplingBudget]
	BuiltinMetricMetaAggSamplingBudget = BuiltinMetrics[BuiltinMetricIDAggSamplingBudget]
	BuiltinMetricMetaSrcSamplingGroupBudget = BuiltinMetrics[BuiltinMetricIDSrcSamplingGroupBudget]
	BuiltinMetricMetaAggSamplingGroupBudget = BuiltinMetrics[BuiltinMetricIDAggSamplingGroupBudget]
	BuiltinMetricMetaPromQLEngineTime = BuiltinMetrics[BuiltinMetricIDPromQLEngineTime]
	BuiltinMetricMetaAPICacheHit = BuiltinMetrics[BuiltinMetricIDAPICacheHit]
	BuiltinMetricMetaAggScrapeTargetDispatch = BuiltinMetrics[BuiltinMetricIDAggScrapeTargetDispatch]
	BuiltinMetricMetaAggScrapeTargetDiscovery = BuiltinMetrics[BuiltinMetricIDAggScrapeTargetDiscovery]
	BuiltinMetricMetaAggScrapeConfigHash = BuiltinMetrics[BuiltinMetricIDAggScrapeConfigHash]
	BuiltinMetricMetaAggSamplingTime = BuiltinMetrics[BuiltinMetricIDAggSamplingTime]
	BuiltinMetricMetaAgentDiskCacheSize = BuiltinMetrics[BuiltinMetricIDAgentDiskCacheSize]
	BuiltinMetricMetaAggContributors = BuiltinMetrics[BuiltinMetricIDAggContributors]
	BuiltinMetricMetaAPICacheBytesAlloc = BuiltinMetrics[BuiltinMetricIDAPICacheBytesAlloc]
	BuiltinMetricMetaAPICacheBytesFree = BuiltinMetrics[BuiltinMetricIDAPICacheBytesFree]
	BuiltinMetricMetaAPICacheBytesTotal = BuiltinMetrics[BuiltinMetricIDAPICacheBytesTotal]
	BuiltinMetricMetaAPICacheAgeEvict = BuiltinMetrics[BuiltinMetricIDAPICacheAgeEvict]
	BuiltinMetricMetaAPICacheAgeTotal = BuiltinMetrics[BuiltinMetricIDAPICacheAgeTotal]
	BuiltinMetricMetaAPIBufferBytesAlloc = BuiltinMetrics[BuiltinMetricIDAPIBufferBytesAlloc]
	BuiltinMetricMetaAPIBufferBytesFree = BuiltinMetrics[BuiltinMetricIDAPIBufferBytesFree]
	BuiltinMetricMetaAPIBufferBytesTotal = BuiltinMetrics[BuiltinMetricIDAPIBufferBytesTotal]
	BuiltinMetricMetaAutoCreateMetric = BuiltinMetrics[BuiltinMetricIDAutoCreateMetric]
	BuiltinMetricMetaRestartTimings = BuiltinMetrics[BuiltinMetricIDRestartTimings]
	BuiltinMetricMetaGCDuration = BuiltinMetrics[BuiltinMetricIDGCDuration]
	BuiltinMetricMetaAggHistoricHostsWaiting = BuiltinMetrics[BuiltinMetricIDAggHistoricHostsWaiting]
	BuiltinMetricMetaAggSamplingEngineTime = BuiltinMetrics[BuiltinMetricIDAggSamplingEngineTime]
	BuiltinMetricMetaAggSamplingEngineKeys = BuiltinMetrics[BuiltinMetricIDAggSamplingEngineKeys]
}

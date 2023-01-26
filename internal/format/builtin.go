// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package format

import (
	"math"
	"strconv"
)

const (
	TagIDShift       = 100 // "0" is set to 100, "1" to 101, "_s" to 99, "_h" to 98, leaving space in both directions
	TagIDShiftLegacy = 2   // in the past we used this shift, so it got into DB

	tagStringForUI = "tag"

	BuiltinGroupIDDefault = -1 // for all metrics with group not known. We want to edit it in the future, so not 0
	BuiltinGroupIDBuiltin = -2 // for all built in metrics

	BuiltinMetricIDAgentSamplingFactor        = -1
	BuiltinMetricIDAggBucketReceiveDelaySec   = -2 // Also approximates insert delay, interesting for historic buckets
	BuiltinMetricIDAggInsertSize              = -3 // If all contributors come on time, count will be 1 (per shard). If some come through historic conveyor, can be larger.
	BuiltinMetricIDTLByteSizePerInflightType  = -4
	BuiltinMetricIDAggKeepAlive               = -5 // How many keep-alive were among contributors
	BuiltinMetricIDAggSizeCompressed          = -6
	BuiltinMetricIDAggSizeUncompressed        = -7
	BuiltinMetricIDAggAdditionsToEstimator    = -8 // How many new tags were inserted into bucket
	BuiltinMetricIDAggHourCardinality         = -9
	BuiltinMetricIDAggSamplingFactor          = -10
	BuiltinMetricIDIngestionStatus            = -11
	BuiltinMetricIDAggInsertTime              = -12
	BuiltinMetricIDAggHistoricBucketsWaiting  = -13
	BuiltinMetricIDAggBucketAggregateTimeSec  = -14
	BuiltinMetricIDAggActiveSenders           = -15
	BuiltinMetricIDAgentDiskCacheErrors       = -18
	BuiltinMetricIDTimingErrors               = -20
	BuiltinMetricIDAgentReceivedBatchSize     = -21
	BuiltinMetricIDAggMapping                 = -23
	BuiltinMetricIDAggInsertTimeReal          = -24
	BuiltinMetricIDAgentHistoricQueueSize     = -25
	BuiltinMetricIDAggHistoricSecondsWaiting  = -26
	BuiltinMetricIDAggInsertSizeReal          = -27
	BuiltinMetricIDAgentPerMetricSampleBudget = -29 // Not informative - TODO improve or remove
	BuiltinMetricIDAgentMapping               = -30
	BuiltinMetricIDAgentReceivedPacketSize    = -31
	BuiltinMetricIDAggMappingCreated          = -33
	BuiltinMetricIDVersions                   = -34
	BuiltinMetricIDBadges                     = -35 // copy of some other metrics for efficient show of errors and warnings
	BuiltinMetricIDAutoConfig                 = -36
	BuiltinMetricIDJournalVersions            = -37 // has smart custom sending logic
	BuiltinMetricIDPromScrapeTime             = -38
	BuiltinMetricIDAgentHeartbeatVersion      = -41 // TODO - remove
	BuiltinMetricIDAgentHeartbeatArgs         = -42 // TODO - remove, this metric was writing larger than allowed strings to DB in the past
	BuiltinMetricIDUsageMemory                = -43
	BuiltinMetricIDUsageCPU                   = -44
	BuiltinMetricIDGeneratorConstCounter      = -45
	BuiltinMetricIDGeneratorSinCounter        = -46
	BuiltinMetricIDHeartbeatVersion           = -47
	BuiltinMetricIDHeartbeatArgs              = -48 // this metric was writing larger than allowed strings to DB in the past
	BuiltinMetricIDAPIRPCServiceTime          = -49 // TODO - harmonize 3 timing API metrics into single new one with protocol
	BuiltinMetricIDAPIBRS                     = -50
	BuiltinMetricIDAPIEndpointResponseTime    = -51
	BuiltinMetricIDAPIEndpointServiceTime     = -52
	BuiltinMetricIDBudgetHost                 = -53 // these 2 metrics are invisible, but host mapping is flood-protected by their names
	BuiltinMetricIDBudgetAggregatorHost       = -54 // we want to see limits properly credited in flood meta metric tags
	BuiltinMetricIDAPIActiveQueries           = -55
	BuiltinMetricIDRPCRequests                = -56
	BuiltinMetricIDBudgetUnknownMetric        = -57
	BuiltinMetricIDHeartbeatArgs2             = -58 // if args do not fit into BuiltinMetricIDHeartbeatArgs, we put tail here.
	BuiltinMetricIDHeartbeatArgs3             = -59 // ...
	BuiltinMetricIDHeartbeatArgs4             = -60 // OK, enough!
	BuiltinMetricIDContributorsLog            = -61
	BuiltinMetricIDContributorsLogRev         = -62
	BuiltinMetricIDGeneratorGapsCounter       = -63
	BuiltinMetricIDGroupSizeBeforeSampling    = -64
	BuiltinMetricIDGroupSizeAfterSampling     = -65
	BuiltinMetricIDAPISelectBytes             = -66
	BuiltinMetricIDAPISelectRows              = -67
	BuiltinMetricIDAPISelectDuration          = -68
	BuiltinMetricIDAgentHistoricQueueSizeSum  = -69

	// metric names used in code directly
	BuiltinMetricNameAggBucketReceiveDelaySec = "__agg_bucket_receive_delay_sec"
	BuiltinMetricNameAgentSamplingFactor      = "__src_sampling_factor"
	BuiltinMetricNameAggSamplingFactor        = "__agg_sampling_factor"
	BuiltinMetricNameIngestionStatus          = "__src_ingestion_status"
	BuiltinMetricNameAggMappingCreated        = "__agg_mapping_created"
	BuiltinMetricNameBadges                   = "__badges"
	BuiltinMetricNamePromScrapeTime           = "__prom_scrape_time"
	BuiltinMetricNameAPIRPCServiceTime        = "__api_rpc_service_time"
	BuiltinMetricNameUsageMemory              = "__usage_mem"
	BuiltinMetricNameUsageCPU                 = "__usage_cpu"
	BuiltinMetricNameAPIBRS                   = "__api_big_response_storage_size"
	BuiltinMetricNameAPISelectBytes           = "__api_ch_select_bytes"
	BuiltinMetricNameAPISelectRows            = "__api_ch_select_rows"
	BuiltinMetricNameAPISelectDuration        = "__api_ch_select_duration"
	BuiltinMetricNameAPIEndpointResponseTime  = "__api_endpoint_response_time"
	BuiltinMetricNameAPIEndpointServiceTime   = "__api_endpoint_service_time"
	BuiltinMetricNameBudgetHost               = "__budget_host"
	BuiltinMetricNameBudgetAggregatorHost     = "__budget_aggregator_host"
	BuiltinMetricNameAPIActiveQueries         = "__api_active_queries"
	BuiltinMetricNameBudgetUnknownMetric      = "__budget_unknown_metric"

	TagValueIDBadgeIngestionErrorsOld  = -11 // remove from API, then stop writing
	TagValueIDBadgeAggMappingErrorsOld = -33 // remove from API, then stop writing
	TagValueIDBadgeAgentSamplingFactor = -1
	TagValueIDBadgeAggSamplingFactor   = -10
	TagValueIDBadgeIngestionErrors     = 1
	TagValueIDBadgeAggMappingErrors    = 2
	TagValueIDBadgeContributors        = 3 // # of agents who sent this second. Hyper important to distinguish between holes in your data and problems with agents (no connectivity, etc.).

	TagValueIDRPCRequestsStatusOK          = 1
	TagValueIDRPCRequestsStatusErrLocal    = 2
	TagValueIDRPCRequestsStatusErrUpstream = 3
	TagValueIDRPCRequestsStatusHijack      = 4
	TagValueIDRPCRequestsStatusNoHandler   = 5

	TagValueIDProduction = 1
	TagValueIDStaging    = 2

	TagValueIDAPILaneFastLight = 1
	TagValueIDAPILaneFastHeavy = 2
	TagValueIDAPILaneSlowLight = 3
	TagValueIDAPILaneSlowHeavy = 4

	TagValueIDConveyorRecent   = 1
	TagValueIDConveyorHistoric = 2

	TagValueIDAggregatorOriginal = 1
	TagValueIDAggregatorSpare    = 2

	TagValueIDTimingFutureBucketRecent         = 1
	TagValueIDTimingFutureBucketHistoric       = 2
	TagValueIDTimingLateRecent                 = 3
	TagValueIDTimingLongWindowThrownAgent      = 4
	TagValueIDTimingLongWindowThrownAggregator = 5
	TagValueIDTimingMissedSeconds              = 6

	TagValueIDRouteDirect       = 1
	TagValueIDRouteIngressProxy = 2

	TagValueIDSecondReal    = 1
	TagValueIDSecondPhantom = 2

	TagValueIDInsertTimeOK    = 1
	TagValueIDInsertTimeError = 2

	TagValueIDHistoricQueueMemory = 1
	TagValueIDHistoricQueueDisk   = 2

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
	TagValueIDSrcIngestionStatusErrMapInvalidRawTagValue     = 34
	TagValueIDSrcIngestionStatusErrMapTagValueCached         = 35
	TagValueIDSrcIngestionStatusErrMapTagValue               = 36
	TagValueIDSrcIngestionStatusErrMapGlobalQueueOverload    = 37
	TagValueIDSrcIngestionStatusErrMapPerMetricQueueOverload = 38
	TagValueIDSrcIngestionStatusErrMapTagValueEncoding       = 39
	TagValueIDSrcIngestionStatusOKLegacy                     = 40 // never written, for historic data
	TagValueIDSrcIngestionStatusErrMetricNonCanonical        = 41 // never written, for historic data
	TagValueIDSrcIngestionStatusErrMetricInvisible           = 42
	TagValueIDSrcIngestionStatusErrLegacyProtocol            = 43 // we stopped adding metrics to conveyor via legacy protocol
	TagValueIDSrcIngestionStatusWarnDeprecatedT              = 44
	TagValueIDSrcIngestionStatusWarnDeprecatedStop           = 45
	TagValueIDSrcIngestionStatusWarnMapTagSetTwice           = 46
	TagValueIDSrcIngestionStatusWarnDeprecatedKeyName        = 47
	TagValueIDSrcIngestionStatusErrMetricNameEncoding        = 48
	TagValueIDSrcIngestionStatusErrMapTagNameEncoding        = 49
	TagValueIDSrcIngestionStatusErrValueUniqueBothSet        = 50
	TagValueIDSrcIngestionStatusWarnOldCounterSemantic       = 51

	TagValueIDPacketFormatLegacy   = 1
	TagValueIDPacketFormatTL       = 2
	TagValueIDPacketFormatMsgPack  = 3
	TagValueIDPacketFormatJSON     = 4
	TagValueIDPacketFormatProtobuf = 5
	TagValueIDPacketFormatRPC      = 6
	TagValueIDPacketFormatEmpty    = 7

	TagValueIDAgentReceiveStatusOK    = 1
	TagValueIDAgentReceiveStatusError = 2

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

	TagValueIDCPUUsageUser = 1
	TagValueIDCPUUsageSys  = 2

	TagValueIDHeartbeatEventStart     = 1
	TagValueIDHeartbeatEventHeartbeat = 2

	TagValueIDBuildArchAMD64 = 1
	TagValueIDBuildArch386   = 2
	TagValueIDBuildArchARM64 = 3
	TagValueIDBuildArchARM   = 4
)

var (
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
				}),
			}, {
				Description: "tag_id",
			}},
			PreKeyTagID: "1",
		},
		BuiltinMetricIDAggInsertTime: {
			Name:        "__agg_insert_time",
			Kind:        MetricKindValue,
			Description: "Time inserting this second into clickhouse took. Written when second is inserted, which can be much later.",
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
			Tags: []MetricMetaTag{{
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description: "-",
			}, {
				Description:   "aggregator_role",
				ValueComments: convertToValueComments(aggregatorRoleToValue),
			}},
		},
		BuiltinMetricIDAggBucketAggregateTimeSec: {
			Name: "__agg_bucket_aggregate_time_sec",
			Kind: MetricKindValue,
			Description: `Time between agent bucket is received and fully aggregated into aggregator bucket.
Set by aggregator. Max(value)@host shows agent responsible for longest aggregation.`,
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
					TagValueIDTimingFutureBucketRecent:         "clock_future_recent",
					TagValueIDTimingFutureBucketHistoric:       "clock_future_historic",
					TagValueIDTimingLateRecent:                 "late_recent",
					TagValueIDTimingLongWindowThrownAgent:      "out_of_window_agent",
					TagValueIDTimingLongWindowThrownAggregator: "out_of_window_aggregator",
					TagValueIDTimingMissedSeconds:              "missed_seconds",
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
			Tags: []MetricMetaTag{{
				Description:   "format",
				ValueComments: convertToValueComments(packetFormatToValue),
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAgentReceiveStatusOK:    "ok",
					TagValueIDAgentReceiveStatusError: "error",
				}),
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
		BuiltinMetricIDAgentHistoricQueueSize: {
			Name:        "__src_historic_queue_size_bytes",
			Kind:        MetricKindValue,
			Description: "Historic queue size in memory and on disk.\nDisk size increases when second is written, decreases when file is deleted.",
			Tags: []MetricMetaTag{{
				Description: "storage",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDHistoricQueueMemory: "memory",
					TagValueIDHistoricQueueDisk:   "disk",
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
			Tags: []MetricMetaTag{{
				Description: "storage",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDHistoricQueueMemory: "memory",
					TagValueIDHistoricQueueDisk:   "disk",
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
			}},
		},
		BuiltinMetricIDAggInsertSizeReal: {
			Name:        "__agg_insert_size_real",
			Kind:        MetricKindValue,
			Description: "Size of aggregated bucket inserted into clickhouse in this second (actual seconds inserted can be from the past).",
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
		BuiltinMetricIDAgentPerMetricSampleBudget: {
			Name:        "__src_per_metric_sample_budget_bytes",
			Kind:        MetricKindValue,
			Description: "Agent sampling budget per sampled metric, or remaining budget if none were sampled.",
			Tags: []MetricMetaTag{{
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAgentFirstSampledMetricBudgetPerMetric: "per_metric",
					TagValueIDAgentFirstSampledMetricBudgetUnused:    "remaining",
				}),
			}},
		},
		BuiltinMetricIDAgentMapping: {
			Name:        "__src_mapping_time",
			Kind:        MetricKindValue,
			Description: "Time and status of mapping request.\nWritten by agent.",
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
			Tags: []MetricMetaTag{{
				Description:   "format",
				ValueComments: convertToValueComments(packetFormatToValue),
			}, {
				Description: "status",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDAgentReceiveStatusOK:    "ok",
					TagValueIDAgentReceiveStatusError: "error",
				}),
			}},
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
				Description: "-",
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
					TagValueIDBadgeIngestionErrorsOld:  "ingestion_errors_legacy",
					TagValueIDBadgeAggMappingErrorsOld: "mapping_errors_legacy",
					TagValueIDBadgeAgentSamplingFactor: "agent_sampling_actor",
					TagValueIDBadgeAggSamplingFactor:   "aggregator_sampling_actor",
					TagValueIDBadgeIngestionErrors:     "ingestion_errors",
					TagValueIDBadgeAggMappingErrors:    "mapping_errors",
					TagValueIDBadgeContributors:        "contributors",
				}),
			}, {
				Description: "metric",
				IsMetric:    true,
			}},
			PreKeyTagID: "2",
		},
		BuiltinMetricIDAutoConfig: {
			Name:        "__autoconfig",
			Kind:        MetricKindCounter,
			Description: "Status of agent get config message, used to configure sharding on agents.\nSet by aggregator.",
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
			}},
		},
		BuiltinMetricIDPromScrapeTime: {
			Name:        BuiltinMetricNamePromScrapeTime,
			Kind:        MetricKindValue,
			Description: "Time of scraping prom metrics",
			Tags: []MetricMetaTag{
				{
					Description: "-",
				}, {
					Description: "job",
				}, {
					Description: "instance",
				}, {
					Description: "err",
					ValueComments: convertToValueComments(map[int32]string{
						TagValueIDScrapeError: "scrape",
					}),
				},
			},
		},
		BuiltinMetricIDUsageMemory: {
			Name:        BuiltinMetricNameUsageMemory,
			Kind:        MetricKindValue,
			Description: "Memory usage of statshouse components.",
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
			Name:                 "__heartbeat_version",
			Kind:                 MetricKindValue,
			Description:          "Heartbeat value is uptime",
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
				Description: "-",
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
			}},
		},
		BuiltinMetricIDHeartbeatArgs: createBuiltinMetricIDHeartbeatArgs("__heartbeat_args",
			"Commandline of statshouse components.\nIf too long, continued in __heartbeat_args2, __heartbeat_args3, __heartbeat_args4.\nHeartbeat value is uptime."),
		BuiltinMetricIDHeartbeatArgs2: createBuiltinMetricIDHeartbeatArgs("__heartbeat_args2",
			"Commandline of statshouse components.\nContinuation of __heartbeat_args.\nHeartbeat value is uptime."),
		BuiltinMetricIDHeartbeatArgs3: createBuiltinMetricIDHeartbeatArgs("__heartbeat_args3",
			"Commandline of statshouse components.\nContinuation of __heartbeat_args.\nHeartbeat value is uptime."),
		BuiltinMetricIDHeartbeatArgs4: createBuiltinMetricIDHeartbeatArgs("__heartbeat_args4",
			"Commandline of statshouse components.\nContinuation of __heartbeat_args.\nHeartbeat value is uptime."),
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
		BuiltinMetricIDAPIRPCServiceTime: { // TODO - harmonize
			Name:        BuiltinMetricNameAPIRPCServiceTime,
			Kind:        MetricKindValue,
			Description: "Time to handle RPC query by API.",
			Tags: []MetricMetaTag{{
				Description: "methods",
			}, {
				Description: "error_code",
				Raw:         true,
			}, {
				Description: "token_name",
			}, {
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
		BuiltinMetricIDAPIEndpointResponseTime: { // TODO - harmonize
			Name:        BuiltinMetricNameAPIEndpointResponseTime,
			Kind:        MetricKindValue,
			Description: "Time to handle and respond to HTTP query by API",
			Tags: []MetricMetaTag{{
				Description: "endpoint",
			}, {
				Description: "metric",
				IsMetric:    true,
			}, {
				Description: "http_code",
			}, {
				Description: "token_name",
			}, {
				Description: "data_format",
			}, {
				Description: "method",
			}},
		},
		BuiltinMetricIDAPIEndpointServiceTime: {
			Name:        BuiltinMetricNameAPIEndpointServiceTime,
			Kind:        MetricKindValue,
			Description: "Time to handle HTTP query by API",
			Tags: []MetricMetaTag{{
				Description: "endpoint",
			}, {
				Description: "metric",
				IsMetric:    true,
			}, {
				Description: "http_code",
			}, {
				Description: "token_name",
			}, {
				Description: "data_format",
			}, {
				Description: "method",
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
					TagValueIDAPILaneFastLight: "fastlight",
					TagValueIDAPILaneFastHeavy: "fastheavy",
					TagValueIDAPILaneSlowLight: "slowlight",
					TagValueIDAPILaneSlowHeavy: "slowheavy"}),
			}, {
				Description: "host",
			}},
		},
		BuiltinMetricIDRPCRequests: {
			Name:        "__rpc_request_size",
			Kind:        MetricKindValue,
			Description: "Size of RPC request bodies.\nFor ingress proxy, key_id can be used to identify senders.",
			Tags: []MetricMetaTag{{
				Description:   "component",
				ValueComments: convertToValueComments(componentToValue),
			}, {
				Description: "tag",
				RawKind:     "hex",
				ValueComments: convertToValueComments(map[int32]string{
					0x3285ff57: "getConfig", // TODO - remove, legacy
					0x4285ff57: "getConfig2",
					0x3285ff54: "getMetrics",  // TODO - remove, legacy
					0x4285ff54: "getMetrics2", // TODO - remove, legacy
					0x42855554: "getMetrics3",
					0x3285ff56: "getTagMapping", // TODO - remove, legacy
					0x4285ff56: "getTagMapping2",
					0x75a7f68e: "getTagMappingBootstrap",
					0x51df72a3: "getTargets", // TODO - remove, legacy
					0x41df72a3: "getTargets2",
					0x3285ff53: "sendKeepAlive", // TODO - remove, legacy
					0x4285ff53: "sendKeepAlive2",
					// int32(0x94575940): "sendSourceBucket", TODO - remove, legacy (will not compile, stupid golang)
					0x44575940: "sendSourceBucket2",
					0x3285ff58: "testConnection", // TODO - remove, legacy
					0x4285ff58: "testConnection2",
					0x28bea524: "autoCreate", // TODO - remove, legacy
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
			}},
		},
		BuiltinMetricIDContributorsLog: {
			Name: "__contributors_log",
			Kind: MetricKindCounter,
			Description: `Used to invalidate API caches.
Timestamps of all inserted seconds per second are recorded here.
To see which seconds change when, use __contributors_log_rev`,
			Tags: []MetricMetaTag{{
				Description: "timestamp",
				RawKind:     "timestamp",
			}},
		},
		BuiltinMetricIDContributorsLogRev: {
			Name:        "__contributors_log_rev",
			Kind:        MetricKindValue,
			Description: "Reverse index of __contributors_log, used to invalidate API caches.\nValue (and key1) is UNIX timestamp of second when this second was changed.",
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
	}

	builtinMetricsInvisible = map[int32]bool{
		BuiltinMetricIDBudgetHost:           true,
		BuiltinMetricIDBudgetAggregatorHost: true,
		BuiltinMetricIDBudgetUnknownMetric:  true,
	}

	// API and metadata sends this metrics via local statshouse instance
	builtinMetricsAllowedToReceive = map[int32]bool{
		BuiltinMetricIDTimingErrors:            true,
		BuiltinMetricIDPromScrapeTime:          true,
		BuiltinMetricIDAPIRPCServiceTime:       true,
		BuiltinMetricIDAPIBRS:                  true,
		BuiltinMetricIDAPIEndpointResponseTime: true,
		BuiltinMetricIDAPIEndpointServiceTime:  true,
		BuiltinMetricIDUsageMemory:             true,
		BuiltinMetricIDUsageCPU:                true,
		BuiltinMetricIDAPIActiveQueries:        true,
		BuiltinMetricIDAPISelectRows:           true,
		BuiltinMetricIDAPISelectBytes:          true,
		BuiltinMetricIDAPISelectDuration:       true,
	}

	metricsWithAgentEnvRouteArch = map[int32]bool{
		BuiltinMetricIDAgentDiskCacheErrors:       true,
		BuiltinMetricIDTimingErrors:               true,
		BuiltinMetricIDAgentMapping:               true,
		BuiltinMetricIDJournalVersions:            true,
		BuiltinMetricIDTLByteSizePerInflightType:  true,
		BuiltinMetricIDIngestionStatus:            true,
		BuiltinMetricIDAgentPerMetricSampleBudget: true,
		BuiltinMetricIDAgentReceivedBatchSize:     true,
		BuiltinMetricIDAgentReceivedPacketSize:    true,
		BuiltinMetricIDAggSizeCompressed:          true,
		BuiltinMetricIDAggSizeUncompressed:        true,
		BuiltinMetricIDAggBucketReceiveDelaySec:   true,
		BuiltinMetricIDAggBucketAggregateTimeSec:  true,
		BuiltinMetricIDAggAdditionsToEstimator:    true,
		BuiltinMetricIDAgentHistoricQueueSize:     true,
		BuiltinMetricIDVersions:                   true,
		BuiltinMetricIDAggKeepAlive:               true,
		BuiltinMetricIDAggMappingCreated:          true,
		BuiltinMetricIDUsageMemory:                true,
		BuiltinMetricIDUsageCPU:                   true,
		BuiltinMetricIDHeartbeatVersion:           true,
		BuiltinMetricIDHeartbeatArgs:              true,
		BuiltinMetricIDHeartbeatArgs2:             true,
		BuiltinMetricIDHeartbeatArgs3:             true,
		BuiltinMetricIDHeartbeatArgs4:             true,
	}

	metricsWithoutAggregatorID = map[int32]bool{
		BuiltinMetricIDTLByteSizePerInflightType:  true,
		BuiltinMetricIDIngestionStatus:            true,
		BuiltinMetricIDAgentDiskCacheErrors:       true,
		BuiltinMetricIDAgentReceivedBatchSize:     true,
		BuiltinMetricIDAgentPerMetricSampleBudget: true,
		BuiltinMetricIDAgentMapping:               true,
		BuiltinMetricIDAgentReceivedPacketSize:    true,
		BuiltinMetricIDBadges:                     true,
		BuiltinMetricIDPromScrapeTime:             true,
		BuiltinMetricIDGeneratorConstCounter:      true,
		BuiltinMetricIDGeneratorSinCounter:        true,
		BuiltinMetricIDAPIRPCServiceTime:          true,
		BuiltinMetricIDAPIBRS:                     true,
		BuiltinMetricIDAPISelectRows:              true,
		BuiltinMetricIDAPISelectBytes:             true,
		BuiltinMetricIDAPISelectDuration:          true,
		BuiltinMetricIDAPIEndpointResponseTime:    true,
		BuiltinMetricIDAPIEndpointServiceTime:     true,
		BuiltinMetricIDAPIActiveQueries:           true,
		BuiltinMetricIDBudgetHost:                 true,
		BuiltinMetricIDBudgetAggregatorHost:       true,
	}

	BuiltinMetricByName           map[string]*MetricMetaValue
	BuiltinMetricAllowedToReceive map[string]*MetricMetaValue

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
)

func TagIDTagToTagID(tagIDTag int32) string {
	return tagIDTag2TagID[tagIDTag]
}

// 4 very similar metrics to record longer than allowed commandline arguments of statshouse components
func createBuiltinMetricIDHeartbeatArgs(name string, description string) *MetricMetaValue {
	return &MetricMetaValue{
		Name:                 name,
		Kind:                 MetricKindValue,
		Description:          description,
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
			Description: "-",
		}, {
			Description: "-",
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
		}},
	}
}

func init() {
	for i := 0; i < MaxTags; i++ {
		legacyName := tagIDPrefix + strconv.Itoa(i)
		tagIDs = append(tagIDs, legacyName)
		newTagIDs = append(newTagIDs, strconv.Itoa(i))
		tagIDToIndexForAPI[legacyName] = i
		tagIDTag2TagID[int32(i+TagIDShiftLegacy)] = legacyName
		tagIDTag2TagID[int32(i+TagIDShift)] = tagStringForUI + " " + strconv.Itoa(i) // for UI only
	}
	tagIDTag2TagID[TagIDShiftLegacy-1] = StringTopTagID
	tagIDTag2TagID[TagIDShift-1] = tagStringForUI + " " + NewStringTopTagID // for UI only
	tagIDTag2TagID[TagIDShift-2] = tagStringForUI + " " + NewHostTagID      // for UI only

	BuiltinMetricByName = make(map[string]*MetricMetaValue, len(BuiltinMetrics))
	BuiltinMetricAllowedToReceive = make(map[string]*MetricMetaValue, len(BuiltinMetrics))
	for id, m := range BuiltinMetrics {
		m.MetricID = id
		m.GroupID = BuiltinGroupIDBuiltin
		m.Visible = !builtinMetricsInvisible[id]
		m.PreKeyFrom = math.MaxInt32 // allow writing, but not yet selecting

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
		if metricsWithAgentEnvRouteArch[id] {
			m.Tags[RouteTag] = MetricMetaTag{Description: "route", ValueComments: convertToValueComments(routeToValue)}
			m.Tags[AgentEnvTag] = MetricMetaTag{
				Description: "statshouse_env",
				ValueComments: convertToValueComments(map[int32]string{
					TagValueIDProduction: "statshouse.production",
					TagValueIDStaging:    "statshouse.staging",
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
		_ = m.RestoreCachedInfo()
	}
}

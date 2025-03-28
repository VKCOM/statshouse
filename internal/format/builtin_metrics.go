// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package format

// metric metas for builtin metrics are accessible directly without search in map
// some tags are added empty, they were used in incompatible way in the past.
// We should not reuse such tags, because there would be garbage in historic data.

const BuiltinMetricIDAgentSamplingFactor = -1

var BuiltinMetricMetaAgentSamplingFactor = &MetricMetaValue{
	Name: "__src_sampling_factor",
	Kind: MetricKindValue,
	Description: `Sample factor selected by agent.
Calculated by agent from scratch every second to fit all collected data into network budget.
Count of this metric is proportional to # of clients who set it in particular second.
Set only if greater than 1.`,
	NoSampleAgent:           false, // marshalled by aggregator to the same shard metric is
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false, // because expensive
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "metric",
		BuiltinKind: BuiltinKindMetric,
	}, {}, {}, {}, AggShardTag - 1: { // we reuse index of aggregator_shard here. -1 because we do not include env tag
		Description: "agent_shard",
		RawKind:     "int",
	}},
	PreKeyTagID: "1",
}

const BuiltinMetricIDAggBucketReceiveDelaySec = -2 // Also approximates insert delay, interesting for historic buckets

var BuiltinMetricMetaAggBucketReceiveDelaySec = &MetricMetaValue{
	Name: "__agg_bucket_receive_delay_sec",
	Kind: MetricKindValue,
	Description: `Difference between timestamp of received bucket and aggregator wall clock.
Count of this metric is # of agents who sent this second (per replica*shard), and they do it every second to keep this metric stable.
Set by aggregator.`,
	MetricType:              MetricSecond,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
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
	}, {}},
}

// If all contributors come on time, count will be 1 (per shard). If some come through historic conveyor, can be larger.
var BuiltinMetricMetaAggInsertSizeLegacy = &MetricMetaValue{
	Name:                    "__agg_insert_size_legacy",
	Kind:                    MetricKindValue,
	Description:             "Size of aggregated bucket inserted into clickhouse. Written when second is inserted, which can be much later.",
	MetricType:              MetricByte,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description:   "type",
		ValueComments: convertToValueComments(insertKindToValue),
	}, {}, {}, {
		Description:   "table",
		ValueComments: convertToValueComments(tableFormatToValue),
	}},
}

var BuiltinMetricMetaTLByteSizePerInflightType = &MetricMetaValue{
	Name:                    "__src_tl_byte_size_per_inflight_type",
	Kind:                    MetricKindValue,
	Description:             "Approximate uncompressed byte size of various parts of TL representation of time bucket.\nSet by agent.",
	MetricType:              MetricByte,
	NoSampleAgent:           true, // generated on agents, fixed cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description:   "inflight_type",
		ValueComments: convertToValueComments(insertKindToValue),
	}, {
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}, AggShardTag - 1: { // we reuse index of aggregator_shard here. -1 because we do not include env tag
		Description: "agent_shard",
		RawKind:     "int",
	}},
}

var BuiltinMetricMetaAggKeepAlive = &MetricMetaValue{
	Name:                    "__agg_keep_alive",
	Kind:                    MetricKindCounter,
	Description:             "Number of keep-alive empty inserts (which follow normal insert conveyor) in aggregated bucket.\nSet by aggregator.",
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags:                    []MetricMetaTag{{}, {}, {}, {}},
}

var BuiltinMetricMetaAggSizeCompressed = &MetricMetaValue{
	Name:                    "__agg_size_compressed",
	Kind:                    MetricKindValue,
	Description:             "Compressed size of bucket received from agent (size of raw TL request).\nSet by aggregator.",
	MetricType:              MetricByte,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description:   "aggregator_role",
		ValueComments: convertToValueComments(aggregatorRoleToValue),
	}, {}},
}

var BuiltinMetricMetaAggSizeUncompressed = &MetricMetaValue{
	Name:                    "__agg_size_uncompressed",
	Kind:                    MetricKindValue,
	Description:             "Uncompressed size of bucket received from agent.\nSet by aggregator.",
	MetricType:              MetricByte,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description:   "aggregator_role",
		ValueComments: convertToValueComments(aggregatorRoleToValue),
	}, {}},
}

const BuiltinMetricIDAggHourCardinality = -9

var BuiltinMetricMetaAggHourCardinality = &MetricMetaValue{
	Name: "__agg_hour_cardinality",
	Kind: MetricKindValue,
	Description: `Estimated unique metric-tag combinations per hour.
Linear interpolation between previous hour and value collected so far for this hour.
Steps of interpolation can be visible on graph.
Each aggregator writes value on every insert to particular second, multiplied by # of aggregator shards.
So avg() of this metric shows estimated full cardinality with or without grouping by aggregator.`,
	Resolution:              60,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description: "metric",
		BuiltinKind: BuiltinKindMetric,
	}},
	PreKeyTagID: "4",
}

const BuiltinMetricIDAggSamplingFactor = -10

var BuiltinMetricMetaAggSamplingFactor = &MetricMetaValue{
	Name: "__agg_sampling_factor",
	Kind: MetricKindValue,
	Description: `Sample factor selected by aggregator.
Calculated by aggregator from scratch every second to fit all collected data into clickhouse insert budget.
Set only if greater than 1.`,
	NoSampleAgent:           false, // marshalled by aggregator to the same shard metric is
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description: "metric",
		BuiltinKind: BuiltinKindMetric,
	}, {
		Description: "-", // we do not show sampling reason for now because there is single reason. We write it, though.
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDAggSamplingFactorReasonInsertSize: "insert_size",
		}),
	}},
	PreKeyTagID: "4",
}

const BuiltinMetricIDIngestionStatus = -11

var BuiltinMetricMetaIngestionStatus = &MetricMetaValue{
	Name: "__src_ingestion_status",
	Kind: MetricKindCounter,
	Description: `Status of receiving metrics by agent.
Most errors are due to various data format violation.
Some, like 'err_map_per_metric_queue_overload', 'err_map_tag_value', 'err_map_tag_value_cached' indicate tag mapping subsystem slowdowns or errors.
This metric uses sampling budgets of metric it refers to, so flooding by errors cannot affect other metrics.
'err_*_utf8'' statuses store original string value in hex.`,
	StringTopDescription:    "string_value",
	NoSampleAgent:           false, // marshalled by aggregator to the same shard metric is
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "metric",
		BuiltinKind: BuiltinKindMetric,
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
			TagValueIDSrcIngestionStatusErrMetricDisabled:            "err_metric_disabled",
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
			TagValueIDSrcIngestionStatusErrShardingFailed:            "err_metric_sharding_failed",
			TagValueIDSrcIngestionStatusWarnTimestampClampedPast:     "warn_timestamp_clamped_past",
			TagValueIDSrcIngestionStatusWarnTimestampClampedFuture:   "warn_timestamp_clamped_future",
			TagValueIDSrcIngestionStatusErrMetricBuiltin:             "err_metric_builtin",
			TagValueIDSrcIngestionStatusOKDup:                        "ok_dup",
		}),
	}, {
		Description: "tag_id",
	}},
	PreKeyTagID: "1",
}

var BuiltinMetricMetaAggInsertTime = &MetricMetaValue{
	Name:                    "__agg_insert_time",
	Kind:                    MetricKindValue,
	Description:             "Time inserting this second into clickhouse took. Written when second is inserted, which can be much later.",
	MetricType:              MetricSecond,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDStatusOK:    "ok",
			TagValueIDStatusError: "error",
		}),
	}, {
		Description: "http_status",
		RawKind:     "int",
	}, {
		Description:   "clickhouse_exception",
		ValueComments: convertToValueComments(clickhouseExceptions),
	}, {
		Description:   "table",
		ValueComments: convertToValueComments(tableFormatToValue),
	}},
}

var BuiltinMetricMetaAggHistoricBucketsWaiting = &MetricMetaValue{
	Name: "__agg_historic_buckets_waiting",
	Kind: MetricKindValue,
	Description: `Time difference of historic seconds (several per contributor) waiting to be inserted via historic conveyor.
Count is number of such seconds waiting.`,
	MetricType:              MetricSecond,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "aggregator_role",
		ValueComments: convertToValueComments(aggregatorRoleToValue),
	}, {
		Description:   "route",
		ValueComments: convertToValueComments(routeToValue),
	}},
}

var BuiltinMetricMetaAggBucketAggregateTimeSec = &MetricMetaValue{
	Name: "__agg_bucket_aggregate_time_sec",
	Kind: MetricKindValue,
	Description: `Time between agent bucket is received and fully aggregated into aggregator bucket.
Set by aggregator. Max(value)@host shows agent responsible for longest aggregation.`,
	MetricType:              MetricSecond,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description:   "aggregator_role",
		ValueComments: convertToValueComments(aggregatorRoleToValue),
	}, {}},
}

var BuiltinMetricMetaAggActiveSenders = &MetricMetaValue{
	Name:                    "__agg_active_senders",
	Kind:                    MetricKindValue,
	Description:             "Number of insert lines between aggregator and clickhouse busy with insertion.",
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}},
}

var BuiltinMetricMetaAggOutdatedAgents = &MetricMetaValue{
	Name:                    "__agg_outdated_agents",
	Kind:                    MetricKindCounter,
	Resolution:              60,
	Description:             "Number of outdated agents.",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description: "owner",
	}, {
		Description: "host",
	}, {
		Description: "remote_ip",
		RawKind:     "ip",
	}},
}

var BuiltinMetricMetaAgentDiskCacheErrors = &MetricMetaValue{
	Name:                    "__src_disc_cache_errors",
	Kind:                    MetricKindCounter,
	Description:             "Disk cache errors. Written by agent.",
	NoSampleAgent:           true, // limited cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
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
}

var BuiltinMetricMetaTimingErrors = &MetricMetaValue{
	Name: "__timing_errors",
	Kind: MetricKindValue,
	Description: `Timing errors - sending data too early or too late.
Set by either agent or aggregator, depending on status.`,
	NoSampleAgent:           true,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
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
	}, {}, {}, {}},
}

var BuiltinMetricMetaAgentReceivedBatchSize = &MetricMetaValue{
	Name:                    "__src_ingested_metric_batch_size",
	Kind:                    MetricKindValue,
	Description:             "Size in bytes of metric batches received by agent.\nCount is # of such batches.",
	MetricType:              MetricByte,
	NoSampleAgent:           true,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
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
}

var BuiltinMetricMetaAggMapping = &MetricMetaValue{
	Name:                    "__agg_mapping_status",
	Kind:                    MetricKindCounter,
	Description:             "Status of mapping on aggregator side.",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
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
}

var BuiltinMetricMetaAggInsertTimeReal = &MetricMetaValue{
	Name:                    "__agg_insert_time_real",
	Kind:                    MetricKindValue,
	Description:             "Time of aggregated bucket inserting into clickhouse took in this second.\nactual seconds inserted can be from the past.",
	MetricType:              MetricSecond,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDStatusOK:    "ok",
			TagValueIDStatusError: "error",
		}),
	}, {
		Description: "http_status",
		RawKind:     "int",
	}, {
		Description:   "clickhouse_exception",
		ValueComments: convertToValueComments(clickhouseExceptions),
	}, {
		Description:   "table",
		ValueComments: convertToValueComments(tableFormatToValue),
	}},
}

var BuiltinMetricMetaAgentHistoricQueueSize = &MetricMetaValue{
	Name: "__src_historic_queue_size_bytes",
	Kind: MetricKindValue,
	Description: `Historic queue size in memory and on disk.
Disk size increases when second is written, decreases when file is deleted.
Storage 'empty' event is recorded with 0 value when there is no unsent data, so we can filter out agents with/without empty queue.`,
	MetricType:              MetricByte,
	NoSampleAgent:           true, // limited cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "storage",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDHistoricQueueMemory:     "memory",
			TagValueIDHistoricQueueDiskUnsent: "disk_unsent",
			TagValueIDHistoricQueueDiskSent:   "disk_sent",
			TagValueIDHistoricQueueEmpty:      "empty",
		}),
	}, {}, {}, {}, {}, {
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}, AggShardTag - 1: { // we reuse index of aggregator_shard here. -1 because we do not include env tag
		Description: "agent_shard",
		RawKind:     "int",
	}},
}

var BuiltinMetricMetaAggHistoricSecondsWaiting = &MetricMetaValue{
	Name:                    "__agg_historic_seconds_waiting",
	Kind:                    MetricKindValue,
	Description:             "Time difference of aggregated historic seconds waiting to be inserted via historic conveyor. Count is number of unique seconds waiting.",
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "aggregator_role",
		ValueComments: convertToValueComments(aggregatorRoleToValue),
	}, {
		Description:   "route",
		ValueComments: convertToValueComments(routeToValue),
	}},
}

var BuiltinMetricMetaAggInsertSizeReal = &MetricMetaValue{
	Name:                    "__agg_insert_size_real",
	Kind:                    MetricKindValue,
	Description:             "Size of aggregated bucket inserted into clickhouse in this second (actual seconds inserted can be from the past).",
	MetricType:              MetricByte,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDStatusOK:    "ok",
			TagValueIDStatusError: "error",
		}),
	}, {
		Description: "http_status",
		RawKind:     "int",
	}, {
		Description:   "clickhouse_exception",
		ValueComments: convertToValueComments(clickhouseExceptions),
	}, {
		Description:   "table",
		ValueComments: convertToValueComments(tableFormatToValue),
	}},
}

var BuiltinMetricMetaAgentMapping = &MetricMetaValue{
	Name:                    "__src_mapping_time",
	Kind:                    MetricKindValue,
	Description:             "Time and status of mapping request.\nWritten by agent.",
	MetricType:              MetricSecond,
	NoSampleAgent:           true, // limited cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
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
}

var BuiltinMetricMetaAgentReceivedPacketSize = &MetricMetaValue{
	Name:                    "__src_ingested_packet_size",
	Kind:                    MetricKindValue,
	Description:             "Size in bytes of packets received by agent. Also count is # of received packets.",
	MetricType:              MetricByte,
	NoSampleAgent:           true, // limited cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
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
}

const BuiltinMetricIDAggMappingCreated = -33

var BuiltinMetricMetaAggMappingCreated = &MetricMetaValue{
	Name: "__agg_mapping_created",
	Kind: MetricKindValue,
	Description: `Status of mapping string tags to integer values.
Value is actual integer value created (by incrementing global counter).
Set by aggregator.`,
	StringTopDescription:    "Tag Values",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description: "metric",
		BuiltinKind: BuiltinKindMetric,
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
	}, {
		Description: "conveyor",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDAggMappingCreatedConveyorOld: "old",
			TagValueIDAggMappingCreatedConveyorNew: "new",
		}),
	}, {
		Description: "unknown_metric_id",
		RawKind:     "int",
	}, {
		Description: "tag_value",
	}},
	PreKeyTagID: "4",
}

var BuiltinMetricMetaVersions = &MetricMetaValue{
	Name:                    "__build_version",
	Kind:                    MetricKindCounter,
	Description:             "Build Version (commit) of statshouse components.",
	StringTopDescription:    "Build Commit",
	Resolution:              60,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}, {}, {
		Description: "commit_timestamp",
		RawKind:     "timestamp",
	}, {
		Description: "commit_hash",
		RawKind:     "hex",
	}, {}},
}

const BuiltinMetricIDBadges = -35 // copy of some other metrics for efficient show of errors and warnings
var BuiltinMetricMetaBadges = &MetricMetaValue{
	Name:                    "__badges",
	Kind:                    MetricKindValue,
	Description:             "System metric used to display UI badges above plot. Stores stripped copy of some other builtin metrics.",
	Resolution:              5,
	NoSampleAgent:           false, // marshalled by aggregator to the same shard metric is
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false, // this metric must be extremely lightweight
	WithAggregatorID:        false, // this metric must be extremely lightweight
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
		BuiltinKind: BuiltinKindMetric,
	}},
	PreKeyTagID: "2",
}

var BuiltinMetricMetaAutoConfig = &MetricMetaValue{
	Name: "__autoconfig",
	Kind: MetricKindCounter,
	Description: `Status of agent getConfig RPC message, used to configure sharding on agents.
Set by aggregator, max host shows actual host of agent who connected.
Ingress proxies first proxy request (to record host and IP of agent), then replace response with their own addresses.'`,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDAutoConfigOK:             "ok_config",
			TagValueIDAutoConfigErrorSend:      "err_send",
			TagValueIDAutoConfigErrorKeepAlive: "err_keep_alive",
			TagValueIDAutoConfigWrongCluster:   "err_config_cluster",
		}),
	}, {
		Description: "shard_replica",
		RawKind:     "int",
	}, {
		Description: "total_shard_replicas",
		RawKind:     "int",
	}},
}

var BuiltinMetricMetaJournalVersions = &MetricMetaValue{
	Name:                    "__metric_journal_version",
	Kind:                    MetricKindCounter,
	Description:             "Metadata journal version plus stable hash of journal state.",
	StringTopDescription:    "Journal Hash",
	Resolution:              60,
	NoSampleAgent:           true, // has limited cardinality, but only on each agent
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}, {}, {}, {}, {
		Description: "version",
		RawKind:     "int",
	}, {
		Description: "journal_hash",
		RawKind:     "hex",
	}, {
		Description: "journal_kind",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDMetaJournalVersionsKindLegacySHA1:  "legacy_sha1",
			TagValueIDMetaJournalVersionsKindLegacyXXH3:  "legacy_xxh3",
			TagValueIDMetaJournalVersionsKindNormalXXH3:  "normal_xxh3",
			TagValueIDMetaJournalVersionsKindCompactXXH3: "compact_xxh3",
		}),
	}},
}

var BuiltinMetricMetaPromScrapeTime = &MetricMetaValue{
	Name:                    "__prom_scrape_time",
	Kind:                    MetricKindValue,
	Description:             "Time of scraping prom metrics",
	MetricType:              MetricSecond,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{
		{}, {
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
}

const BuiltinMetricIDAgentHeartbeatVersion = -41 // TODO - remove
const BuiltinMetricIDAgentHeartbeatArgs = -42    // TODO - remove, this metric was writing larger than allowed strings to DB in the past

var BuiltinMetricMetaUsageMemory = &MetricMetaValue{
	Name:                    "__usage_mem",
	Kind:                    MetricKindValue,
	Description:             "Memory usage of statshouse components.",
	MetricType:              MetricByte,
	Resolution:              60,
	NoSampleAgent:           true, // limited cardinality
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}},
}

var BuiltinMetricMetaUsageCPU = &MetricMetaValue{
	Name:                    "__usage_cpu",
	Kind:                    MetricKindValue,
	Description:             "CPU usage of statshouse components, CPU seconds per second.",
	MetricType:              MetricSecond,
	Resolution:              60,
	NoSampleAgent:           true, // limited cardinality
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}, {
		Description: "sys/user",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDCPUUsageSys:  "sys",
			TagValueIDCPUUsageUser: "user"}),
	}},
}

const BuiltinMetricIDHeartbeatVersion = -47

var BuiltinMetricMetaHeartbeatVersion = &MetricMetaValue{
	Name:                    "__heartbeat_version",
	Kind:                    MetricKindValue,
	Description:             "Heartbeat value is uptime",
	MetricType:              MetricSecond,
	StringTopDescription:    "Build Commit",
	Resolution:              60,
	NoSampleAgent:           true, // limited cardinality, but only on each agent
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}, {
		Description: "event_type",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDHeartbeatEventStart:     "start",
			TagValueIDHeartbeatEventHeartbeat: "heartbeat"}),
	}, {}, {
		Description: "commit_hash",
		RawKind:     "hex",
	}, { // former commit_date, obsolete by nice UI formatting of the next tag
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
}

const BuiltinMetricIDHeartbeatArgs = -48 // this metric was writing larger than allowed strings to DB in the past
var BuiltinMetricMetaHeartbeatArgs = &MetricMetaValue{
	Name:                    "__heartbeat_args",
	Kind:                    MetricKindValue,
	Description:             "Commandline of statshouse components.\nHeartbeat value is uptime.",
	MetricType:              MetricSecond,
	StringTopDescription:    "Arguments",
	Resolution:              60,
	NoSampleAgent:           true, // limited cardinality, but only on each agent
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
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
	}, {}, {
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
}

var BuiltinMetricMetaAPIBRS = &MetricMetaValue{ // TODO - harmonize
	Name:                    "__api_big_response_storage_size",
	Kind:                    MetricKindValue,
	Description:             "Size of storage inside API of big response chunks",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaBudgetHost = &MetricMetaValue{
	Name:                    "__budget_host",
	Kind:                    MetricKindCounter,
	Disable:                 true, // disabled, but host mapping is flood-protected
	Description:             "Invisible metric used only for accounting budget to create host mappings",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags:                    []MetricMetaTag{},
}

// we want to see limits properly credited in flood meta metric tags
var BuiltinMetricMetaBudgetAggregatorHost = &MetricMetaValue{
	Name:                    "__budget_aggregator_host",
	Kind:                    MetricKindCounter,
	Disable:                 true, // disabled, but aggregator host mapping is flood-protected
	Description:             "Invisible metric used only for accounting budget to create host mappings of aggregators themselves",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags:                    []MetricMetaTag{},
}

var BuiltinMetricMetaAPIActiveQueries = &MetricMetaValue{
	Name:                    "__api_active_queries",
	Kind:                    MetricKindValue,
	Description:             "Active queries to clickhouse by API.\nRequests are assigned to lanes by estimated processing time.",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{ // if we need another component
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
}

const BuiltinMetricIDRPCRequests = -56

var BuiltinMetricMetaRPCRequests = &MetricMetaValue{
	Name:                    "__rpc_request_size",
	Kind:                    MetricKindValue,
	Description:             "Size of RPC request bodies.\nFor ingress proxy, key_id can be used to identify senders.",
	MetricType:              MetricByte,
	StringTopDescription:    "error_text",
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}, {
		Description: "tag",
		RawKind:     "hex",
		ValueComments: convertToValueComments(map[int32]string{
			0x193f1b22: "rpcCancelReq", // recorded by ingress proxy, so we added it here
			0x28bea524: "statshouse.autoCreate",
			0x4285ff57: "statshouse.getConfig2",
			0x7d7b4991: "statshouse.getConfig3",
			0x42855554: "statshouse.getMetrics3",
			0x4285ff56: "statshouse.getTagMapping2",
			0x75a7f68e: "statshouse.getTagMappingBootstrap",
			0x41df72a3: "statshouse.getTargets2",
			0x4285ff53: "statshouse.sendKeepAlive2",
			0x44575940: "statshouse.sendSourceBucket2",
			0x0d04aa3f: "statshouse.sendSourceBucket3",
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
	}, { // in the future - error code
	}, { // in the future - something
	}, {
		Description: "key_id", // this is unrelated to metric keys, this is ingress key ID
		RawKind:     "hex",
	}, {
		Description: "host", // filled by aggregator for ingress proxy
	}, {
		Description: "protocol",
		RawKind:     "int",
	}},
}

const BuiltinMetricIDBudgetUnknownMetric = -57

var BuiltinMetricMetaBudgetUnknownMetric = &MetricMetaValue{
	Name:                    "__budget_unknown_metric",
	Kind:                    MetricKindCounter,
	Disable:                 true, // disabled, but mapping for unknown metrics is flood-protected
	Description:             "Invisible metric used only for accounting budget to create mappings with metric not found",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags:                    []MetricMetaTag{},
}

const BuiltinMetricIDContributorsLog = -61

var BuiltinMetricMetaContributorsLog = &MetricMetaValue{
	Name: "__contributors_log",
	Kind: MetricKindValue,
	Description: `Used to invalidate API caches.
Timestamps of all inserted seconds per second are recorded here in key1.
Value is delta between second value and time it was inserted.
To see which seconds change when, use __contributors_log_rev`,
	NoSampleAgent:           false, // marshalled by aggregator to the same shard metric is
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description: "timestamp",
		RawKind:     "timestamp",
	}},
}

const BuiltinMetricIDContributorsLogRev = -62

var BuiltinMetricMetaContributorsLogRev = &MetricMetaValue{
	Name: "__contributors_log_rev",
	Kind: MetricKindValue,
	Description: `Reverse index of __contributors_log, used to invalidate API caches.
key1 is UNIX timestamp of second when this second was changed.
Value is delta between second value and time it was inserted.`,
	NoSampleAgent:           false, // marshalled by aggregator to the same shard metric is
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description: "insert_timestamp",
		RawKind:     "timestamp",
	}},
}

var BuiltinMetricMetaGroupSizeBeforeSampling = &MetricMetaValue{
	Name:                    "__group_size_before_sampling",
	Kind:                    MetricKindValue,
	Description:             "Group size before sampling, bytes.",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}, {
		Description: "group",
		RawKind:     "int",
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDGroupSizeSamplingFit:     "fit",
			TagValueIDGroupSizeSamplingSampled: "sampled",
		}),
	}},
}

var BuiltinMetricMetaGroupSizeAfterSampling = &MetricMetaValue{
	Name:                    "__group_size_after_sampling",
	Kind:                    MetricKindValue,
	Description:             "Group size after sampling, bytes.",
	MetricType:              MetricByte,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}, {
		Description: "group",
		RawKind:     "int",
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDGroupSizeSamplingFit:     "fit",
			TagValueIDGroupSizeSamplingSampled: "sampled",
		}),
	}},
}

var BuiltinMetricMetaAPISelectBytes = &MetricMetaValue{
	Name: "__api_ch_select_bytes",
	Kind: MetricKindValue,
	// TODO replace with logs
	StringTopDescription:    "error",
	Description:             "Number of bytes was handled by ClickHouse SELECT query",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "query type",
	}},
}

var BuiltinMetricMetaAPISelectRows = &MetricMetaValue{
	Name: "__api_ch_select_rows",
	Kind: MetricKindValue,
	// TODO replace with logs
	StringTopDescription:    "error",
	Description:             "Number of rows was handled by ClickHouse SELECT query",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "query type",
	}},
}

var BuiltinMetricMetaAPISelectDuration = &MetricMetaValue{
	Name:                    "__api_ch_select_duration",
	Kind:                    MetricKindValue,
	MetricType:              MetricSecond,
	Description:             "Duration of clickhouse query",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "query type",
	}, {
		Description: "metric",
		BuiltinKind: BuiltinKindMetric,
	}, {
		Description: "table",
	}, {
		Description: "kind",
	}, {
		Description: "status",
	}, {
		Description: "token-short",
	}, {
		Description: "token-long",
	}, {
		Description: "shard", // metric % 16 for now, experimental
		RawKind:     "int",
	}},
}

// const BuiltinMetricIDAgentHistoricQueueSizeSum = -69 // Removed, not needed after queue size metric is written by agents
var BuiltinMetricMetaAPISourceSelectRows = &MetricMetaValue{
	Name:                    "__api_ch_source_select_rows",
	Kind:                    MetricKindValue,
	Description:             "Value of this metric number of rows was selected from DB or cache",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description: "source type",
	}, {
		Description: "metric",
		BuiltinKind: BuiltinKindMetric,
	}, {
		Description: "table",
	}, {
		Description: "kind",
	}},
}

var BuiltinMetricMetaSystemMetricScrapeDuration = &MetricMetaValue{
	Name:                    "__system_metrics_duration",
	Kind:                    MetricKindValue,
	Description:             "System metrics scrape duration in seconds",
	MetricType:              MetricSecond,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
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
}

var BuiltinMetricMetaMetaServiceTime = &MetricMetaValue{ // TODO - harmonize
	Name:                    "__meta_rpc_service_time",
	Kind:                    MetricKindValue,
	Description:             "Time to handle RPC query by meta.",
	MetricType:              MetricSecond,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description: "host",
	}, {
		Description: "method",
	}, {
		Description: "query_type",
	}, {
		Description: "status",
	}},
}

var BuiltinMetricMetaMetaClientWaits = &MetricMetaValue{ // TODO - harmonize
	Name:                    "__meta_load_journal_client_waits",
	Kind:                    MetricKindValue,
	Description:             "Number of clients waiting journal updates",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaAgentUDPReceiveBufferSize = &MetricMetaValue{
	Name:                    "__src_udp_receive_buffer_size",
	Kind:                    MetricKindValue,
	Resolution:              60,
	Description:             "Size in bytes of agent UDP receive buffer.",
	MetricType:              MetricByte,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
}

var BuiltinMetricMetaAPIMetricUsage = &MetricMetaValue{
	Name:                    "__api_metric_usage",
	Resolution:              60,
	Kind:                    MetricKindCounter,
	Description:             "Metric usage",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "type",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDRPC:  "RPC",
			TagValueIDHTTP: "http",
		}),
	}, {
		Description: "user",
	}, {
		Description: "metric",
		BuiltinKind: BuiltinKindMetric,
	}},
}

var BuiltinMetricMetaAPIServiceTime = &MetricMetaValue{
	Name:                    "__api_service_time",
	Kind:                    MetricKindValue,
	Description:             "Time to handle API query.",
	MetricType:              MetricSecond,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
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
		RawKind:     "int",
	}, {
		Description: "metric",
		BuiltinKind: BuiltinKindMetric,
	}, {
		Description: "priority",
		RawKind:     "int",
	}},
}

var BuiltinMetricMetaAPIResponseTime = &MetricMetaValue{
	Name:                    "__api_response_time",
	Kind:                    MetricKindValue,
	Description:             "Time to handle and respond to query by API",
	MetricType:              MetricSecond,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
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
		RawKind:     "int",
	}, {
		Description: "metric",
		BuiltinKind: BuiltinKindMetric,
	}, {
		Description: "priority",
		RawKind:     "int",
	}},
}

var BuiltinMetricMetaSrcTestConnection = &MetricMetaValue{
	Name:                    "__src_test_connection",
	Kind:                    MetricKindValue,
	Resolution:              60,
	Description:             "Duration of call test connection rpc method",
	MetricType:              MetricSecond,
	NoSampleAgent:           true, // written only 1 per minute from each agent
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagOKConnection: "ok",
			TagOtherError:   "other",
			TagRPCError:     "rpc_error",
			TagNoConnection: "no_connection",
			TagTimeoutError: "timeout",
		}),
	}, {
		Description: "rpc_code",
		RawKind:     "int",
	}, AggShardTag - 1: { //  // we reuse index of aggregator_shard here. -1 because we do not include env tag
		Description: "agent_shard",
		RawKind:     "int",
	}, AggReplicaTag - 1: { //  // we reuse index of aggregator_replica here. -1 because we do not include env tag
		Description: "agent_replica",
		RawKind:     "int",
	}},
}

var BuiltinMetricMetaAgentAggregatorTimeDiff = &MetricMetaValue{
	Name:                    "__src_agg_time_diff",
	Kind:                    MetricKindValue,
	Resolution:              60,
	Description:             "Aggregator time - agent time when start testConnection",
	MetricType:              MetricSecond,
	NoSampleAgent:           true, // written only 1 per minute from each agent
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}, {}, AggShardTag - 1: { //  // we reuse index of aggregator_shard here. -1 because we do not include env tag
		Description: "agent_shard",
		RawKind:     "int",
	}, AggReplicaTag - 1: { //  // we reuse index of aggregator_replica here. -1 because we do not include env tag
		Description: "agent_replica",
		RawKind:     "int",
	}},
}

var BuiltinMetricMetaSrcSamplingMetricCount = &MetricMetaValue{
	Name:                    "__src_sampling_metric_count",
	Kind:                    MetricKindValue,
	Description:             `Metric count processed by sampler on agent.`,
	NoSampleAgent:           true, // limited cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Name:          "component",
		ValueComments: convertToValueComments(componentToValue),
	}},
}

var BuiltinMetricMetaAggSamplingMetricCount = &MetricMetaValue{
	Name:                    "__agg_sampling_metric_count",
	Kind:                    MetricKindValue,
	Description:             `Metric count processed by sampler on aggregator.`,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Name:          "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDStatusOK:    "ok",
			TagValueIDStatusError: "error",
		}),
	}, {
		Description:   "table",
		ValueComments: convertToValueComments(tableFormatToValue),
	}},
}

var BuiltinMetricMetaSrcSamplingSizeBytes = &MetricMetaValue{
	Name:                    "__src_sampling_size_bytes",
	Kind:                    MetricKindValue,
	MetricType:              MetricByte,
	Description:             `Size in bytes processed by sampler on agent.`,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
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
		BuiltinKind: BuiltinKindNamespace,
		ValueComments: convertToValueComments(map[int32]string{
			BuiltinNamespaceIDDefault: "default",
			BuiltinNamespaceIDMissing: "missing",
		}),
	}, {
		Name:        "group",
		BuiltinKind: BuiltinKindGroup,
		ValueComments: convertToValueComments(map[int32]string{
			BuiltinGroupIDDefault: "default",
			BuiltinGroupIDBuiltin: "builtin",
			BuiltinGroupIDHost:    "host",
			BuiltinGroupIDMissing: "missing",
		}),
	}},
}

var BuiltinMetricMetaAggSamplingSizeBytes = &MetricMetaValue{
	Name:                    "__agg_sampling_size_bytes",
	Kind:                    MetricKindValue,
	MetricType:              MetricByte,
	Description:             `Size in bytes processed by sampler on aggregator.`,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
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
		BuiltinKind: BuiltinKindNamespace,
		ValueComments: convertToValueComments(map[int32]string{
			BuiltinNamespaceIDDefault: "default",
			BuiltinNamespaceIDMissing: "missing",
		}),
	}, {
		Name:        "group",
		BuiltinKind: BuiltinKindGroup,
		ValueComments: convertToValueComments(map[int32]string{
			BuiltinGroupIDDefault: "default",
			BuiltinGroupIDBuiltin: "builtin",
			BuiltinGroupIDHost:    "host",
			BuiltinGroupIDMissing: "missing",
		}),
	}, {
		// former metric_id
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDStatusOK:    "ok",
			TagValueIDStatusError: "error",
		}),
	}, {
		Description:   "table",
		ValueComments: convertToValueComments(tableFormatToValue),
	}},
}

var BuiltinMetricMetaUIErrors = &MetricMetaValue{
	Name:                    "__ui_errors",
	Kind:                    MetricKindValue,
	Description:             `Errors on the frontend.`,
	StringTopDescription:    "error_string",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags:                    []MetricMetaTag{{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}},
}

var BuiltinMetricMetaStatsHouseErrors = &MetricMetaValue{
	Name:                    "__statshouse_errors",
	Kind:                    MetricKindCounter,
	Description:             `Always empty metric because SH don't have errors'`,
	StringTopDescription:    "error_string",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "error_type",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDDMESGParseError: "dmesg_parse",
			TagValueIDAPIPanicError:   "api_panic",
		}),
	}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}},
}

var BuiltinMetricMetaSrcSamplingBudget = &MetricMetaValue{
	Name:                    "__src_sampling_budget",
	Kind:                    MetricKindValue,
	MetricType:              MetricByte,
	Description:             `Budget allocated on agent.`,
	NoSampleAgent:           true, // limited cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Name:          "component",
		ValueComments: convertToValueComments(componentToValue),
	}},
}

var BuiltinMetricMetaAggSamplingBudget = &MetricMetaValue{
	Name:                    "__agg_sampling_budget",
	Kind:                    MetricKindValue,
	MetricType:              MetricByte,
	Description:             `Budget allocated on aggregator.`,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Name:          "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDStatusOK:    "ok",
			TagValueIDStatusError: "error",
		}),
	}, {
		Description:   "table",
		ValueComments: convertToValueComments(tableFormatToValue),
	}},
}

var BuiltinMetricMetaSrcSamplingGroupBudget = &MetricMetaValue{
	Name:                    "__src_sampling_group_budget",
	Kind:                    MetricKindValue,
	MetricType:              MetricByte,
	Description:             `Group budget allocated on agent.`,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Name:          "component",
		ValueComments: convertToValueComments(componentToValue),
	}, {
		Name:        "namespace",
		BuiltinKind: BuiltinKindNamespace,
		ValueComments: convertToValueComments(map[int32]string{
			BuiltinNamespaceIDDefault: "default",
			BuiltinNamespaceIDMissing: "missing",
		}),
	}, {
		Name:        "group",
		BuiltinKind: BuiltinKindGroup,
		ValueComments: convertToValueComments(map[int32]string{
			BuiltinGroupIDDefault: "default",
			BuiltinGroupIDBuiltin: "builtin",
			BuiltinGroupIDHost:    "host",
			BuiltinGroupIDMissing: "missing",
		}),
	}},
}

var BuiltinMetricMetaAggSamplingGroupBudget = &MetricMetaValue{
	Name:                    "__agg_sampling_group_budget",
	Kind:                    MetricKindValue,
	MetricType:              MetricByte,
	Description:             `Group budget allocated on aggregator.`,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Name:          "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Name:        "namespace",
		BuiltinKind: BuiltinKindNamespace,
		ValueComments: convertToValueComments(map[int32]string{
			BuiltinNamespaceIDDefault: "default",
			BuiltinNamespaceIDMissing: "missing",
		}),
	}, {
		Name:        "group",
		BuiltinKind: BuiltinKindGroup,
		ValueComments: convertToValueComments(map[int32]string{
			BuiltinGroupIDDefault: "default",
			BuiltinGroupIDBuiltin: "builtin",
			BuiltinGroupIDHost:    "host",
			BuiltinGroupIDMissing: "missing",
		}),
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDStatusOK:    "ok",
			TagValueIDStatusError: "error",
		}),
	}, {
		Description:   "table",
		ValueComments: convertToValueComments(tableFormatToValue),
	}},
}

var BuiltinMetricMetaPromQLEngineTime = &MetricMetaValue{
	Name:                    "__promql_engine_time",
	Kind:                    MetricKindValue,
	Description:             "Time spent in PromQL engine",
	MetricType:              MetricSecond,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
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
}

var BuiltinMetricMetaAPICacheHit = &MetricMetaValue{
	Name:                    "__api_cache_hit_rate",
	Kind:                    MetricKindValue,
	Description:             `API cache hit rate`,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description: "source",
	}, {
		Description: "metric",
		BuiltinKind: BuiltinKindMetric,
	}, {
		Description: "table",
	}, {
		Description: "kind",
	}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}},
}

var BuiltinMetricMetaAggScrapeTargetDispatch = &MetricMetaValue{
	Name:                    "__agg_scrape_target_dispatch",
	Kind:                    MetricKindCounter,
	Description:             "Scrape target-to-agent assigment events",
	StringTopDescription:    "agent_host",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description:   "status",
		ValueComments: convertToValueComments(successOrFailure),
	}, {
		Description: "event_type",
		ValueComments: convertToValueComments(map[int32]string{
			1: "targets_ready",
			2: "targets_sent",
		}),
	},
	},
}

var BuiltinMetricMetaAggScrapeTargetDiscovery = &MetricMetaValue{
	Name:                    "__agg_scrape_target_discovery",
	Kind:                    MetricKindCounter,
	Description:             "Scrape targets found by service discovery",
	StringTopDescription:    "scrape_target",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
}

var BuiltinMetricMetaAggScrapeConfigHash = &MetricMetaValue{
	Name:                    "__agg_scrape_config_hash",
	Kind:                    MetricKindCounter,
	Description:             "Scrape configuration string SHA1 hash",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description: "config_hash",
		RawKind:     "hex",
	}},
}

var BuiltinMetricMetaAggSamplingTime = &MetricMetaValue{
	Name:                    "__agg_sampling_time",
	Kind:                    MetricKindValue,
	MetricType:              MetricSecond,
	Description:             "Time sampling this second took. Written when second is inserted, which can be much later.",
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDStatusOK:    "ok",
			TagValueIDStatusError: "error",
		}),
	}, {
		Description: "http_status",
		RawKind:     "int",
	}, {
		Description:   "clickhouse_exception",
		ValueComments: convertToValueComments(clickhouseExceptions),
	}, {
		Description:   "table",
		ValueComments: convertToValueComments(tableFormatToValue),
	}},
}

var BuiltinMetricMetaAgentDiskCacheSize = &MetricMetaValue{
	Name:                    "__src_disk_cache_size",
	Kind:                    MetricKindValue,
	MetricType:              MetricByte,
	Description:             "Size of agent mapping cache",
	NoSampleAgent:           true, // limited cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}},
}

var BuiltinMetricMetaAggContributors = &MetricMetaValue{
	Name:                    "__agg_contributors",
	Kind:                    MetricKindValue,
	Description:             "Number of contributors used to calculate sampling budget.",
	NoSampleAgent:           true, // limited cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDStatusOK:    "ok",
			TagValueIDStatusError: "error",
		}),
	}, {
		Description:   "table",
		ValueComments: convertToValueComments(tableFormatToValue),
	}},
}

var BuiltinMetricMetaAPICacheChunkCount = &MetricMetaValue{
	Name:                    "__api_cache_chunk_count",
	Kind:                    MetricKindCounter,
	Description:             "API cache chunk count.",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaAPICacheSize = &MetricMetaValue{
	Name:                    "__api_cache_size",
	Kind:                    MetricKindValue,
	Description:             "API cache size in bytes.",
	MetricType:              MetricByte,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaAPICacheTrim = &MetricMetaValue{
	Name:                    "__api_cache_trim",
	Kind:                    MetricKindValue,
	Description:             "API cache trim events.",
	MetricType:              MetricByte,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}, {
		Description: "event",
		ValueComments: map[string]string{
			" 1": "start",
			" 2": "end",
		},
	}, {
		Description: "reason",
		ValueComments: map[string]string{
			" 1": "age",
			" 2": "size",
		},
	}},
}

var BuiltinMetricMetaAPICacheAge = &MetricMetaValue{
	Name:                    "__api_cache_age",
	Kind:                    MetricKindValue,
	Description:             "API cache age.",
	MetricType:              MetricSecond,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaAPIBufferBytesAlloc = &MetricMetaValue{
	Name:                    "__api_buffer_bytes_alloc",
	Kind:                    MetricKindValue,
	Description:             "API buffer allocation in bytes.",
	MetricType:              MetricByte,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}, {
		Description:   "kind",
		ValueComments: convertToValueComments(apiBufferKind),
	}},
}

var BuiltinMetricMetaAPIBufferBytesFree = &MetricMetaValue{
	Name:                    "__api_buffer_bytes_free",
	Kind:                    MetricKindValue,
	Description:             "API buffer deallocation in bytes.",
	MetricType:              MetricByte,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}, {
		Description:   "kind",
		ValueComments: convertToValueComments(apiBufferKind),
	}},
}

var BuiltinMetricMetaAPIBufferBytesTotal = &MetricMetaValue{
	Name:                    "__api_buffer_bytes_total",
	Kind:                    MetricKindValue,
	Description:             "API buffer pool size in bytes.",
	MetricType:              MetricByte,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaAutoCreateMetric = &MetricMetaValue{
	Name:                    "__agg_autocreate_metric",
	Kind:                    MetricKindCounter,
	Description:             "Event of automatically created metrics.",
	MetricType:              MetricByte,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
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
}

var BuiltinMetricMetaRestartTimings = &MetricMetaValue{
	Name:                    "__src_restart_timings",
	Kind:                    MetricKindValue,
	MetricType:              MetricSecond,
	Description:             "Time of various restart phases (inactive is time between process stop and start)",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
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
			TagValueIDRestartTimingsPhaseSaveMappings:      "save_mappings",
			TagValueIDRestartTimingsPhaseSaveJournal:       "save_journal",
		}),
	}, {}, {}},
}

var BuiltinMetricMetaGCDuration = &MetricMetaValue{
	Name:                    "__gc_duration",
	Kind:                    MetricKindValue,
	MetricType:              MetricSecond,
	Description:             "Count - number of GC, Value - time spent to gc",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{ // reserved for host
	}, {
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}},
}

var BuiltinMetricMetaAggHistoricHostsWaiting = &MetricMetaValue{
	Name:                    "__agg_historic_hosts_waiting",
	Kind:                    MetricKindValue,
	Description:             "Approximate number of different hosts waiting with historic data.",
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "aggregator_role",
		ValueComments: convertToValueComments(aggregatorRoleToValue),
	}, {
		Description:   "route",
		ValueComments: convertToValueComments(routeToValue),
	}},
}

var BuiltinMetricMetaAggSamplingEngineTime = &MetricMetaValue{
	Name:                    "__agg_sampling_engine_time",
	Kind:                    MetricKindValue,
	MetricType:              MetricSecond,
	Description:             "Time spent in sampling engine",
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description: "phase",
		ValueComments: map[string]string{
			" 1": "append",
			" 2": "partition",
			" 3": "budgeting",
			" 4": "sampling",
			" 5": "meta",
		},
	}, {}, {}, {
		Description:   "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDStatusOK:    "ok",
			TagValueIDStatusError: "error",
		}),
	}, {
		Description:   "table",
		ValueComments: convertToValueComments(tableFormatToValue),
	}},
}

var BuiltinMetricMetaAggSamplingEngineKeys = &MetricMetaValue{
	Name:                    "__agg_sampling_engine_keys",
	Kind:                    MetricKindCounter,
	Description:             "Number of series went through sampling engine",
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDStatusOK:    "ok",
			TagValueIDStatusError: "error",
		}),
	}, {
		Description:   "table",
		ValueComments: convertToValueComments(tableFormatToValue),
	}},
}

var BuiltinMetricMetaProxyAcceptHandshakeError = &MetricMetaValue{
	Name:                    "__igp_accept_handshake_error",
	Kind:                    MetricKindCounter,
	Description:             "Proxy refused to accept incoming connection because of failed  handshake.",
	StringTopDescription:    "remote_ip",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}, {
		Description: "magic_head",
	}, {
		Description: "error",
	}},
}

var BuiltinMetricMetaProxyVmSize = &MetricMetaValue{
	Name:                    "__igp_vm_size",
	Kind:                    MetricKindValue,
	Description:             "StatsHouse proxy virtual memory size.",
	Resolution:              60,
	NoSampleAgent:           true, // low cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaProxyVmRSS = &MetricMetaValue{
	Name:                    "__igp_vm_rss",
	Kind:                    MetricKindValue,
	Description:             "StatsHouse proxy resident set size.",
	Resolution:              60,
	NoSampleAgent:           true, // low cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaProxyHeapAlloc = &MetricMetaValue{
	Name:                    "__igp_heap_alloc",
	Kind:                    MetricKindValue,
	Description:             "StatsHouse proxy bytes of allocated heap objects.",
	Resolution:              60,
	NoSampleAgent:           true, // low cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaProxyHeapSys = &MetricMetaValue{
	Name:                    "__igp_heap_sys",
	Kind:                    MetricKindValue,
	Description:             "StatsHouse proxy bytes of heap memory obtained from the OS.",
	Resolution:              60,
	NoSampleAgent:           true, // low cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaProxyHeapIdle = &MetricMetaValue{
	Name:                    "__igp_heap_idle",
	Kind:                    MetricKindValue,
	Description:             "StatsHouse proxy bytes in idle (unused) spans.",
	Resolution:              60,
	NoSampleAgent:           true, // low cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaProxyHeapInuse = &MetricMetaValue{
	Name:                    "__igp_heap_inuse",
	Kind:                    MetricKindValue,
	Description:             "StatsHouse proxy bytes in in-use spans.",
	Resolution:              60,
	NoSampleAgent:           true, // low cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaApiVmSize = &MetricMetaValue{
	Name:                    "__api_vm_size",
	Kind:                    MetricKindValue,
	Description:             "StatsHouse API virtual memory size.",
	Resolution:              60,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaApiVmRSS = &MetricMetaValue{
	Name:                    "__api_vm_rss",
	Kind:                    MetricKindValue,
	Description:             "StatsHouse API resident set size.",
	Resolution:              60,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaApiHeapAlloc = &MetricMetaValue{
	Name:                    "__api_heap_alloc",
	Kind:                    MetricKindValue,
	Description:             "StatsHouse API bytes of allocated heap objects.",
	Resolution:              60,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaApiHeapSys = &MetricMetaValue{
	Name:                    "__api_heap_sys",
	Kind:                    MetricKindValue,
	Description:             "StatsHouse API bytes of heap memory obtained from the OS.",
	Resolution:              60,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaApiHeapIdle = &MetricMetaValue{
	Name:                    "__api_heap_idle",
	Kind:                    MetricKindValue,
	Description:             "StatsHouse API bytes in idle (unused) spans.",
	Resolution:              60,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaApiHeapInuse = &MetricMetaValue{
	Name:                    "__api_heap_inuse",
	Kind:                    MetricKindValue,
	Description:             "StatsHouse API bytes in in-use spans.",
	Resolution:              60,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "host",
	}},
}

var BuiltinMetricMetaClientWriteError = &MetricMetaValue{
	Name:                    "__src_client_write_err",
	Kind:                    MetricKindValue,
	MetricType:              MetricByte,
	Description:             "Bytes lost on StatsHouse clients.",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: true,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "lang",
		ValueComments: convertToValueComments(map[int32]string{
			1: "golang",
		}),
	}, {
		Description: "cause",
		ValueComments: convertToValueComments(map[int32]string{
			1: "would_block",
		}),
	}, {
		Description: "application",
	}},
}

// TODO - remove after all agents updated to v3
var BuiltinMetricMetaAgentTimingsLegacy = &MetricMetaValue{
	Name:                    "__src_timings_legacy",
	Kind:                    MetricKindValue,
	Description:             "Timings of agent operations",
	MetricType:              MetricNanosecond,
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "group",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDAgentTimingGroupPipeline: "pipeline",
			TagValueIDAgentTimingGroupSend:     "send",
		}),
	}, {
		Description: "measure",
		ValueComments: convertToValueComments(map[int32]string{
			// pipeline
			TagValueIDAgentTimingMapping:     "mapping",
			TagValueIDAgentTimingMappingSlow: "mapping_slow",
			TagValueIDAgentTimingApplyMetric: "apply_metric",
			TagValueIDAgentTimingFlush:       "flush",
			TagValueIDAgentTimingPreprocess:  "preprocess",
			// send
			TagValueIDAgentTimingSendRecent:   "send_recent",
			TagValueIDAgentTimingSendHistoric: "send_historic",
		}),
	}, {
		Description: "commit_timestamp",
		RawKind:     "timestamp",
	}, {
		Description: "commit_hash",
		RawKind:     "hex",
	}},
}

var BuiltinMetricMetaAggBucketInfo = &MetricMetaValue{
	Name:                    "__agg_bucket_info",
	Kind:                    MetricKindValue,
	Description:             `Statistics on received bucket`,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description:   "aggregator_role",
		ValueComments: convertToValueComments(aggregatorRoleToValue),
	}, {
		Description: "measurement",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDAggBucketInfoRows:               "rows",
			TagValueIDAggBucketInfoIntKeys:            "int_keys",
			TagValueIDAggBucketInfoStringKeys:         "string_keys",
			TagValueIDAggBucketInfoMappingHits:        "mapping_hits",
			TagValueIDAggBucketInfoMappingMisses:      "mapping_misses",
			TagValueIDAggBucketInfoMappingUnknownKeys: "mapping_unknown_keys",
			TagValueIDAggBucketInfoMappingLocks:       "locks",
			TagValueIDAggBucketInfoCentroids:          "centroids",
			TagValueIDAggBucketInfoUniqueBytes:        "unique_bytes",
			TagValueIDAggBucketInfoStringTops:         "string_tops",
			TagValueIDAggBucketInfoIntTops:            "int_tops",
			TagValueIDAggBucketInfoNewKeys:            "new_keys",
			TagValueIDAggBucketInfoMetrics:            "metrics",
			TagValueIDAggBucketInfoOutdatedRows:       "outdated_rows",
		}),
	}, {}},
}

var BuiltinMetricMetaBudgetOwner = &MetricMetaValue{
	Name:                    "__budget_owner",
	Kind:                    MetricKindCounter,
	Disable:                 true, // disabled, but owner mapping is flood-protected
	Description:             "Invisible metric used only for accounting budget to create owner mappings",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        false,
	Tags:                    []MetricMetaTag{},
}

var BuiltinMetricMetaMappingCacheElements = &MetricMetaValue{
	Name:                    "__mapping_cache_elements",
	Kind:                    MetricKindValue,
	Description:             "",   // self-explanatory
	NoSampleAgent:           true, // low cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}},
}

var BuiltinMetricMetaMappingCacheSize = &MetricMetaValue{
	Name:                    "__mapping_cache_size",
	Kind:                    MetricKindValue,
	Description:             "", // self-explanatory
	MetricType:              MetricByte,
	NoSampleAgent:           true, // low cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}},
}

var BuiltinMetricMetaMappingCacheAverageTTL = &MetricMetaValue{
	Name:                    "__mapping_cache_average_ttl",
	Kind:                    MetricKindValue,
	Description:             "Average cache element timestamp relative to now",
	MetricType:              MetricSecond,
	NoSampleAgent:           true, // low cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}},
}

var BuiltinMetricMetaMappingCacheEvent = &MetricMetaValue{
	Name:                    "__mapping_cache_events",
	Kind:                    MetricKindCounter,
	Description:             "",   // self-explanatory
	NoSampleAgent:           true, // low cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{
		Description:   "component",
		ValueComments: convertToValueComments(componentToValue),
	}, {
		Description: "event",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDMappingCacheEventHit:                 "hit",
			TagValueIDMappingCacheEventMiss:                "miss",
			TagValueIDMappingCacheEventTimestampUpdate:     "update",
			TagValueIDMappingCacheEventTimestampUpdateSkip: "update_skip",
			TagValueIDMappingCacheEventAdd:                 "add",
			TagValueIDMappingCacheEventEvict:               "evict",
		}),
	}},
}

var BuiltinMetricMetaMappingQueueSize = &MetricMetaValue{
	Name:                    "__mapping_queue_size",
	Kind:                    MetricKindValue,
	Description:             "Elements in aggregator new conveyor mapping queue",
	NoSampleAgent:           true, // low cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags:                    []MetricMetaTag{{ // reserve for component
	}},
}

var BuiltinMetricMetaMappingQueueEvent = &MetricMetaValue{
	Name:                    "__mapping_queue_events",
	Kind:                    MetricKindCounter,
	Description:             "Events in aggregator new conveyor mapping queue",
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{ // reserve for component
	}, {
		Description: "event",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDMappingQueueEventUnknownMapRemove:  "uknown_map_remove",
			TagValueIDMappingQueueEventUnknownMapAdd:     "unknown_map_add",
			TagValueIDMappingQueueEventUnknownListRemove: "unknown_list_remove",
			TagValueIDMappingQueueEventUnknownListAdd:    "unknown_list_add",
			TagValueIDMappingQueueEventCreateMapAdd:      "create_map_add",
			TagValueIDMappingQueueEventCreateMapRemove:   "create_map_remove",
		}),
	}},
}

var BuiltinMetricMetaMappingQueueRemovedHitsAvg = &MetricMetaValue{
	Name:                    "__mapping_queue_removed_hits_avg",
	Kind:                    MetricKindValue,
	Description:             "When aggregator new conveyor mapping queue is full, elements will be removed. Their average hit counter is reported here.",
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags:                    []MetricMetaTag{{ // reserve for component
	}},
}

var BuiltinMetricMetaAgentTimings = &MetricMetaValue{
	Name:                    "__src_timings",
	Kind:                    MetricKindValue,
	Description:             "Timings of agent operations",
	MetricType:              MetricSecond,
	NoSampleAgent:           true, // low cardinality
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "group",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDAgentTimingGroupPipeline: "pipeline",
			TagValueIDAgentTimingGroupSend:     "send",
		}),
	}, {
		Description: "measure",
		ValueComments: convertToValueComments(map[int32]string{
			// pipeline
			TagValueIDAgentTimingMapping:     "mapping",
			TagValueIDAgentTimingMappingSlow: "mapping_slow",
			TagValueIDAgentTimingApplyMetric: "apply_metric",
			TagValueIDAgentTimingFlush:       "flush",
			TagValueIDAgentTimingPreprocess:  "preprocess",
			// send
			TagValueIDAgentTimingSendRecent:   "send_recent",
			TagValueIDAgentTimingSendHistoric: "send_historic",
		}),
	}, {
		Description: "commit_timestamp",
		RawKind:     "timestamp",
	}, {
		Description: "commit_hash",
		RawKind:     "hex",
	}},
}

// If all contributors come on time, count will be 1 (per shard). If some come through historic conveyor, can be larger.
var BuiltinMetricMetaAggInsertSize = &MetricMetaValue{
	Name:                    "__agg_insert_size",
	Kind:                    MetricKindValue,
	Description:             "Size of aggregated bucket inserted into clickhouse. Written when second is inserted, which can be much later.",
	MetricType:              MetricByte,
	NoSampleAgent:           true, // generated on aggregators, must be delivered without losses
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   false,
	WithAggregatorID:        true,
	Tags: []MetricMetaTag{{}, {}, {}, {
		Description:   "conveyor",
		ValueComments: convertToValueComments(conveyorToValue),
	}, {
		Description: "status",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDStatusOK:    "ok",
			TagValueIDStatusError: "error",
		}),
	}, {
		Description: "http_status",
		RawKind:     "int",
	}, {
		Description:   "clickhouse_exception",
		ValueComments: convertToValueComments(clickhouseExceptions),
	}, {
		Description:   "table",
		ValueComments: convertToValueComments(tableFormatToValue),
	}, {
		Description:   "type",
		ValueComments: convertToValueComments(insertKindToValue),
	}},
}

var BuiltinMetricMetaAggOldMetrics = &MetricMetaValue{
	Name:                    "__agg_old_metrics",
	Kind:                    MetricKindCounter,
	Resolution:              60,
	Description:             "Distribution of historical points",
	NoSampleAgent:           false,
	BuiltinAllowedToReceive: false,
	WithAgentEnvRouteArch:   true,
	WithAggregatorID:        false,
	Tags: []MetricMetaTag{{
		Description: "age",
		ValueComments: convertToValueComments(map[int32]string{
			TagValueIDOldMetricForm6hTo1d: "6h-1d",
			TagValueIDOldMetricForm1dTo2d: "1d-2d",
			TagValueIDOldMetricForm2d:     "2d+",
		}),
	}, {
		Description: "metric",
		BuiltinKind: BuiltinKindMetric,
	}},
}

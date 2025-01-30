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

	// [-1000..-2000] reserved by host system metrics
	// [-10000..-12000] reserved by builtin dashboard
	// [-20000..-22000] reserved by well known configuration IDs
	PrometheusConfigID          = -20000
	PrometheusGeneratedConfigID = -20001
	KnownTagsConfigID           = -20002
)

var (
	BuiltinMetricByName map[string]*MetricMetaValue

	BuiltinMetrics = map[int32]*MetricMetaValue{
		BuiltinMetricIDAgentSamplingFactor:        BuiltinMetricMetaAgentSamplingFactor,
		BuiltinMetricIDAggBucketReceiveDelaySec:   BuiltinMetricMetaAggBucketReceiveDelaySec,
		BuiltinMetricIDAggInsertSize:              BuiltinMetricMetaAggInsertSize,
		BuiltinMetricIDTLByteSizePerInflightType:  BuiltinMetricMetaTLByteSizePerInflightType,
		BuiltinMetricIDAggKeepAlive:               BuiltinMetricMetaAggKeepAlive,
		BuiltinMetricIDAggSizeCompressed:          BuiltinMetricMetaAggSizeCompressed,
		BuiltinMetricIDAggSizeUncompressed:        BuiltinMetricMetaAggSizeUncompressed,
		BuiltinMetricIDAggHourCardinality:         BuiltinMetricMetaAggHourCardinality,
		BuiltinMetricIDAggSamplingFactor:          BuiltinMetricMetaAggSamplingFactor,
		BuiltinMetricIDIngestionStatus:            BuiltinMetricMetaIngestionStatus,
		BuiltinMetricIDAggInsertTime:              BuiltinMetricMetaAggInsertTime,
		BuiltinMetricIDAggHistoricBucketsWaiting:  BuiltinMetricMetaAggHistoricBucketsWaiting,
		BuiltinMetricIDAggBucketAggregateTimeSec:  BuiltinMetricMetaAggBucketAggregateTimeSec,
		BuiltinMetricIDAggActiveSenders:           BuiltinMetricMetaAggActiveSenders,
		BuiltinMetricIDAggOutdatedAgents:          BuiltinMetricMetaAggOutdatedAgents,
		BuiltinMetricIDAgentDiskCacheErrors:       BuiltinMetricMetaAgentDiskCacheErrors,
		BuiltinMetricIDTimingErrors:               BuiltinMetricMetaTimingErrors,
		BuiltinMetricIDAgentReceivedBatchSize:     BuiltinMetricMetaAgentReceivedBatchSize,
		BuiltinMetricIDAggMapping:                 BuiltinMetricMetaAggMapping,
		BuiltinMetricIDAggInsertTimeReal:          BuiltinMetricMetaAggInsertTimeReal,
		BuiltinMetricIDAgentHistoricQueueSize:     BuiltinMetricMetaAgentHistoricQueueSize,
		BuiltinMetricIDAggHistoricSecondsWaiting:  BuiltinMetricMetaAggHistoricSecondsWaiting,
		BuiltinMetricIDAggInsertSizeReal:          BuiltinMetricMetaAggInsertSizeReal,
		BuiltinMetricIDAgentMapping:               BuiltinMetricMetaAgentMapping,
		BuiltinMetricIDAgentReceivedPacketSize:    BuiltinMetricMetaAgentReceivedPacketSize,
		BuiltinMetricIDAggMappingCreated:          BuiltinMetricMetaAggMappingCreated,
		BuiltinMetricIDVersions:                   BuiltinMetricMetaVersions,
		BuiltinMetricIDBadges:                     BuiltinMetricMetaBadges,
		BuiltinMetricIDAutoConfig:                 BuiltinMetricMetaAutoConfig,
		BuiltinMetricIDJournalVersions:            BuiltinMetricMetaJournalVersions,
		BuiltinMetricIDPromScrapeTime:             BuiltinMetricMetaPromScrapeTime,
		BuiltinMetricIDUsageMemory:                BuiltinMetricMetaUsageMemory,
		BuiltinMetricIDUsageCPU:                   BuiltinMetricMetaUsageCPU,
		BuiltinMetricIDHeartbeatVersion:           BuiltinMetricMetaHeartbeatVersion,
		BuiltinMetricIDHeartbeatArgs:              BuiltinMetricMetaHeartbeatArgs,
		BuiltinMetricIDAPIBRS:                     BuiltinMetricMetaAPIBRS,
		BuiltinMetricIDBudgetHost:                 BuiltinMetricMetaBudgetHost,
		BuiltinMetricIDBudgetAggregatorHost:       BuiltinMetricMetaBudgetAggregatorHost,
		BuiltinMetricIDAPIActiveQueries:           BuiltinMetricMetaAPIActiveQueries,
		BuiltinMetricIDRPCRequests:                BuiltinMetricMetaRPCRequests,
		BuiltinMetricIDBudgetUnknownMetric:        BuiltinMetricMetaBudgetUnknownMetric,
		BuiltinMetricIDContributorsLog:            BuiltinMetricMetaContributorsLog,
		BuiltinMetricIDContributorsLogRev:         BuiltinMetricMetaContributorsLogRev,
		BuiltinMetricIDGroupSizeBeforeSampling:    BuiltinMetricMetaGroupSizeBeforeSampling,
		BuiltinMetricIDGroupSizeAfterSampling:     BuiltinMetricMetaGroupSizeAfterSampling,
		BuiltinMetricIDAPISelectBytes:             BuiltinMetricMetaAPISelectBytes,
		BuiltinMetricIDAPISelectRows:              BuiltinMetricMetaAPISelectRows,
		BuiltinMetricIDAPISelectDuration:          BuiltinMetricMetaAPISelectDuration,
		BuiltinMetricIDAgentHistoricQueueSizeSum:  BuiltinMetricMetaAgentHistoricQueueSizeSum,
		BuiltinMetricIDAPISourceSelectRows:        BuiltinMetricMetaAPISourceSelectRows,
		BuiltinMetricIDSystemMetricScrapeDuration: BuiltinMetricMetaSystemMetricScrapeDuration,
		BuiltinMetricIDMetaServiceTime:            BuiltinMetricMetaMetaServiceTime,
		BuiltinMetricIDMetaClientWaits:            BuiltinMetricMetaMetaClientWaits,
		BuiltinMetricIDAgentUDPReceiveBufferSize:  BuiltinMetricMetaAgentUDPReceiveBufferSize,
		BuiltinMetricIDAPIMetricUsage:             BuiltinMetricMetaAPIMetricUsage,
		BuiltinMetricIDAPIServiceTime:             BuiltinMetricMetaAPIServiceTime,
		BuiltinMetricIDAPIResponseTime:            BuiltinMetricMetaAPIResponseTime,
		BuiltinMetricIDSrcTestConnection:          BuiltinMetricMetaSrcTestConnection,
		BuiltinMetricIDAgentAggregatorTimeDiff:    BuiltinMetricMetaAgentAggregatorTimeDiff,
		BuiltinMetricIDSrcSamplingMetricCount:     BuiltinMetricMetaSrcSamplingMetricCount,
		BuiltinMetricIDAggSamplingMetricCount:     BuiltinMetricMetaAggSamplingMetricCount,
		BuiltinMetricIDSrcSamplingSizeBytes:       BuiltinMetricMetaSrcSamplingSizeBytes,
		BuiltinMetricIDAggSamplingSizeBytes:       BuiltinMetricMetaAggSamplingSizeBytes,
		BuiltinMetricIDUIErrors:                   BuiltinMetricMetaUIErrors,
		BuiltinMetricIDStatsHouseErrors:           BuiltinMetricMetaStatsHouseErrors,
		BuiltinMetricIDSrcSamplingBudget:          BuiltinMetricMetaSrcSamplingBudget,
		BuiltinMetricIDAggSamplingBudget:          BuiltinMetricMetaAggSamplingBudget,
		BuiltinMetricIDSrcSamplingGroupBudget:     BuiltinMetricMetaSrcSamplingGroupBudget,
		BuiltinMetricIDAggSamplingGroupBudget:     BuiltinMetricMetaAggSamplingGroupBudget,
		BuiltinMetricIDPromQLEngineTime:           BuiltinMetricMetaPromQLEngineTime,
		BuiltinMetricIDAPICacheHit:                BuiltinMetricMetaAPICacheHit,
		BuiltinMetricIDAggScrapeTargetDispatch:    BuiltinMetricMetaAggScrapeTargetDispatch,
		BuiltinMetricIDAggScrapeTargetDiscovery:   BuiltinMetricMetaAggScrapeTargetDiscovery,
		BuiltinMetricIDAggScrapeConfigHash:        BuiltinMetricMetaAggScrapeConfigHash,
		BuiltinMetricIDAggSamplingTime:            BuiltinMetricMetaAggSamplingTime,
		BuiltinMetricIDAgentDiskCacheSize:         BuiltinMetricMetaAgentDiskCacheSize,
		BuiltinMetricIDAggContributors:            BuiltinMetricMetaAggContributors,
		BuiltinMetricIDAPIBufferBytesAlloc:        BuiltinMetricMetaAPIBufferBytesAlloc,
		BuiltinMetricIDAPIBufferBytesFree:         BuiltinMetricMetaAPIBufferBytesFree,
		BuiltinMetricIDAPIBufferBytesTotal:        BuiltinMetricMetaAPIBufferBytesTotal,
		BuiltinMetricIDAutoCreateMetric:           BuiltinMetricMetaAutoCreateMetric,
		BuiltinMetricIDRestartTimings:             BuiltinMetricMetaRestartTimings,
		BuiltinMetricIDGCDuration:                 BuiltinMetricMetaGCDuration,
		BuiltinMetricIDAggHistoricHostsWaiting:    BuiltinMetricMetaAggHistoricHostsWaiting,
		BuiltinMetricIDAggSamplingEngineTime:      BuiltinMetricMetaAggSamplingEngineTime,
		BuiltinMetricIDAggSamplingEngineKeys:      BuiltinMetricMetaAggSamplingEngineKeys,
		BuiltinMetricIDProxyAcceptHandshakeError:  BuiltinMetricMetaProxyAcceptHandshakeError,
		BuiltinMetricIDProxyVmSize:                BuiltinMetricMetaProxyVmSize,
		BuiltinMetricIDProxyVmRSS:                 BuiltinMetricMetaProxyVmRSS,
		BuiltinMetricIDProxyHeapAlloc:             BuiltinMetricMetaProxyHeapAlloc,
		BuiltinMetricIDProxyHeapSys:               BuiltinMetricMetaProxyHeapSys,
		BuiltinMetricIDProxyHeapIdle:              BuiltinMetricMetaProxyHeapIdle,
		BuiltinMetricIDProxyHeapInuse:             BuiltinMetricMetaProxyHeapInuse,
		BuiltinMetricIDApiVmSize:                  BuiltinMetricMetaApiVmSize,
		BuiltinMetricIDApiVmRSS:                   BuiltinMetricMetaApiVmRSS,
		BuiltinMetricIDApiHeapAlloc:               BuiltinMetricMetaApiHeapAlloc,
		BuiltinMetricIDApiHeapSys:                 BuiltinMetricMetaApiHeapSys,
		BuiltinMetricIDApiHeapIdle:                BuiltinMetricMetaApiHeapIdle,
		BuiltinMetricIDApiHeapInuse:               BuiltinMetricMetaApiHeapInuse,
		BuiltinMetricIDClientWriteError:           BuiltinMetricMetaClientWriteError,
		BuiltinMetricIDAgentTimings:               BuiltinMetricMetaAgentTimings,
		BuiltinMetricIDAggBucketInfo:              BuiltinMetricMetaAggBucketInfo,
		BuiltinMetricIDBudgetOwner:                BuiltinMetricMetaBudgetOwner,
		BuiltinMetricIDMappingCacheElements:       BuiltinMetricMetaMappingCacheElements,
		BuiltinMetricIDMappingCacheSize:           BuiltinMetricMetaMappingCacheSize,
		BuiltinMetricIDMappingCacheAverageTTL:     BuiltinMetricMetaMappingCacheAverageTTL,
		BuiltinMetricIDMappingCacheEvent:          BuiltinMetricMetaMappingCacheEvent,
		BuiltinMetricIDMappingQueueSize:           BuiltinMetricMetaMappingQueueSize,
		BuiltinMetricIDMappingQueueEvent:          BuiltinMetricMetaMappingQueueEvent,
		BuiltinMetricIDMappingQueueRemovedHitsAvg: BuiltinMetricMetaMappingQueueRemovedHitsAvg,
	}

	// this set is very small, and we do not want to set Visible property for hunderds of metrics
	builtinMetricsInvisible = map[int32]bool{
		BuiltinMetricIDBudgetHost:           true,
		BuiltinMetricIDBudgetAggregatorHost: true,
		BuiltinMetricIDBudgetUnknownMetric:  true,
		BuiltinMetricIDBudgetOwner:          true,
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
		// v.NoSampleAgent = false
		v.BuiltinAllowedToReceive = true
		// v.WithAgentEnvRouteArch = false
		// v.WithAggregatorID = false
	}
	for i := 0; i < NewMaxTags; i++ {
		name := strconv.Itoa(i)
		legacyName := legacyTagIDPrefix + name
		tagIDsLegacy = append(tagIDsLegacy, legacyName)
		tagIDs = append(tagIDs, name)
		tagIDToIndex[name] = i
		apiCompatTagID[name] = name
		apiCompatTagID[legacyName] = name
		tagIDTag2TagID[int32(i+TagIDShiftLegacy)] = legacyName
		tagIDTag2TagID[int32(i+TagIDShift)] = tagStringForUI + " " + strconv.Itoa(i) // for UI only
		defaultMetaTags = append(defaultMetaTags, MetricMetaTag{Index: int32(i)})
	}
	defaultMetaTags[0].Description = "environment" // the only fixed description
	defaultSTag.Index = StringTopTagIndex
	defaultHTag.Index = HostTagIndex

	apiCompatTagID[StringTopTagID] = StringTopTagID
	apiCompatTagID[LegacyStringTopTagID] = StringTopTagID
	tagIDTag2TagID[TagIDShiftLegacy-1] = StringTopTagID
	tagIDTag2TagID[TagIDShift-1] = tagStringForUI + " " + StringTopTagID // for UI only
	tagIDTag2TagID[TagIDShift-2] = tagStringForUI + " " + HostTagID      // for UI only

	BuiltinMetricByName = make(map[string]*MetricMetaValue, len(BuiltinMetrics))
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

		if id == BuiltinMetricIDIngestionStatus || id == BuiltinMetricIDAggMappingCreated {
			m.Tags = append([]MetricMetaTag{{Description: "environment"}}, m.Tags...)
		} else {
			m.Tags = append([]MetricMetaTag{{Description: "-"}}, m.Tags...)
		}
		for len(m.Tags) < MaxTags {
			m.Tags = append(m.Tags, MetricMetaTag{Description: "-"})
		}
		if m.WithAggregatorID {
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
		if m.WithAgentEnvRouteArch {
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
}

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package format

import (
	"log"
	"math"
	"strconv"
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
		BuiltinMetricIDAgentSamplingFactor:      BuiltinMetricMetaAgentSamplingFactor,      // used for badges
		BuiltinMetricIDAggBucketReceiveDelaySec: BuiltinMetricMetaAggBucketReceiveDelaySec, // used for badges
		-3:                                      BuiltinMetricMetaAggInsertSizeLegacy,
		-4:                                      BuiltinMetricMetaTLByteSizePerInflightType,
		-5:                                      BuiltinMetricMetaAggKeepAlive,
		-6:                                      BuiltinMetricMetaAggSizeCompressed,
		-7:                                      BuiltinMetricMetaAggSizeUncompressed,
		BuiltinMetricIDAggHourCardinality:       BuiltinMetricMetaAggHourCardinality, // sharded by metric tag, caculated on aggregator
		BuiltinMetricIDAggSamplingFactor:        BuiltinMetricMetaAggSamplingFactor,  // sharded by metric tag, used for badges
		BuiltinMetricIDIngestionStatus:          BuiltinMetricMetaIngestionStatus,    // sharded by metric tag, used for badges
		-12:                                     BuiltinMetricMetaAggInsertTime,
		-13:                                     BuiltinMetricMetaAggHistoricBucketsWaiting,
		-14:                                     BuiltinMetricMetaAggBucketAggregateTimeSec,
		-15:                                     BuiltinMetricMetaAggActiveSenders,
		-16:                                     BuiltinMetricMetaAggOutdatedAgents,
		-18:                                     BuiltinMetricMetaAgentDiskCacheErrors,
		-20:                                     BuiltinMetricMetaTimingErrors,
		-21:                                     BuiltinMetricMetaAgentReceivedBatchSize,
		-23:                                     BuiltinMetricMetaAggMapping,
		-24:                                     BuiltinMetricMetaAggInsertTimeReal,
		-25:                                     BuiltinMetricMetaAgentHistoricQueueSize,
		-26:                                     BuiltinMetricMetaAggHistoricSecondsWaiting,
		-27:                                     BuiltinMetricMetaAggInsertSizeReal,
		-30:                                     BuiltinMetricMetaAgentMapping,
		-31:                                     BuiltinMetricMetaAgentReceivedPacketSize,
		BuiltinMetricIDAggMappingCreated:        BuiltinMetricMetaAggMappingCreated, // used for bages
		-34:                                     BuiltinMetricMetaVersions,
		BuiltinMetricIDBadges:                   BuiltinMetricMetaBadges, // sharded by metric tag
		-36:                                     BuiltinMetricMetaAutoConfig,
		-37:                                     BuiltinMetricMetaJournalVersions,
		-38:                                     BuiltinMetricMetaPromScrapeTime,
		-43:                                     BuiltinMetricMetaUsageMemory,
		-44:                                     BuiltinMetricMetaUsageCPU,
		BuiltinMetricIDHeartbeatVersion:         BuiltinMetricMetaHeartbeatVersion, // used to set extra tags on agg, can be removed
		BuiltinMetricIDHeartbeatArgs:            BuiltinMetricMetaHeartbeatArgs,    // used to set extra tags on agg, can be removed
		-50:                                     BuiltinMetricMetaAPIBRS,
		-53:                                     BuiltinMetricMetaBudgetHost,
		-54:                                     BuiltinMetricMetaBudgetAggregatorHost,
		-55:                                     BuiltinMetricMetaAPIActiveQueries,
		BuiltinMetricIDRPCRequests:              BuiltinMetricMetaRPCRequests,         // used to set extra tags on agg, can be removed
		BuiltinMetricIDBudgetUnknownMetric:      BuiltinMetricMetaBudgetUnknownMetric, // used only for accounting
		BuiltinMetricIDContributorsLog:          BuiltinMetricMetaContributorsLog,     // generated and written on aggregator
		BuiltinMetricIDContributorsLogRev:       BuiltinMetricMetaContributorsLogRev,  // generated and written on aggregator
		-64:                                     BuiltinMetricMetaGroupSizeBeforeSampling,
		-65:                                     BuiltinMetricMetaGroupSizeAfterSampling,
		-66:                                     BuiltinMetricMetaAPISelectBytes,
		-67:                                     BuiltinMetricMetaAPISelectRows,
		-68:                                     BuiltinMetricMetaAPISelectDuration,
		-70:                                     BuiltinMetricMetaAPISourceSelectRows,
		-71:                                     BuiltinMetricMetaSystemMetricScrapeDuration,
		-72:                                     BuiltinMetricMetaMetaServiceTime,
		-73:                                     BuiltinMetricMetaMetaClientWaits,
		-74:                                     BuiltinMetricMetaAgentUDPReceiveBufferSize,
		-75:                                     BuiltinMetricMetaAPIMetricUsage,
		-76:                                     BuiltinMetricMetaAPIServiceTime,
		-77:                                     BuiltinMetricMetaAPIResponseTime,
		-78:                                     BuiltinMetricMetaSrcTestConnection,
		-79:                                     BuiltinMetricMetaAgentAggregatorTimeDiff,
		-80:                                     BuiltinMetricMetaSrcSamplingMetricCount,
		-81:                                     BuiltinMetricMetaAggSamplingMetricCount,
		-82:                                     BuiltinMetricMetaSrcSamplingSizeBytes,
		-83:                                     BuiltinMetricMetaAggSamplingSizeBytes,
		-84:                                     BuiltinMetricMetaUIErrors,
		-85:                                     BuiltinMetricMetaStatsHouseErrors,
		-86:                                     BuiltinMetricMetaSrcSamplingBudget,
		-87:                                     BuiltinMetricMetaAggSamplingBudget,
		-88:                                     BuiltinMetricMetaSrcSamplingGroupBudget,
		-89:                                     BuiltinMetricMetaAggSamplingGroupBudget,
		-90:                                     BuiltinMetricMetaPromQLEngineTime,
		-91:                                     BuiltinMetricMetaAPICacheHit,
		-92:                                     BuiltinMetricMetaAggScrapeTargetDispatch,
		-93:                                     BuiltinMetricMetaAggScrapeTargetDiscovery,
		-94:                                     BuiltinMetricMetaAggScrapeConfigHash,
		-95:                                     BuiltinMetricMetaAggSamplingTime,
		-96:                                     BuiltinMetricMetaAgentDiskCacheSize,
		-97:                                     BuiltinMetricMetaAggContributors,
		-99:                                     BuiltinMetricMetaAPICacheChunkCount,
		-101:                                    BuiltinMetricMetaAPICacheSize,
		-102:                                    BuiltinMetricMetaAPICacheTrim,
		-103:                                    BuiltinMetricMetaAPICacheAge,
		-104:                                    BuiltinMetricMetaAPIBufferBytesAlloc,
		-105:                                    BuiltinMetricMetaAPIBufferBytesFree,
		-106:                                    BuiltinMetricMetaAPIBufferBytesTotal,
		-107:                                    BuiltinMetricMetaAutoCreateMetric,
		-108:                                    BuiltinMetricMetaRestartTimings,
		-109:                                    BuiltinMetricMetaGCDuration,
		-110:                                    BuiltinMetricMetaAggHistoricHostsWaiting,
		-111:                                    BuiltinMetricMetaAggSamplingEngineTime,
		-112:                                    BuiltinMetricMetaAggSamplingEngineKeys,
		-113:                                    BuiltinMetricMetaProxyAcceptHandshakeError,
		-114:                                    BuiltinMetricMetaProxyVmSize,
		-115:                                    BuiltinMetricMetaProxyVmRSS,
		-116:                                    BuiltinMetricMetaProxyHeapAlloc,
		-117:                                    BuiltinMetricMetaProxyHeapSys,
		-118:                                    BuiltinMetricMetaProxyHeapIdle,
		-119:                                    BuiltinMetricMetaProxyHeapInuse,
		-120:                                    BuiltinMetricMetaApiVmSize,
		-121:                                    BuiltinMetricMetaApiVmRSS,
		-122:                                    BuiltinMetricMetaApiHeapAlloc,
		-123:                                    BuiltinMetricMetaApiHeapSys,
		-124:                                    BuiltinMetricMetaApiHeapIdle,
		-125:                                    BuiltinMetricMetaApiHeapInuse,
		-126:                                    BuiltinMetricMetaClientWriteError,
		-127:                                    BuiltinMetricMetaAgentTimingsLegacy,
		-128:                                    BuiltinMetricMetaAggBucketInfo,
		-129:                                    BuiltinMetricMetaBudgetOwner,
		-130:                                    BuiltinMetricMetaMappingCacheElements,
		-131:                                    BuiltinMetricMetaMappingCacheSize,
		-132:                                    BuiltinMetricMetaMappingCacheAverageTTL,
		-133:                                    BuiltinMetricMetaMappingCacheEvent,
		-134:                                    BuiltinMetricMetaMappingQueueSize,
		-135:                                    BuiltinMetricMetaMappingQueueEvent,
		-136:                                    BuiltinMetricMetaMappingQueueRemovedHitsAvg,
		-137:                                    BuiltinMetricMetaAgentTimings,
		-138:                                    BuiltinMetricMetaAggInsertSize,
		-139:                                    BuiltinMetricMetaAggOldMetrics,
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
		}
		m.PreKeyFrom = math.MaxInt32 // allow writing, but not yet selecting

		BuiltinMetricByName[m.Name] = m

		if id == BuiltinMetricIDIngestionStatus || id == BuiltinMetricIDAggMappingCreated {
			m.Tags = append([]MetricMetaTag{{Description: "environment"}}, m.Tags...)
		} else {
			m.Tags = append([]MetricMetaTag{{Description: "-"}}, m.Tags...)
		}
		for len(m.Tags) < MaxTags { // prevent overflows in code below
			m.Tags = append(m.Tags, MetricMetaTag{Description: "-"})
		}
		if m.WithAggregatorID {
			m.Tags[AggHostTag] = MetricMetaTag{Description: "aggregator_host"}
			m.Tags[AggShardTag] = MetricMetaTag{Description: "aggregator_shard", RawKind: "int"}
			m.Tags[AggReplicaTag] = MetricMetaTag{Description: "aggregator_replica", RawKind: "int"}
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
					TagValueIDProduction:  "statshouse.production",
					TagValueIDStaging1:    "statshouse.staging1",
					TagValueIDStaging2:    "statshouse.staging2",
					TagValueIDDevelopment: "statshouse.development",
				}),
			}
			m.Tags[BuildArchTag] = MetricMetaTag{
				Description:   "build_arch",
				ValueComments: convertToValueComments(buildArchToValue),
			}
		}

		for i := range m.Tags {
			t := &m.Tags[i]
			if t.Description == "" && t.Name == "" {
				t.Description = "-" // remove unused tags from UI
			}
			if t.Raw {
				panic("for built-in metric definitions please set only raw_kind, not raw flag")
			}
			if i == 0 { // env is not raw
				continue
			}
			if t.Description == "tag_id" { // cannot set at init() because # of tags is dynamic
				t.ValueComments = convertToValueComments(tagIDTag2TagID)
			}
			if t.RawKind != "" {
				continue
			}
			if t.Description == "-" && t.Name == "" {
				t.RawKind = "int" // we have garbage from the past there, avoid mapping it
			}
			if t.BuiltinKind != 0 || t.ValueComments != nil {
				t.RawKind = "int"
			}
		}

		_ = m.RestoreCachedInfo()
	}
}

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package format

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"runtime/debug"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"go.uber.org/multierr"
	"go4.org/mem"

	"github.com/mailru/easyjson/opt"

	"github.com/vkcom/statshouse-go"
)

const (
	MaxTags      = 16
	NewMaxTags   = 48
	MaxDraftTags = 128
	MaxStringLen = 128 // both for normal tags and _s, _h tags (string tops, hostnames)

	tagValueCodePrefix     = " " // regular tag values can't start with whitespace
	TagValueCodeZero       = tagValueCodePrefix + "0"
	TagValueIDUnspecified  = 0
	TagValueIDMappingFlood = -1

	EffectiveWeightOne          = 128                      // metric.Weight is multiplied by this and rounded. Do not make too big or metric with weight set to 0 will disappear completely.
	MaxEffectiveWeight          = 100 * EffectiveWeightOne // do not make too high, we multiply this by sum of metric serialized length during sampling
	MaxEffectiveGroupWeight     = 10_000 * EffectiveWeightOne
	MaxEffectiveNamespaceWeight = 10_000 * EffectiveWeightOne

	StringTopTagID            = "_s"
	StringTopTagIDV3          = "47" // for backward compatibility with v2 we write "_s" into "47"
	HostTagID                 = "_h"
	ShardTagID                = "_shard_num"
	EnvTagID                  = "0"
	LETagName                 = "le"
	ScrapeNamespaceTagName    = "__scrape_namespace__"
	HistogramBucketsStartMark = "Buckets$"
	HistogramBucketsDelim     = ","
	HistogramBucketsDelimC    = ','
	HistogramBucketsEndMark   = "$"
	HistogramBucketsEndMarkC  = '$'

	LETagIndex          = 15
	StringTopTagIndex   = -1 // used as flag during mapping
	StringTopTagIndexV3 = 47
	HostTagIndex        = -2 // used as flag during mapping

	// added for lots of built-in metrics automatically
	BuildArchTag  = 10
	AggHostTag    = 11
	AggShardTag   = 12
	AggReplicaTag = 13
	RouteTag      = 14
	AgentEnvTag   = 15

	// legacy values, used only by API to process data from version1 database
	TagValueIDProductionLegacy   = 22676293   // stlogs_s_production, actual value matters only for legacy conveyor
	TagValueNullLegacy           = "null"     // see TagValueIDNull
	TagValueIDMappingFloodLegacy = 22136242   // STATLOGS_KEY_KEYOVERFLOW_INT
	TagValueIDRawDeltaLegacy     = 10_000_000 // STATLOGS_INT_NOCONVERT

	NamespaceSeparator     = ":"
	NamespaceSeparatorRune = ':'

	// if more than 0 then agents with older commitTs will be declined
	LeastAllowedAgentCommitTs = 0
)

// Do not change values, they are stored in DB
const (
	MetricKindCounter          = "counter"
	MetricKindValue            = "value"
	MetricKindValuePercentiles = "value_p"
	MetricKindUnique           = "unique"
	MetricKindMixed            = "mixed"
	MetricKindMixedPercentiles = "mixed_p"
)

const (
	MetricSecond      = "second"
	MetricMillisecond = "millisecond"
	MetricMicrosecond = "microsecond"
	MetricNanosecond  = "nanosecond"

	MetricByte       = "byte"
	MetricByteAsBits = "byte_as_bits"

	/*
		MetricBit = "bit"
		MetricKilobyte = "kilobyte"
		MetricMegabyte = "megabyte"
		MetricGigabyte = "gigabyte"

		MetricKibibyte = "kibibyte"
		MetricMebibyte = "mebibyte"
	*/
)

// Legacy, left for API backward compatibility
const (
	LegacyStringTopTagID      = "skey"
	legacyTagIDPrefix         = "key"
	legacyEnvTagID            = legacyTagIDPrefix + "0"
	legacyMetricKindStringTop = "stop" // converted into counter during RestoreMetricInfo
)

var (
	tagIDs         []string             // initialized in builtin.go due to dependency
	tagIDTag2TagID = map[int32]string{} // initialized in builtin.go due to dependency
	tagIDToIndex   = map[string]int{}   // initialized in builtin.go due to dependency

	errInvalidCodeTagValue = fmt.Errorf("invalid code tag value") // must be fast
	errBadEncoding         = fmt.Errorf("bad utf-8 encoding")     // must be fast
	reservedMetricPrefix   = []string{"host_", "__"}
)

// Legacy, left for API backward compatibility
var (
	tagIDsLegacy   []string              // initialized in builtin.go due to dependency
	apiCompatTagID = map[string]string{} // initialized in builtin.go due to dependency
)

type MetricKind int

type MetaStorageInterface interface { // agent uses this to avoid circular dependencies
	Version() int64
	StateHash() string
	GetMetaMetric(metricID int32) *MetricMetaValue
	GetMetaMetricDelayed(metricID int32) *MetricMetaValue
	GetMetaMetricByName(metricName string) *MetricMetaValue
	GetGroup(id int32) *MetricsGroup
	GetNamespace(id int32) *NamespaceMeta
	GetNamespaceByName(name string) *NamespaceMeta
	GetGroupByName(name string) *MetricsGroup
}

// This struct is immutable, it is accessed by mapping code without any locking
type MetricMetaTag struct {
	Name          string            `json:"name,omitempty"`
	Description   string            `json:"description,omitempty"`
	Raw           bool              `json:"raw,omitempty"`
	RawKind       string            `json:"raw_kind,omitempty"` // UI can show some raw values beautifully - timestamps, hex values, etc.
	ID2Value      map[int32]string  `json:"id2value,omitempty"`
	ValueComments map[string]string `json:"value_comments,omitempty"`
	SkipMapping   bool              `json:"skip_mapping,omitempty"` // used only in v3 conveer

	Comment2Value map[string]string `json:"-"` // Should be restored from ValueComments after reading
	IsMetric      bool              `json:"-"` // Only for built-in metrics so never saved or parsed
	IsGroup       bool              `json:"-"` // Only for built-in metrics so never saved or parsed
	IsNamespace   bool              `json:"-"` // Only for built-in metrics so never saved or parsed
	Index         int               `json:"-"` // Should be restored from position in MetricMetaValue.Tags
}

const (
	MetricEvent       int32 = 0
	DashboardEvent    int32 = 1
	MetricsGroupEvent int32 = 2
	PromConfigEvent   int32 = 3
	NamespaceEvent    int32 = 4
)

type NamespaceMeta struct {
	ID         int32  `json:"namespace_id"`
	Name       string `json:"name"`
	Version    int64  `json:"version"`
	UpdateTime uint32 `json:"update_time"`
	DeleteTime uint32 `json:"delete_time"`

	Weight  float64 `json:"weight"`
	Disable bool    `json:"disable"`

	EffectiveWeight int64 `json:"-"`
}

// This struct is immutable, it is accessed by mapping code without any locking
type DashboardMeta struct {
	DashboardID int32  `json:"dashboard_id"` // I'm sure day will come when we will be forced to make this int32
	Name        string `json:"name"`
	Version     int64  `json:"version,omitempty"`
	UpdateTime  uint32 `json:"update_time"`

	DeleteTime uint32                 `json:"delete_time"` // TODO - do not store event-specific information in journal event.
	JSONData   map[string]interface{} `json:"data"`        // TODO - there must be a better way?
}

// This struct is immutable, it is accessed by mapping code without any locking
type MetricsGroup struct {
	ID          int32  `json:"group_id"`
	NamespaceID int32  `json:"namespace_id"`
	Name        string `json:"name"`
	Version     int64  `json:"version"`
	UpdateTime  uint32 `json:"update_time"`

	Weight  float64 `json:"weight,omitempty"`
	Disable bool    `json:"disable,omitempty"`

	EffectiveWeight int64          `json:"-"`
	Namespace       *NamespaceMeta `json:"-"`
}

// possible sharding strategies
const (
	ShardBy16MappedTagsHash   = "16_mapped_tags_hash"
	ShardBy16MappedTagsHashId = 0
	ShardFixed                = "fixed_shard"
	ShardFixedId              = 1
	ShardByTag                = "tag"
	ShardByTagId              = 2
	// some builtin metrics are produced direclty by aggregator, so they are written to shard in which they are produced
	ShardAggInternal   = "agg_internal"
	ShardAggInternalId = 3
	// shard = metric_id % num_shards
	// it's only used for hardware metrics, overall not recommended because shard depends on total number of shards
	ShardByMetric   = "metric_id"
	ShardByMetricId = 4
)

type MetricSharding struct {
	Strategy   string `json:"strategy"` // possible values: mapped_tags, fixed_shard, tag
	StrategyId int
	Shard      opt.Uint32 `json:"shard,omitempty"`  // only for "fixed_shard" strategy
	TagId      opt.Uint32 `json:"tag_id,omitempty"` // only for "tag" strategy
	AfterTs    opt.Uint32 `json:"after_ts,omitempty"`
}

// This struct is immutable, it is accessed by mapping code without any locking
type MetricMetaValue struct {
	MetricID    int32  `json:"metric_id"`
	NamespaceID int32  `json:"namespace_id"` // RO
	Name        string `json:"name"`
	Version     int64  `json:"version,omitempty"`
	UpdateTime  uint32 `json:"update_time"`

	Description          string                   `json:"description,omitempty"`
	Tags                 []MetricMetaTag          `json:"tags,omitempty"`
	TagsDraft            map[string]MetricMetaTag `json:"tags_draft,omitempty"`
	Visible              bool                     `json:"visible,omitempty"`
	Kind                 string                   `json:"kind"`
	Weight               float64                  `json:"weight,omitempty"`
	Resolution           int                      `json:"resolution,omitempty"`             // no invariants
	StringTopName        string                   `json:"string_top_name,omitempty"`        // no invariants
	StringTopDescription string                   `json:"string_top_description,omitempty"` // no invariants
	PreKeyTagID          string                   `json:"pre_key_tag_id,omitempty"`
	PreKeyFrom           uint32                   `json:"pre_key_from,omitempty"`
	SkipMaxHost          bool                     `json:"skip_max_host,omitempty"`
	SkipMinHost          bool                     `json:"skip_min_host,omitempty"`
	SkipSumSquare        bool                     `json:"skip_sum_square,omitempty"`
	PreKeyOnly           bool                     `json:"pre_key_only,omitempty"`
	MetricType           string                   `json:"metric_type"`
	FairKeyTagIDs        []string                 `json:"fair_key_tag_ids,omitempty"`
	Sharding             []MetricSharding         `json:"sharding,omitempty"`

	RawTagMask           uint32                   `json:"-"` // Should be restored from Tags after reading
	Name2Tag             map[string]MetricMetaTag `json:"-"` // Should be restored from Tags after reading
	EffectiveResolution  int                      `json:"-"` // Should be restored from Tags after reading
	PreKeyIndex          int                      `json:"-"` // index of tag which goes to 'prekey' column, or <0 if no tag goes
	FairKey              []int                    `json:"-"`
	EffectiveWeight      int64                    `json:"-"`
	HasPercentiles       bool                     `json:"-"`
	RoundSampleFactors   bool                     `json:"-"` // Experimental, set if magic word in description is found
	ShardUniqueValues    bool                     `json:"-"` // Experimental, set if magic word in description is found
	NoSampleAgent        bool                     `json:"-"` // Built-in metrics with fixed/limited # of rows on agent
	WhalesOff            bool                     `json:"-"` // "whales" sampling algorithm disabled
	HistorgamBuckets     []float32                `json:"-"` // Prometheus histogram buckets
	IsHardwareSlowMetric bool                     `json:"-"`
	GroupID              int32                    `json:"-"`

	Group     *MetricsGroup  `json:"-"` // don't use directly
	Namespace *NamespaceMeta `json:"-"` // don't use directly
}

type MetricMetaValueOld struct {
	MetricID             int32           `json:"metric_id"`
	Name                 string          `json:"name"`
	Description          string          `json:"description,omitempty"`
	Tags                 []MetricMetaTag `json:"tags,omitempty"`
	Visible              bool            `json:"visible,omitempty"`
	Kind                 string          `json:"kind"`
	Weight               int64           `json:"weight,omitempty"`
	Resolution           int             `json:"resolution,omitempty"`             // no invariants
	StringTopName        string          `json:"string_top_name,omitempty"`        // no invariants
	StringTopDescription string          `json:"string_top_description,omitempty"` // no invariants
	PreKeyTagID          string          `json:"pre_key_tag_id,omitempty"`
	PreKeyFrom           uint32          `json:"pre_key_from,omitempty"`
	UpdateTime           uint32          `json:"update_time"`
	Version              int64           `json:"version,omitempty"`
}

// TODO - better place?
type CreateMappingExtra struct {
	Create    bool
	Metric    string
	TagIDKey  int32
	ClientEnv int32
	AgentEnv  int32
	Route     int32
	BuildArch int32
	HostName  string
	Host      int32
}

func (m MetricMetaValue) MarshalBinary() ([]byte, error) { return json.Marshal(m) }
func (m *MetricMetaValue) UnmarshalBinary(data []byte) error {
	if err := json.Unmarshal(data, m); err != nil {
		return err
	}
	_ = m.RestoreCachedInfo() // name collisions must not prevent journal sync
	return nil
}

func MetricJSON(value *MetricMetaValue) ([]byte, error) {
	if err := value.RestoreCachedInfo(); err != nil {
		return nil, err
	}
	metricBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize metric: %w", err)
	}
	return metricBytes, nil
}

func (m DashboardMeta) MarshalBinary() ([]byte, error) { return json.Marshal(m) }
func (m *DashboardMeta) UnmarshalBinary(data []byte) error {
	if err := json.Unmarshal(data, m); err != nil {
		return err
	}
	return nil
}

// updates error if name collision happens
func (m *MetricMetaValue) setName2Tag(name string, sTag MetricMetaTag, canonical bool, err *error) {
	if name == "" {
		return
	}
	if !canonical && !ValidTagName(name) {
		if err != nil {
			*err = fmt.Errorf("invalid tag name %q", name)
		}
	}
	if _, ok := m.Name2Tag[name]; ok {
		if err != nil {
			*err = fmt.Errorf("name %q collision, tags must have unique alternative names and cannot have collisions with canonical (key*, skey, environment) names", name)
		}
		return
	}
	m.Name2Tag[name] = sTag
}

// Always restores maximum info, if error is returned, metric is non-canonical and should not be saved
func (m *MetricMetaValue) RestoreCachedInfo() error {
	var err error
	if !ValidMetricName(mem.S(m.Name)) {
		err = multierr.Append(err, fmt.Errorf("invalid metric name: %q", m.Name))
	}
	if !IsValidMetricType(m.MetricType) {
		err = multierr.Append(err, fmt.Errorf("invalid metric type: %s", m.MetricType))
		m.MetricType = ""
	}
	if !ValidMetricPrefix(m.Name) {
		err = multierr.Append(err, fmt.Errorf("invalid metric name (reserved prefix: %s)", strings.Join(reservedMetricPrefix, ", ")))
	}

	if m.Kind == legacyMetricKindStringTop {
		m.Kind = MetricKindCounter
		if m.StringTopName == "" && m.StringTopDescription == "" { // UI displayed string tag on condition of "kind string top or any of this two set"
			m.StringTopDescription = "string_top"
		}
	}
	if !ValidMetricKind(m.Kind) {
		err = multierr.Append(err, fmt.Errorf("invalid metric kind %q", m.Kind))
	}

	var mask uint32
	m.Name2Tag = map[string]MetricMetaTag{}

	if m.StringTopName == StringTopTagID { // remove redundancy
		m.StringTopName = ""
	}
	sTag := MetricMetaTag{
		Name:        m.StringTopName,
		Description: m.StringTopDescription,
		Index:       StringTopTagIndex,
	}
	m.setName2Tag(m.StringTopName, sTag, false, &err)
	if len(m.Tags) != 0 {
		// for mast metrics in database, this name is set to "env". We do not want to continue using it.
		// We want mapEnvironment() to work even when metric is not found, so we must not allow users to
		// set their environment tag name. They must use canonical "0" name instead.
		m.Tags[0].Name = ""
	}
	m.PreKeyIndex = -1
	tags := m.Tags
	if len(tags) > MaxTags { // prevent index out of range during mapping
		tags = tags[:MaxTags]
		err = multierr.Append(err, fmt.Errorf("too many tags, limit is: %d", MaxTags))
	}
	for i := range tags {
		var (
			tag   = &tags[i]
			tagID = TagID(i)
		)
		if tag.Name == tagID { // remove redundancy
			tag.Name = ""
		}
		if m.PreKeyFrom != 0 { // restore prekey index
			switch m.PreKeyTagID {
			case tagID:
				m.PreKeyIndex = i
			case TagIDLegacy(i):
				m.PreKeyIndex = i
				m.PreKeyTagID = tagID // fix legacy name
			}
		}
		if !ValidRawKind(tag.RawKind) {
			err = multierr.Append(err, fmt.Errorf("invalid raw kind %q of tag %d", tag.RawKind, i))
		}
	}
	if m.PreKeyIndex == -1 && m.PreKeyTagID != "" {
		err = multierr.Append(err, fmt.Errorf("invalid pre_key_tag_id: %q", m.PreKeyTagID))
	}
	if m.PreKeyOnly && m.PreKeyIndex == -1 {
		m.PreKeyOnly = false
		err = multierr.Append(err, fmt.Errorf("pre_key_only is true, but pre_key_tag_id is not defined"))
	}
	for i := range tags {
		tag := &tags[i]
		if tag.Raw {
			mask |= 1 << i
		}

		if len(tag.ID2Value) > 0 { // Legacy info, set for many metrics. Move to modern one.
			tag.ValueComments = convertToValueComments(tag.ID2Value)
			tag.ID2Value = nil
		}
		var c2v map[string]string
		if len(tag.ValueComments) > 0 {
			c2v = make(map[string]string, len(tag.ValueComments))
		}
		for v, c := range tag.ValueComments {
			c2v[c] = v
		}
		tag.Comment2Value = c2v
		tag.Index = i

		m.setName2Tag(tag.Name, *tag, false, &err)
	}
	m.setName2Tag(StringTopTagID, sTag, true, &err)
	hTag := MetricMetaTag{
		Name:        "",
		Description: "",
		Index:       HostTagIndex,
	}
	m.setName2Tag(HostTagID, hTag, true, &err)
	for i := 0; i < MaxTags; i++ { // separate pass to overwrite potential collisions with canonical names in the loop above
		if i < len(tags) {
			m.setName2Tag(TagID(i), tags[i], true, &err)
		} else {
			m.setName2Tag(TagID(i), MetricMetaTag{Index: i}, true, &err)
		}
	}
	m.RawTagMask = mask
	m.EffectiveResolution = AllowedResolution(m.Resolution)
	if m.EffectiveResolution != m.Resolution {
		err = multierr.Append(err, fmt.Errorf("resolution %d must be factor of 60", m.Resolution))
	}
	if math.IsNaN(m.Weight) || m.Weight < 0 || m.Weight > math.MaxInt32 {
		err = multierr.Append(err, fmt.Errorf("weight must be from %d to %d", 0, math.MaxInt32))
		m.EffectiveWeight = EffectiveWeightOne
	} else {
		m.EffectiveWeight = int64(m.Weight * EffectiveWeightOne)
		if m.EffectiveWeight < 1 {
			m.EffectiveWeight = 1
		}
		if m.EffectiveWeight > MaxEffectiveWeight {
			m.EffectiveWeight = MaxEffectiveWeight
		}
	}
	m.HasPercentiles = m.Kind == MetricKindValuePercentiles || m.Kind == MetricKindMixedPercentiles
	m.RoundSampleFactors = strings.Contains(m.Description, "__round_sample_factors") // Experimental
	m.ShardUniqueValues = strings.Contains(m.Description, "__shard_unique_values")   // Experimental
	m.WhalesOff = strings.Contains(m.Description, "__whales_off")                    // Experimental
	if m.Kind == MetricKindCounter {
		if i := strings.Index(m.Description, HistogramBucketsStartMark); i != -1 {
			s := m.Description[i+len(HistogramBucketsStartMark):]
			if i = strings.Index(s, HistogramBucketsEndMark); i != -1 {
				s = s[:i]
				m.HistorgamBuckets = make([]float32, 0, strings.Count(s, HistogramBucketsDelim)+1)
				for i, j := 0, 1; i < len(s); {
					for j < len(s) && s[j] != HistogramBucketsDelimC {
						j++
					}
					if f, err := strconv.ParseFloat(s[i:j], 32); err == nil {
						m.HistorgamBuckets = append(m.HistorgamBuckets, float32(f))
					}
					i = j + 1
					j = i + 1
				}
			}
		}
	}
	m.NoSampleAgent = builtinMetricsNoSamplingAgent[m.MetricID]
	if m.GroupID == 0 || m.GroupID == BuiltinGroupIDDefault {
		m.GroupID = BuiltinGroupIDDefault
		m.Group = BuiltInGroupDefault[BuiltinGroupIDDefault]
	}
	if m.NamespaceID == 0 || m.NamespaceID == BuiltinNamespaceIDDefault {
		m.NamespaceID = BuiltinNamespaceIDDefault
		m.Namespace = BuiltInNamespaceDefault[BuiltinNamespaceIDDefault]
	}
	// restore fair key index
	if len(m.FairKeyTagIDs) != 0 {
		m.FairKey = make([]int, 0, len(m.FairKeyTagIDs))
		for _, v := range m.FairKeyTagIDs {
			if tag, ok := m.Name2Tag[v]; ok {
				m.FairKey = append(m.FairKey, tag.Index)
			}
		}
	}
	// default strategy if it's not configured
	if len(m.Sharding) == 0 {
		m.Sharding = []MetricSharding{{Strategy: ShardBy16MappedTagsHash}}
	}
	for _, sh := range m.Sharding {
		if id, validationErr := ShardingStrategyId(sh); validationErr != nil {
			err = multierr.Append(err, validationErr)
			break
		} else {
			sh.StrategyId = id
		}
	}

	if slowHostMetricID[m.MetricID] {
		m.IsHardwareSlowMetric = true
	}

	return err
}

// 'APICompat' functions are expected to be used to handle user input, exists for backward compatibility
func (m *MetricMetaValue) APICompatGetTag(tagNameOrID string) (tag MetricMetaTag, ok bool, legacyName bool) {
	if res, ok := m.Name2Tag[tagNameOrID]; ok {
		return res, true, false
	}
	if tagID, ok := apiCompatTagID[tagNameOrID]; ok {
		tag, ok = m.Name2Tag[tagID]
		return tag, ok, true
	}
	return MetricMetaTag{}, false, false
}

// 'APICompat' functions are expected to be used to handle user input, exists for backward compatibility
func (m *MetricMetaValue) APICompatGetTagFromBytes(tagNameOrID []byte) (tag MetricMetaTag, ok bool, legacyName bool) {
	if res, ok := m.Name2Tag[string(tagNameOrID)]; ok {
		return res, true, false
	}
	if tagID, ok := apiCompatTagID[string(tagNameOrID)]; ok {
		tag, ok = m.Name2Tag[tagID]
		return tag, ok, true
	}
	return MetricMetaTag{}, false, false
}

func (m *MetricMetaValue) GetTagDraft(tagName []byte) (tag MetricMetaTag, ok bool) {
	if m.TagsDraft == nil {
		return MetricMetaTag{}, false
	}
	tag, ok = m.TagsDraft[string(tagName)]
	return tag, ok
}

// Always restores maximum info, if error is returned, group is non-canonical and should not be saved
func (m *MetricsGroup) RestoreCachedInfo(builtin bool) error {
	var err error
	if !builtin {
		if !ValidGroupName(m.Name) {
			err = fmt.Errorf("invalid group name: %q", m.Name)
		}
	}
	if math.IsNaN(m.Weight) || m.Weight < 0 || m.Weight > math.MaxInt32 {
		err = fmt.Errorf("weight must be from %d to %d", 0, math.MaxInt32)
	}
	m.EffectiveWeight = int64(m.Weight * EffectiveWeightOne)
	if m.EffectiveWeight < 1 {
		m.EffectiveWeight = 1
	}
	if m.EffectiveWeight > MaxEffectiveGroupWeight {
		m.EffectiveWeight = MaxEffectiveGroupWeight
	}
	if m.NamespaceID == 0 || m.NamespaceID == BuiltinNamespaceIDDefault {
		m.NamespaceID = BuiltinNamespaceIDDefault
		m.Namespace = BuiltInNamespaceDefault[BuiltinNamespaceIDDefault]
	}
	return err
}

// Always restores maximum info, if error is returned, group is non-canonical and should not be saved
func (m *NamespaceMeta) RestoreCachedInfo(builtin bool) error {
	var err error
	if !builtin {
		if !ValidGroupName(m.Name) {
			err = fmt.Errorf("invalid namespace name: %q", m.Name)
		}
	}
	if math.IsNaN(m.Weight) || m.Weight < 0 || m.Weight > math.MaxInt32 {
		err = fmt.Errorf("weight must be from %d to %d", 0, math.MaxInt32)
	}
	m.EffectiveWeight = int64(m.Weight * EffectiveWeightOne)
	if m.EffectiveWeight < 1 {
		m.EffectiveWeight = 1
	}
	if m.EffectiveWeight > MaxEffectiveNamespaceWeight {
		m.EffectiveWeight = MaxEffectiveNamespaceWeight
	}
	return err
}

func (m *MetricsGroup) MetricIn(metric *MetricMetaValue) bool {
	return !m.Disable && strings.HasPrefix(metric.Name, m.Name)
}

// '@' is reserved by api access.Do not use it here
func ValidMetricName(s mem.RO) bool {
	if s.Len() == 0 || s.Len() > MaxStringLen || !isLetter(s.At(0)) {
		return false
	}
	namespaceSepCount := 0
	for i := 1; i < s.Len(); i++ {
		c := s.At(i)
		if c == NamespaceSeparatorRune {
			if namespaceSepCount > 0 {
				return false
			}
			namespaceSepCount++
			continue
		}
		if !isLetter(c) && c != '_' && !(c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func ValidMetricPrefix(s string) bool {
	for _, p := range reservedMetricPrefix {
		if strings.HasPrefix(s, p) {
			return false
		}
	}
	return true
}

func ValidGroupName(s string) bool {
	return ValidMetricName(mem.S(s))
}

var validDashboardSymbols = map[uint8]bool{
	'_': true,
	'-': true,
	' ': true,
	'[': true,
	']': true,
	'{': true,
	'}': true,
	':': true,
	'.': true,
}

func ValidDashboardName(s string) bool {
	if len(s) == 0 || len(s) > MaxStringLen || s[0] == ' ' {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if !isLetter(c) && !(c >= '0' && c <= '9') && !validDashboardSymbols[c] {
			return false
		}
	}
	return true
}

func ValidTagName(s string) bool {
	return validIdent(mem.S(s))
}

func ValidMetricKind(kind string) bool {
	switch kind {
	case MetricKindCounter, MetricKindValue, MetricKindValuePercentiles,
		MetricKindUnique, MetricKindMixed, MetricKindMixedPercentiles:
		return true
	}
	return false
}

func ValidRawKind(s string) bool {
	// Do not change values, they are stored in DB
	// uint:            interpret number bits as uint32, print as decimal number
	// ip:              167901850 (0xA01FA9A) -> 10.1.250.154, interpret number bits as uint32, high byte contains first element of IP address, lower byte contains last element of IP address
	// ip_bswap:        same as ip, but do bswap after interpreting number bits as uint32
	// hex:             interpret number bits as uint32, print as hex number, do not omit leading 000
	// hex_bswap:       same as hex, but do bswap after interpreting number bits as uint32
	// timestamp:       UNIX timestamp, show as is (in GMT)
	// timestamp_local: UNIX timestamp, show local time for this TS
	// lexenc_float:    See @LexEncode - float encoding that preserves ordering
	// float:           same as float
	// EMPTY:           decimal number, can be negative
	switch s {
	case "", "uint", "ip", "ip_bswap", "hex", "hex_bswap", "timestamp", "timestamp_local", "lexenc_float", "float":
		return true
	}
	return false
}

func ShardingStrategyId(sharding MetricSharding) (int, error) {
	switch sharding.Strategy {
	case ShardBy16MappedTagsHash:
		if sharding.Shard.IsDefined() || sharding.TagId.IsDefined() {
			return -1, fmt.Errorf("%s strategy is incompative with shard or tag_id", sharding.Strategy)
		}
		return ShardBy16MappedTagsHashId, nil
	case ShardAggInternal:
		if sharding.Shard.IsDefined() || sharding.TagId.IsDefined() {
			return -1, fmt.Errorf("%s strategy is incompative with shard or tag_id", sharding.Strategy)
		}
		return ShardAggInternalId, nil
	case ShardByMetric:
		if sharding.Shard.IsDefined() || sharding.TagId.IsDefined() {
			return -1, fmt.Errorf("%s strategy is incompative with shard or tag_id", sharding.Strategy)
		}
		return ShardByMetricId, nil
	case ShardFixed:
		if !sharding.Shard.IsDefined() || sharding.TagId.IsDefined() {
			return -1, fmt.Errorf("%s strategy requires shard to be set", ShardFixed)
		}
		if sharding.TagId.IsDefined() {
			return -1, fmt.Errorf("%s strategy is incompative with tag_id", ShardFixed)
		}
		return ShardFixedId, nil
	case ShardByTag:
		if !sharding.TagId.IsDefined() {
			return -1, fmt.Errorf("%s strategy requires tag_id to be set", ShardByTag)
		}
		if sharding.Shard.IsDefined() {
			return -1, fmt.Errorf("%s strategy is incompative with shard", ShardByTag)
		}
		return ShardByTagId, nil
	}
	return -1, fmt.Errorf("unknown strategy %s", sharding.Strategy)
}

func TagIndex(tagID string) int { // inverse of 'TagID'
	i, ok := tagIDToIndex[tagID]
	if !ok {
		return -1
	}
	return i
}

func TagIDLegacy(i int) string {
	return tagIDsLegacy[i]
}

func TagID(i int) string {
	return tagIDs[i]
}

func AllowedResolution(r int) int {
	return allowedResolutionTable(r)
}

func allowedResolutionSwitch(r int) int {
	// 60 must be divisible by allowed resolution
	switch {
	case r <= 1: // fast path
		return 1
	case r <= 6:
		return r
	case r <= 10:
		return 10
	case r <= 12:
		return 12
	case r <= 15:
		return 15
	case r <= 20:
		return 20
	case r <= 30:
		return 30
	}
	return 60
}

const resolutionsTableStr = "\x01\x01\x02\x03\x04\x05\x06\x0a\x0a\x0a\x0a\x0c\x0c\x0f\x0f\x0f\x14\x14\x14\x14\x14\x1e\x1e\x1e\x1e\x1e\x1e\x1e\x1e\x1e\x1e\x3c"

func allowedResolutionTable(r int) int {
	if r < 0 {
		return 1
	}
	if r >= len(resolutionsTableStr) {
		return 60
	}
	// 60 must be divisible by allowed resolution
	return int(resolutionsTableStr[r])
}

func isLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func validIdent(s mem.RO) bool {
	if s.Len() == 0 || s.Len() > MaxStringLen || !isLetter(s.At(0)) {
		return false
	}
	for i := 1; i < s.Len(); i++ {
		c := s.At(i)
		if !isLetter(c) && c != '_' && !(c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func ValidFloatValue(f float64) bool {
	return !math.IsNaN(f) && !math.IsInf(f, 0)
}

func ClampFloatValue(f float64) float64 {
	if f > math.MaxFloat32 {
		return math.MaxFloat32
	}
	if f < -math.MaxFloat32 {
		return -math.MaxFloat32
	}
	return f
}

func ClampCounter(f float64) (_ float64, errorTag int32) {
	if !ValidFloatValue(f) {
		return f, TagValueIDSrcIngestionStatusErrNanInfCounter
	}
	if f < 0 {
		return f, TagValueIDSrcIngestionStatusErrNegativeCounter
	}
	if f > math.MaxFloat32 {
		return math.MaxFloat32, 0
	}
	return f, 0
}

func ClampValue(f float64) (_ float64, errorTag int32) {
	if !ValidFloatValue(f) {
		return f, TagValueIDSrcIngestionStatusErrNanInfValue
	}
	return ClampFloatValue(f), 0
}

// Legacy rules replaced non-printables including whitespaces (except ASCII space) into roadsigns
// This was found to be not ideal set of rules, so they were changed
func ValidStringValueLegacy(s mem.RO) bool {
	return validStringValueLegacy(s, MaxStringLen)
}

// Motivation - we want our tag values be 'good' and displayable in UI, copyable to edit boxes, etc.
// Our rules:
// tag must be UTF-8 string
// tag must not be longer than MaxStringLen
// tag contains no whitespaces to the left or to the right (trimmed)
// all whitespaces inside are ASCII spaces, and there must not be more than 1 consecutive space.
//
// Often, scripts insert tags with extra spaces, new line, some trash, etc.
// We help people by replacing slightly invalid tag values with valid tag values.
// 1. trim unicode whitespaces to the left and right
// 2. replace all consecutive unicode whitespaces inside with single ASCII space
// 3. trim to MaxStringLen (if las utf symbol fits only partly, we remove it completely)
// 4. non-printable characters are replaced by roadsign (invalid rune)
// but invalid UTF-8 is still error
//
// TODO - replace invalid UTF-8 with roadsigns as well to simplify rules
func ValidStringValue(s mem.RO) bool {
	return validStringValue(s, MaxStringLen)
}

func validStringValueLegacy(s mem.RO, maxLen int) bool {
	if s.Len() > maxLen {
		return false
	}
	if mem.TrimSpace(s).Len() != s.Len() {
		return false
	}
	for i, w := 0, 0; i < s.Len(); i += w {
		r, size := mem.DecodeRune(s.SliceFrom(i))
		if (r == utf8.RuneError && size <= 1) || !unicode.IsPrint(r) {
			return false
		}
		w = size
	}
	return true
}

// TODO - this can likely be speed up with ASCII fast-path
func validStringValue(s mem.RO, maxLen int) bool {
	if s.Len() > maxLen {
		return false
	}
	if mem.TrimSpace(s).Len() != s.Len() {
		return false
	}
	previousSpace := false
	for r := 0; r < s.Len(); {
		c, nr := mem.DecodeRune(s.SliceFrom(r))
		if c == utf8.RuneError && nr <= 1 {
			return false
		}
		switch {
		case unicode.IsSpace(c):
			if previousSpace {
				return false
			}
			previousSpace = true
		case !unicode.IsPrint(c):
			return false
		default:
			previousSpace = false
		}
		r += nr
	}
	return true
}

// We have a lot of 1251 encoding still, also we have java people saving strings in default UTF-16 encoding,
// so we consider rejecting large percentage of such strings is good.
// Let's consider using ForceValidStringValue everywhere after last component using 1251 encoding is removed.
// Do not forget to eliminate difference in speed between functions then.
func AppendValidStringValue(dst []byte, src []byte) ([]byte, error) {
	return appendValidStringValue(dst, src, MaxStringLen, false)
}

// We use this function only to validate host names and args for now.
func ForceValidStringValue(src string) []byte {
	dst, _ := appendValidStringValue(nil, []byte(src), MaxStringLen, true)
	return dst
}

func appendValidStringValue(dst []byte, src []byte, maxLen int, force bool) ([]byte, error) {
	// trim
	src = bytes.TrimSpace(src) // packet is limited, so this will not be too slow
	// fast path
	if len(src) <= maxLen { // calculating dst size slows us down when dealing with double spaces
		fastPath := true
		previousSpace := false
		for _, c := range src {
			if !bytePrint(c) {
				fastPath = false
				break
			}
			doubleSpace := c == ' ' && previousSpace
			previousSpace = c == ' '
			if doubleSpace {
				fastPath = false // they are rare, and we do not want to slow fast path down dealing with them
				break
			}
		}
		if fastPath {
			return append(dst, src...), nil
		}
	}
	// slow path
	var buf [MaxStringLen + utf8.UTFMax]byte
	w := 0
	previousSpace := false
	for r := 0; r < len(src); {
		c, nr := utf8.DecodeRune(src[r:])
		if c == utf8.RuneError && nr <= 1 && !force {
			return dst, errBadEncoding
		}
		switch {
		case unicode.IsSpace(c):
			if previousSpace {
				r += nr
				continue
			}
			c = ' '
			previousSpace = true
		case !unicode.IsPrint(c):
			c = utf8.RuneError
			previousSpace = false
		default:
			previousSpace = false
		}
		nw := utf8.EncodeRune(buf[w:], c)
		if w+nw > maxLen {
			break
		}
		r += nr
		w += nw
	}
	dst = append(dst, buf[:w]...)
	return bytes.TrimRightFunc(dst, unicode.IsSpace), nil
}

func AppendHexStringValue(dst []byte, src []byte) []byte {
	src = bytes.TrimSpace(src) // packet is limited, so this will not be too slow
	src = bytes.TrimRightFunc(src, unicode.IsSpace)
	if len(src) > MaxStringLen/2 {
		src = src[:MaxStringLen/2]
	}
	const hextable = "0123456789abcdef"
	var buf [MaxStringLen]byte
	for i, c := range src {
		buf[i*2] = hextable[c>>4]
		buf[i*2+1] = hextable[c&0x0f]
	}
	return append(dst, buf[:len(src)*2]...)
}

// fast-path inlineable version of unicode.IsPrint for single-byte UTF-8 runes
func bytePrint(c byte) bool {
	return c >= 0x20 && c <= 0x7e
}

func CodeTagValue(code int32) string {
	if code == 0 {
		return TagValueCodeZero // fast-path with no allocations
	}
	return tagValueCodePrefix + strconv.Itoa(int(code))
}

func ParseCodeTagValue(s string) (int32, error) {
	if !strings.HasPrefix(s, tagValueCodePrefix) {
		return 0, errInvalidCodeTagValue
	}
	i, err := strconv.Atoi(s[len(tagValueCodePrefix):])
	if err != nil {
		return 0, err
	}
	if i < math.MinInt32 || i > math.MaxInt32 {
		return 0, errInvalidCodeTagValue
	}
	return int32(i), nil
}

func ValidTagValueForAPI(s string) bool {
	if ValidStringValueLegacy(mem.S(s)) { // TODO - change to new rules after all agent and aggregators are running again
		return true
	}
	_, err := ParseCodeTagValue(s)
	return err == nil
}

// We allow both signed and unsigned 32-bit values, however values outside both ranges are prohibited
func ContainsRawTagValue(s mem.RO) (int32, bool) {
	if s.Len() == 0 {
		return 0, true // make sure empty string is the same as value not set, even for raw values
	}
	i, err := mem.ParseInt(s, 10, 64)
	return int32(i), err == nil && i >= math.MinInt32 && i <= math.MaxUint32
}

// Limit build arch for built in-metrics collected by source
// We do not sample built-in metrics, hence should protect from cardinality explosion
func FilterBuildArch(buildArch int32) int32 {
	if _, ok := buildArchToValue[buildArch]; ok {
		return buildArch
	}
	return 0
}

// Not optimal, called on startup only
func GetBuildArchKey(arch string) int32 {
	for k, v := range buildArchToValue {
		if v == arch {
			return k
		}
	}
	return 0
}

func HasRawValuePrefix(s string) bool {
	return strings.HasPrefix(s, tagValueCodePrefix)
}

func AddRawValuePrefix(s string) string {
	return tagValueCodePrefix + s
}

// 'APICompat' functions are expected to be used to handle user input, exists for backward compatibility
func APICompatNormalizeTagID(tagID string) (string, error) {
	res, ok := apiCompatTagID[tagID]
	if ok {
		return res, nil
	}
	return "", fmt.Errorf("invalid tag ID %q", tagID)
}

// 'APICompat' functions are expected to be used to handle user input, exists for backward compatibility
func APICompatIsEnvTagID(tagID []byte) bool {
	switch string(tagID) {
	case EnvTagID, "key0", "env":
		return true
	default:
		return false
	}
}

func convertToValueComments(id2value map[int32]string) map[string]string {
	vc := make(map[string]string, len(id2value))
	for v, c := range id2value {
		vc[CodeTagValue(v)] = c
	}

	return vc
}

func ISO8601Date2BuildDateKey(str string) int32 {
	// layout is "2006-01-02T15:04:05-0700", but we do not bother parsing as date
	// we want 20060102 value from that string
	if len(str) < 11 || str[4] != '-' || str[7] != '-' || str[10] != 'T' {
		return 0
	}
	str = strings.ReplaceAll(str[:10], "-", "")
	n, err := strconv.ParseUint(str, 10, 32)
	if err != nil {
		return 0
	}
	return int32(n) // Will always fit
}

func IsValidMetricType(typ_ string) bool {
	switch typ_ {
	case MetricSecond, MetricMillisecond, MetricMicrosecond, MetricNanosecond, MetricByte, MetricByteAsBits, "":
		return true
	}
	return false
}

func NamespaceName(namespace string, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + NamespaceSeparator + name
}

func SplitNamespace(metricName string) (string, string) {
	ix := strings.Index(metricName, NamespaceSeparator)
	if ix == -1 {
		return metricName, ""
	}
	return metricName[ix+1:], metricName[:ix]
}

func EventTypeToName(typ int32) string {
	switch typ {
	case MetricEvent:
		return "metric"
	case DashboardEvent:
		return "dashboard"
	case MetricsGroupEvent:
		return "group"
	case PromConfigEvent:
		return "prom-config"
	case NamespaceEvent:
		return "namespace"
	default:
		return "unknown"
	}
}

func ReportAPIPanic(r any) {
	log.Println("panic:", string(debug.Stack()))
	statshouse.StringTop(BuiltinMetricNameStatsHouseErrors, statshouse.Tags{1: strconv.FormatInt(TagValueIDAPIPanicError, 10)}, fmt.Sprintf("%v", r))
}

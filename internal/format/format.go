// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package format

//go:generate easyjson -no_std_marshalers format.go

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/mailru/easyjson"
	"go.uber.org/multierr"
	"go4.org/mem"
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
	TagValueIDDoesNotExist = -2

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
	ToggleDescriptionMark     = "statshouse$" // all experimental toggles should have this mark
	HistogramBucketsStartMark = "Buckets$"
	HistogramBucketsDelim     = ","
	HistogramBucketsDelimC    = ','
	HistogramBucketsEndMark   = "$"
	HistogramBucketsEndMarkC  = '$'

	StringTopTagIndex   = -1 // used as flag during mapping
	StringTopTagIndexV3 = 47
	HostTagIndex        = -2 // used as flag during mapping
	ShardTagIndex       = -3

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

	// agents with older commitTs will be declined
	LeastAllowedAgentCommitTs uint32 = 0

	StatshouseAgentRemoteConfigMetric      = "statshouse_agent_remote_config"
	StatshouseJournalDump                  = "statshouse_journal_dump" // journals will be dumped to disk for analysis at each point before this event
	StatshouseAggregatorRemoteConfigMetric = "statshouse_aggregator_remote_config"
	StatshouseAPIRemoteConfig              = "statshouse_api_remote_config"
)

// Do not change values, they are stored in DB
const (
	MetricKindCounter          = "counter"
	MetricKindValue            = "value"
	MetricKindValuePercentiles = "value_p"
	MetricKindUnique           = "unique"
	MetricKindMixed            = "mixed" // empty string is also considered mixed kind
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
	LegacyStringTopTagID = "skey"
	legacyTagIDPrefix    = "key"
)

var (
	tagIDs          []string             // initialized in builtin.go due to dependency
	defaultMetaTags []MetricMetaTag      // most tags use no custom meta info, so point to this array, saving a lot of memory
	defaultSTag     MetricMetaTag        // most S tags use no custom meta info, so point to this value
	defaultHTag     MetricMetaTag        // host tags use no custom meta info, so point to this value
	tagIDTag2TagID  = map[int32]string{} // initialized in builtin.go due to dependency
	tagIDToIndex    = map[string]int{}   // initialized in builtin.go due to dependency

	errInvalidCodeTagValue = fmt.Errorf("invalid code tag value") // must be fast
	errBadEncoding         = fmt.Errorf("bad utf-8 encoding")     // must be fast
	reservedMetricPrefix   = []string{"host_"}
)

// Legacy, left for API backward compatibility
var (
	tagIDsLegacy   []string              // initialized in builtin.go due to dependency
	apiCompatTagID = map[string]string{} // initialized in builtin.go due to dependency
)

type AgentEnvRouteArch struct {
	AgentEnv  int32
	Route     int32
	BuildArch int32
}

type MetricKind int

type MetaStorageInterface interface { // agent uses this to avoid circular dependencies
	GetMetaMetric(metricID int32) *MetricMetaValue
	GetMetaMetricByName(metricName string) *MetricMetaValue
	GetGroup(id int32) *MetricsGroup
	GetNamespace(id int32) *NamespaceMeta
	GetNamespaceByName(name string) *NamespaceMeta
	GetGroupByName(name string) *MetricsGroup
}

// This struct is immutable, it is accessed by mapping code without any locking
// We want it compact, so reorder fields and use int32 index
type MetricMetaTag struct {
	Name          string            `json:"name,omitempty"`
	Description   string            `json:"description,omitempty"`
	RawKind       string            `json:"raw_kind,omitempty"` // UI can show some raw values beautifully - timestamps, hex values, etc.
	ValueComments map[string]string `json:"value_comments,omitempty"`

	Comment2Value map[string]string `json:"-"`             // Should be restored from ValueComments after reading
	Index         int32             `json:"-"`             // Should be restored from position in MetricMetaValue.Tags
	BuiltinKind   uint8             `json:"-"`             // Only for built-in metrics so never saved or parsed
	Raw           bool              `json:"raw,omitempty"` // Depends on RawKind, serialized for old agents only
	raw64         bool
}

func (t *MetricMetaTag) IsMetric() bool    { return t.BuiltinKind == BuiltinKindMetric }
func (t *MetricMetaTag) IsGroup() bool     { return t.BuiltinKind == BuiltinKindGroup }
func (t *MetricMetaTag) IsNamespace() bool { return t.BuiltinKind == BuiltinKindNamespace }
func (t *MetricMetaTag) Raw64() bool       { return t.raw64 }

const (
	MetricEvent       int32 = 0
	DashboardEvent    int32 = 1
	MetricsGroupEvent int32 = 2
	PromConfigEvent   int32 = 3
	NamespaceEvent    int32 = 4

	BuiltinKindMetric    = 1
	BuiltinKindGroup     = 2
	BuiltinKindNamespace = 3
)

// TODO - omitempty everywhere

//easyjson:json
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
//
//easyjson:json
type MetricsGroup struct {
	ID          int32  `json:"group_id"`
	NamespaceID int32  `json:"namespace_id"`
	Name        string `json:"name"`
	Version     int64  `json:"version"`
	UpdateTime  uint32 `json:"update_time"`

	Weight  float64 `json:"weight,omitempty"`
	Disable bool    `json:"disable,omitempty"`

	EffectiveWeight int64 `json:"-"`
}

// possible sharding strategies
const (
	ShardByTagsHash = "tags_hash"
	ShardFixed      = "fixed_shard"
	ShardByMetric   = "metric_id" // shard = metric_id % num_shards
)

// This struct is immutable, it is accessed by mapping code without any locking
//
//easyjson:json
type MetricMetaValue struct {
	MetricID    int32  `json:"metric_id,omitempty"`
	NamespaceID int32  `json:"namespace_id,omitempty"` // RO
	Name        string `json:"name,omitempty"`
	Version     int64  `json:"version,omitempty"`
	UpdateTime  uint32 `json:"-"` // updated from event

	Description          string                   `json:"description,omitempty"`
	Tags                 []MetricMetaTag          `json:"tags,omitempty"`
	TagsDraft            map[string]MetricMetaTag `json:"tags_draft,omitempty"`
	Visible              bool                     `json:"visible,omitempty"`
	Disable              bool                     `json:"disable,omitempty"` // TODO - we migrate from visible flag to this flag
	Kind                 string                   `json:"kind,omitempty"`
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
	MetricType           string                   `json:"metric_type,omitempty"`
	FairKeyTagIDs        []string                 `json:"fair_key_tag_ids,omitempty"`
	ShardStrategy        string                   `json:"shard_strategy,omitempty"`
	ShardNum             uint32                   `json:"shard_num,omitempty"`
	PipelineVersion      uint8                    `json:"pipeline_version,omitempty"`

	name2Tag             map[string]*MetricMetaTag // Should be restored from Tags after reading
	EffectiveResolution  int                       `json:"-"` // Should be restored from Tags after reading
	PreKeyIndex          int                       `json:"-"` // index of tag which goes to 'prekey' column, or <0 if no tag goes
	FairKey              []int                     `json:"-"`
	EffectiveWeight      int64                     `json:"-"`
	HasPercentiles       bool                      `json:"-"`
	RoundSampleFactors   bool                      `json:"-"` // Experimental, set if magic word in description is found
	WhalesOff            bool                      `json:"-"` // "whales" sampling algorithm disabled
	HistogramBuckets     []float32                 `json:"-"` // Prometheus histogram buckets
	IsHardwareSlowMetric bool                      `json:"-"`
	GroupID              int32                     `json:"-"`

	NoSampleAgent           bool `json:"-"` // Built-in metrics with fixed/limited # of rows on agent. Set only in constant initialization of builtin metrics
	BuiltinAllowedToReceive bool `json:"-"` // we allow only small subset of built-in metrics through agent receiver.
	WithAgentEnvRouteArch   bool `json:"-"` // set for some built-in metrics to add common set of tags
	WithAggregatorID        bool `json:"-"` // set for some built-in metrics to add common set of tags
}

// TODO - better place?
type CreateMappingExtra struct {
	Create    bool
	Metric    string // set by old conveyor, TODO - remove?
	MetricID  int32  // set by new conveyor
	TagIDKey  int32
	ClientEnv int32
	Aera      AgentEnvRouteArch
	HostName  string
	Host      int32
}

func (m MetricMetaValue) MarshalBinary() ([]byte, error) { return easyjson.Marshal(m) }
func (m *MetricMetaValue) UnmarshalBinary(data []byte) error {
	if err := easyjson.Unmarshal(data, m); err != nil {
		return err
	}
	_ = m.RestoreCachedInfo() // name collisions must not prevent journal sync
	return nil
}

func (m DashboardMeta) MarshalBinary() ([]byte, error) { return json.Marshal(m) }
func (m *DashboardMeta) UnmarshalBinary(data []byte) error {
	if err := json.Unmarshal(data, m); err != nil {
		return err
	}
	return nil
}

func (m *MetricMetaValue) WithGroupID(groupID int32) *MetricMetaValue {
	if m.GroupID == groupID {
		return m
	}
	c := *m
	c.GroupID = groupID
	return &c
}

// this method is faster than string hash, plus saves a lot of memory in maps
func (m *MetricMetaValue) name2TagFast(name string) *MetricMetaTag {
	var num uint
	switch len(name) {
	case 0:
		return nil
	case 1:
		num = uint(name[0]) - '0'
		if num > 9 {
			return nil
		}
	case 2:
		num = uint(name[0]) - '0'
		if num > 9 {
			return nil
		}
		num2 := uint(name[1]) - '0'
		if num2 > 9 {
			return nil
		}
		num = num*10 + num2
	default:
		return nil
	}
	if num < uint(len(m.Tags)) {
		return &m.Tags[num]
	}
	if num < uint(len(defaultMetaTags)) {
		return &defaultMetaTags[num]
	}
	return nil
}

// this method is faster than string hash, plus saves a lot of memory in maps
func (m *MetricMetaValue) name2TagFastBytes(name []byte) *MetricMetaTag {
	var num uint
	switch len(name) {
	case 0:
		return nil
	case 1:
		num = uint(name[0]) - '0'
		if num > 9 {
			return nil
		}
	case 2:
		num = uint(name[0]) - '0'
		if num > 9 {
			return nil
		}
		num2 := uint(name[1]) - '0'
		if num2 > 9 {
			return nil
		}
		num = num*10 + num2
	default:
		return nil
	}
	if num < uint(len(m.Tags)) {
		return &m.Tags[num]
	}
	if num < uint(len(defaultMetaTags)) {
		return &defaultMetaTags[num]
	}
	return nil
}

func (m *MetricMetaValue) Name2Tag(name string) *MetricMetaTag {
	if tag := m.name2TagFast(name); tag != nil {
		return tag
	}
	return m.name2Tag[name]
}

func (m *MetricMetaValue) Name2TagBytes(name []byte) *MetricMetaTag {
	if tag := m.name2TagFastBytes(name); tag != nil {
		return tag
	}
	return m.name2Tag[string(name)]
}

func (m *MetricMetaValue) AppendTagNames(res []string) []string {
	for name := range m.name2Tag {
		res = append(res, name)
	}
	res = append(res, tagIDs[:MaxTags]...)
	return res
}

// updates error if name collision happens
func (m *MetricMetaValue) setName2Tag(name string, newTag *MetricMetaTag, canonical bool, err *error) {
	if !canonical && !ValidTagName(mem.S(name)) {
		if err != nil {
			*err = fmt.Errorf("invalid tag name %q", name)
		}
	}
	if tag := m.Name2Tag(name); tag != nil {
		if err != nil {
			*err = fmt.Errorf("name %q collision, tags must have unique alternative names and cannot have collisions with canonical (0, 1 .. , _s, _h) names", name)
		}
		return
	}
	m.name2Tag[name] = newTag
}

// while we have v2 agents, we must serialize Raw, Visible fields
// so we must make sure they are always correctly set
// after we have no more v2 agents, TODO - remove this function
func (m *MetricMetaValue) BeforeSavingCheck() error {
	var err error
	if !ValidMetricKind(m.Kind) { // We have relaxed check in RestoreCachedInfo
		err = multierr.Append(err, fmt.Errorf("invalid metric kind %q", m.Kind))
	}
	for i, tag := range m.Tags {
		if (!tag.Raw && tag.RawKind != "") || (tag.Raw && tag.RawKind == "") {
			err = multierr.Append(err, fmt.Errorf("mismatch of raw kind %q and Raw %v of tag %d", tag.RawKind, tag.Raw, i))
		}
	}
	return err
}

// Always restores maximum info, if error is returned, metric is non-canonical and should not be saved
func (m *MetricMetaValue) RestoreCachedInfo() error {
	var err error
	if !ValidMetricName(mem.S(m.Name)) && m.Name != "404_page" && m.Name != "404_page_top_urls" { // TODO - ask monolith team to update names in code before removing exceptions
		err = multierr.Append(err, fmt.Errorf("invalid metric name: %q", m.Name))
	}
	if !IsValidMetricType(m.MetricType) {
		err = multierr.Append(err, fmt.Errorf("invalid metric type: %s", m.MetricType))
		m.MetricType = ""
	}
	if !ValidMetricPrefix(m.Name) {
		err = multierr.Append(err, fmt.Errorf("invalid metric name (one of reserved prefixes: %s)", strings.Join(reservedMetricPrefix, ", ")))
	}

	if !ValidMetricKind(m.Kind) && m.Kind != "" { // Kind is empty in compact form
		err = multierr.Append(err, fmt.Errorf("invalid metric kind %q", m.Kind))
	}
	m.Visible = !m.Disable // Visible is serialized for v2 agents only

	m.name2Tag = map[string]*MetricMetaTag{}

	if m.StringTopName == StringTopTagID { // remove redundancy
		m.StringTopName = ""
	}
	if len(m.Tags) != 0 {
		// for mast metrics in database, this name is set to "env". We do not want to continue using it.
		// We want mapEnvironment() to work even when metric is not found, so we must not allow users to
		// set their environment tag name. They must use canonical "0" name instead.
		m.Tags[0].Name = ""
		m.Tags[0].Raw = false // TODO - remove after it removing v2 agents
		m.Tags[0].RawKind = ""
		m.Tags[0].ValueComments = nil
		if m.Tags[0].Description != "-" { // most builtin metrics have no environment, we want to remove it from UI
			m.Tags[0].Description = "environment"
		}
	}
	m.PreKeyIndex = -1
	tags := m.Tags
	if len(tags) > NewMaxTags { // prevent various overflows in code
		tags = tags[:NewMaxTags]
		err = multierr.Append(err, fmt.Errorf("too many tags, limit is: %d", NewMaxTags))
	}
	for name, tag := range m.TagsDraft {
		// in compact journal we clear tag.Name, so we must restore
		// also in case of difference, we want key to take precedence
		tag.Name = name
		m.TagsDraft[name] = tag
	}
	for i := range tags {
		tag := &tags[i]
		tagID := TagID(i)
		if tag.Name == tagID { // remove redundancy
			tag.Name = ""
		}
		if tag.Name != "" && !ValidTagName(mem.S(tag.Name)) {
			err = multierr.Append(err, fmt.Errorf("invalid tag name %q of tag %d", tag.Name, i))
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
		tag.Raw = tag.RawKind != "" // Raw is serialized for v2 agents only
		tag.raw64 = IsRaw64Kind(tag.RawKind)
		if tag.raw64 && tag.Index >= NewMaxTags-1 { // for now, to avoid overflows in v2 and v3 mapping
			err = multierr.Append(err, fmt.Errorf("last tag cannot be raw64 kind %q of tag %d", tag.RawKind, i))
			tag.raw64 = false
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

		var c2v map[string]string
		if len(tag.ValueComments) > 0 {
			c2v = make(map[string]string, len(tag.ValueComments))
		}
		for v, c := range tag.ValueComments {
			c2v[c] = v
		}
		tag.Comment2Value = c2v
		tag.Index = int32(i)

		if tag.Name != "" {
			m.setName2Tag(tag.Name, tag, false, &err) // not set if equalsToDefault
		}
	}
	if m.StringTopName == "" && m.StringTopDescription == "" {
		m.setName2Tag(StringTopTagID, &defaultSTag, true, &err)
	} else {
		sTag := &MetricMetaTag{
			Name:        m.StringTopName,
			Description: m.StringTopDescription,
			Index:       StringTopTagIndex,
		}
		if m.StringTopName != "" {
			m.setName2Tag(m.StringTopName, sTag, false, &err) // not set if equalsToDefault
		}
		m.setName2Tag(StringTopTagID, sTag, true, &err)
	}
	m.setName2Tag(HostTagID, &defaultHTag, true, &err)
	m.EffectiveResolution = AllowedResolution(m.Resolution)
	if m.EffectiveResolution != m.Resolution && m.Resolution != 0 { // Resolution 0 is good default for JSON
		err = multierr.Append(err, fmt.Errorf("resolution %d must be factor of 60", m.Resolution))
	}
	if m.Weight == 0 { // Weight 0 is good default for JSON
		m.Weight = 1
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
	m.RoundSampleFactors = strings.Contains(m.Description, "__round_sample_factors") || // TODO - deprecate
		strings.Contains(m.Description, ToggleDescriptionMark+"round_sample_factors")
	m.WhalesOff = strings.Contains(m.Description, "__whales_off") || // TODO - deprecate
		strings.Contains(m.Description, ToggleDescriptionMark+"whales_off")
	if ind := strings.Index(m.Description, HistogramBucketsStartMark); ind != -1 {
		s := m.Description[ind+len(HistogramBucketsStartMark):]
		if ind = strings.Index(s, HistogramBucketsEndMark); ind != -1 {
			s = s[:ind]
			m.HistogramBuckets = make([]float32, 0, strings.Count(s, HistogramBucketsDelim)+1)
			for i, j := 0, 1; i < len(s); {
				for j < len(s) && s[j] != HistogramBucketsDelimC {
					j++
				}
				if f, err := strconv.ParseFloat(s[i:j], 32); err == nil {
					m.HistogramBuckets = append(m.HistogramBuckets, float32(f))
				}
				i = j + 1
				j = i + 1
			}
		}
	}
	// m.NoSampleAgent we never set it here, it is set in code for some built-in metrics
	if m.GroupID == 0 {
		m.GroupID = BuiltinGroupIDDefault
	}
	if m.NamespaceID == 0 {
		m.NamespaceID = BuiltinNamespaceIDDefault
	}
	// restore fair key index
	if len(m.FairKeyTagIDs) != 0 {
		m.FairKey = make([]int, 0, len(m.FairKeyTagIDs))
		for _, v := range m.FairKeyTagIDs {
			if tag := m.Name2Tag(v); tag != nil {
				m.FairKey = append(m.FairKey, int(tag.Index))
			}
		}
	}
	if m.PipelineVersion != 3 { // PipelineVersion 0 is good default for JSON
		m.PipelineVersion = 0
	}

	if slowHostMetricID[m.MetricID] {
		m.IsHardwareSlowMetric = true
	}

	return err
}

// 'APICompat' functions are expected to be used to handle user input, exists for backward compatibility
func (m *MetricMetaValue) APICompatGetTag(tagNameOrID string) (tag *MetricMetaTag, legacyName bool) {
	if res := m.Name2Tag(tagNameOrID); res != nil {
		return res, false
	}
	if tagID, ok := apiCompatTagID[tagNameOrID]; ok {
		return m.Name2Tag(tagID), true
	}
	return nil, false
}

// 'APICompat' functions are expected to be used to handle user input, exists for backward compatibility
func (m *MetricMetaValue) APICompatGetTagFromBytes(tagNameOrID []byte) (tag *MetricMetaTag, legacyName bool) {
	if res := m.Name2TagBytes(tagNameOrID); res != nil {
		return res, false
	}
	if tagID, ok := apiCompatTagID[string(tagNameOrID)]; ok {
		return m.Name2Tag(tagID), true
	}
	return nil, false
}

func (m *MetricMetaValue) GetTagDraft(tagName []byte) (tag MetricMetaTag, ok bool) {
	if m.TagsDraft == nil {
		return MetricMetaTag{}, false
	}
	tag, ok = m.TagsDraft[string(tagName)]
	return tag, ok
}

func (m *MetricMetaValue) GroupBy(groupBy []string) (res []int) {
	for _, name := range groupBy {
		if t, _ := m.APICompatGetTag(name); t != nil {
			x := t.Index
			if x == StringTopTagIndex {
				x = StringTopTagIndexV3
			}
			res = append(res, int(x))
		}
	}
	return res
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
	if s.Len() == 0 || s.Len() > MaxStringLen || !isAsciiLetter(s.At(0)) {
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
		if !isAsciiLetter(c) && c != '_' && !(c >= '0' && c <= '9') {
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
		if !isAsciiLetter(c) && !(c >= '0' && c <= '9') && !validDashboardSymbols[c] {
			return false
		}
	}
	return true
}

func ValidTagName(s mem.RO) bool {
	return validIdent(s)
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
	// EMPTY:           decimal number, can be negative
	switch s {
	case "", "int", "uint", "ip", "ip_bswap", "hex", "hex_bswap", "timestamp", "timestamp_local", "lexenc_float", "int64", "uint64":
		return true
	}
	return false
}

func IsRaw64Kind(s string) bool {
	switch s {
	case "int64", "uint64":
		return true
	}
	return false
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

func isAsciiLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func validIdent(s mem.RO) bool {
	if s.Len() == 0 || s.Len() > MaxStringLen || !isAsciiLetter(s.At(0)) {
		return false
	}
	for i := 1; i < s.Len(); i++ {
		c := s.At(i)
		if !isAsciiLetter(c) && c != '_' && !(c >= '0' && c <= '9') {
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
		return "" // fast-path with no allocations
	}
	return tagValueCodePrefix + strconv.Itoa(int(code))
}

func CodeTagValue64(code int64) string {
	if code == 0 {
		return "" // fast-path with no allocations
	}
	return tagValueCodePrefix + strconv.FormatInt(code, 10)
}

func ParseCodeTagValue(s string) (int64, error) {
	if !strings.HasPrefix(s, tagValueCodePrefix) {
		return 0, errInvalidCodeTagValue
	}
	i, err := strconv.ParseInt(s[len(tagValueCodePrefix):], 10, 64)
	if err != nil {
		return 0, err
	}
	return int64(i), nil
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
	i, err := mem.ParseInt(s, 10, 64) // TODO - remove allocation in case of error
	return int32(i), err == nil && i >= math.MinInt32 && i <= math.MaxUint32
}

func ContainsRawTagValue64(s mem.RO) (lo int32, hi int32, ok bool) {
	if s.Len() == 0 { // never happens, but seems rather cheap check
		return 0, 0, false
	}
	if s.At(0) == '-' { // save allocation of error on half input space
		i, err := mem.ParseInt(s, 10, 64) // TODO - remove allocation in case of error
		lo = int32(i)
		hi = int32(i >> 32)
		return lo, hi, err == nil
	}
	i, err := mem.ParseUint(s, 10, 64) // TODO - remove allocation in case of error
	lo = int32(uint32(i))
	hi = int32(uint32(i >> 32))
	return lo, hi, err == nil
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

func convertToValueComments(id2value map[int32]string) map[string]string {
	vc := make(map[string]string, len(id2value))
	for v, c := range id2value {
		vc[CodeTagValue(v)] = c
	}

	return vc
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

func RemoteConfigMetric(name string) bool {
	switch name {
	case StatshouseAgentRemoteConfigMetric, StatshouseJournalDump, StatshouseAggregatorRemoteConfigMetric, StatshouseAPIRemoteConfig:
		return true
	default:
		return false
	}
}

func SameCompactTag(ta, tb *MetricMetaTag) bool {
	return ta.Index == tb.Index && ta.Name == tb.Name &&
		ta.Raw == tb.Raw && ta.Raw64() == tb.Raw64()
}

func SameCompactMetric(a, b *MetricMetaValue) bool {
	if a.MetricID != b.MetricID ||
		a.Name != b.Name ||
		a.NamespaceID != b.NamespaceID ||
		a.GroupID != b.GroupID ||
		a.Disable != b.Disable ||
		a.Visible != b.Visible || // we have legacy agents checking Visible flag, so must not change
		a.EffectiveWeight != b.EffectiveWeight ||
		a.EffectiveResolution != b.EffectiveResolution ||
		!slices.Equal(a.FairKey, b.FairKey) ||
		a.ShardStrategy != b.ShardStrategy ||
		a.ShardNum != b.ShardNum ||
		a.PipelineVersion != b.PipelineVersion ||
		a.HasPercentiles != b.HasPercentiles ||
		a.WhalesOff != b.WhalesOff ||
		a.RoundSampleFactors != b.RoundSampleFactors {
		return false
	}
	for i := 0; i < NewMaxTags; i++ {
		ta := a.Name2Tag(TagID(i))
		tb := b.Name2Tag(TagID(i))
		if !SameCompactTag(ta, tb) {
			return false
		}
	}
	for name, ta := range a.name2Tag {
		tb := b.Name2Tag(name)
		if !SameCompactTag(ta, tb) {
			return false
		}
	}
	for name, tb := range b.name2Tag {
		ta := a.Name2Tag(name)
		if !SameCompactTag(ta, tb) {
			return false
		}
	}
	for name, ta := range a.TagsDraft {
		tb := b.TagsDraft[name]
		if !SameCompactTag(&ta, &tb) {
			return false
		}
	}
	for name, tb := range b.TagsDraft {
		ta := a.TagsDraft[name]
		if !SameCompactTag(&ta, &tb) {
			return false
		}
	}
	return true
}

func keepCompactMetricDescription(value *MetricMetaValue) bool {
	return RemoteConfigMetric(value.Name) ||
		value.RoundSampleFactors || value.WhalesOff || // legacy marks without ToggleDescriptionMark
		strings.Contains(value.Description, ToggleDescriptionMark)
}

// beware, modifies value so should be called with value you own
func MakeCompactMetric(value *MetricMetaValue) {
	if !keepCompactMetricDescription(value) {
		value.Description = ""
	}
	value.Visible = false // this flag is restored from Disabled
	value.MetricID = 0    // restored from event anyway
	value.NamespaceID = 0 // restored from event anyway
	value.Name = ""       // restored from event anyway
	value.Version = 0     // restored from event anyway
	cutTags := 0
	for ti := range value.Tags {
		tag := &value.Tags[ti]
		tag.Description = ""
		tag.ValueComments = nil
		tag.Raw = false // this flag is restored from RawKind
		if tag.RawKind != "" || tag.Name != "" {
			cutTags = ti + 1 // keep this tag
		}
	}
	value.Tags = value.Tags[:cutTags]
	for k, v := range value.TagsDraft {
		v.Name = ""
		value.TagsDraft[k] = v
	}
	if !value.HasPercentiles {
		value.Kind = ""
	}
	if value.Weight == 1 {
		value.Weight = 0
	}
	if value.EffectiveResolution == 1 {
		value.Resolution = 0
	}
	value.StringTopDescription = ""
	value.PreKeyTagID = ""
	value.PreKeyFrom = 0
	value.SkipMinHost = false
	value.SkipMaxHost = false
	value.SkipSumSquare = false
	value.PreKeyOnly = false
	value.MetricType = ""
}

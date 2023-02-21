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
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"go4.org/mem"
)

const (
	MaxTags      = 16
	MaxStringLen = 128 // both for normal tags and _s, _h tags (string tops, hostnames)

	tagValueCodePrefix     = " " // regular tag values can't start with whitespace
	tagValueCodeZero       = tagValueCodePrefix + "0"
	TagValueIDUnspecified  = 0
	TagValueIDMappingFlood = -1

	EffectiveWeightOne = 128                      // metric.Weight is multiplied by this and rounded. Do not make too big or metric with weight set to 0 will disappear completely.
	MaxEffectiveWeight = 100 * EffectiveWeightOne // do not make too high, we multiply this by sum of metric serialized length during sampling

	// we deprecated legacy canonical names "key0".."key15", "skey"
	// and made shorter new names "0".."15", "_s", "_h"
	PreKeyTagID    = "prekey"
	StringTopTagID = "skey"
	tagIDPrefix    = "key"
	EnvTagID       = tagIDPrefix + "0"
	EnvTagName     = "env" // this is legacy name. We want to get rid of all different names for tag 0.
	LETagName      = "le"

	NewStringTopTagID = "_s"
	NewHostTagID      = "_h"

	StringTopTagIndex = -1 // used as flag during mapping
	HostTagIndex      = -2 // used as flag during mapping

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
)

// Do not change values, they are stored in DB
const (
	MetricKindCounter          = "counter"
	MetricKindValue            = "value"
	MetricKindValuePercentiles = "value_p"
	MetricKindUnique           = "unique"
	MetricKindStringTopLegacy  = "stop" // Legacy, not valid. Converted into counter during RestoreMetricInfo
	MetricKindMixed            = "mixed"
	MetricKindMixedPercentiles = "mixed_p"
)

var (
	tagIDs             []string             // initialized in builtin.go due to dependency
	newTagIDs          []string             // initialized in builtin.go due to dependency
	tagIDTag2TagID     = map[int32]string{} // initialized in builtin.go due to dependency
	tagIDToIndexForAPI = map[string]int{}   // initialized in builtin.go due to dependency

	errInvalidCodeTagValue = fmt.Errorf("invalid code tag value") // must be fast
	errBadEncoding         = fmt.Errorf("bad utf-8 encoding")     // must be fast
)

type MetricKind int

type MetaStorageInterface interface { // agent uses this to avoid circular dependencies
	Version() int64
	StateHash() string
	GetMetaMetric(metricID int32) *MetricMetaValue
	GetMetaMetricByName(metricName string) *MetricMetaValue
	GetGroup(id int32) *MetricsGroup
}

// This struct is immutable, it is accessed by mapping code without any locking
type MetricMetaTag struct {
	Name          string            `json:"name,omitempty"`
	Description   string            `json:"description,omitempty"`
	Raw           bool              `json:"raw,omitempty"`
	RawKind       string            `json:"raw_kind,omitempty"` // UI can show some raw values beautifully - timestamps, hex values, etc.
	ID2Value      map[int32]string  `json:"id2value,omitempty"`
	ValueComments map[string]string `json:"value_comments,omitempty"`

	Comment2Value map[string]string `json:"-"` // Should be restored from ValueComments after reading
	IsMetric      bool              `json:"-"` // Only for built-in metrics so never saved or parsed
	Index         int               `json:"-"` // Should be restored from position in MetricMetaValue.Tags
	LegacyName    bool              `json:"-"` // Set for "key0".."key15", "env", "skey" to generate ingestion warning
}

const (
	MetricEvent       int32 = 0
	DashboardEvent    int32 = 1
	MetricsGroupEvent int32 = 2
	PromConfigEvent   int32 = 3
)

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
	ID         int32  `json:"group_id"`
	Name       string `json:"name"`
	Version    int64  `json:"version"`
	UpdateTime uint32 `json:"update_time"`

	Weight            float64 `json:"weight,omitempty"`
	Visible           bool    `json:"visible,omitempty"`
	IsWeightEffective bool    `json:"is_weight_effective,omitempty"`
	Protected         bool    `json:"protected,omitempty"`

	EffectiveWeight int64 `json:"-"`
}

// This struct is immutable, it is accessed by mapping code without any locking
type MetricMetaValue struct {
	MetricID   int32  `json:"metric_id"`
	Name       string `json:"name"`
	Version    int64  `json:"version,omitempty"`
	UpdateTime uint32 `json:"update_time"`

	Description          string          `json:"description,omitempty"`
	Tags                 []MetricMetaTag `json:"tags,omitempty"`
	Visible              bool            `json:"visible,omitempty"`
	Kind                 string          `json:"kind"`
	Weight               float64         `json:"weight,omitempty"`
	Resolution           int             `json:"resolution,omitempty"`             // no invariants
	StringTopName        string          `json:"string_top_name,omitempty"`        // no invariants
	StringTopDescription string          `json:"string_top_description,omitempty"` // no invariants
	PreKeyTagID          string          `json:"pre_key_tag_id,omitempty"`
	PreKeyFrom           uint32          `json:"pre_key_from,omitempty"`

	RawTagMask          uint32                   `json:"-"` // Should be restored from Tags after reading
	Name2Tag            map[string]MetricMetaTag `json:"-"` // Should be restored from Tags after reading
	EffectiveResolution int                      `json:"-"` // Should be restored from Tags after reading
	PreKeyIndex         int                      `json:"-"` // index of tag which goes to 'prekey' column, or <0 if no tag goes
	EffectiveWeight     int64                    `json:"-"`
	HasPercentiles      bool                     `json:"-"`
	RoundSampleFactors  bool                     `json:"-"` // Experimental, set if magic word in description is found
	GroupID             int32                    `json:"-"`
	Group               *MetricsGroup            `json:"-"`
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

func (m DashboardMeta) MarshalBinary() ([]byte, error) { return json.Marshal(m) }
func (m *DashboardMeta) UnmarshalBinary(data []byte) error {
	if err := json.Unmarshal(data, m); err != nil {
		return err
	}
	return nil
}

// updates error if name collision happens
func (m *MetricMetaValue) setName2Tag(name string, sTag MetricMetaTag, canonical bool, legacyName bool, err *error) {
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
	sTag.LegacyName = legacyName
	m.Name2Tag[name] = sTag
}

// Always restores maximum info, if error is returned, metric is non-canonical and should not be saved
func (m *MetricMetaValue) RestoreCachedInfo() error {
	var err error
	if !ValidMetricName(mem.S(m.Name)) {
		err = fmt.Errorf("invalid metric name: %q", m.Name)
	}

	if m.Kind == MetricKindStringTopLegacy {
		m.Kind = MetricKindCounter
		if m.StringTopName == "" && m.StringTopDescription == "" { // UI displayed string tag on condition of "kind string top or any of this two set"
			m.StringTopDescription = "string_top"
		}
	}
	if !ValidMetricKind(m.Kind) {
		err = fmt.Errorf("invalid metric kind %q", m.Kind)
	}

	var mask uint32
	m.Name2Tag = map[string]MetricMetaTag{}

	if m.StringTopName == NewStringTopTagID { // remove redundancy
		m.StringTopName = ""
	}
	sTag := MetricMetaTag{
		Name:        m.StringTopName,
		Description: m.StringTopDescription,
		Index:       StringTopTagIndex,
	}
	m.setName2Tag(m.StringTopName, sTag, false, false, &err)
	if len(m.Tags) != 0 {
		// for mast metrics in database, this name is set to "env". We do not want to continue using it.
		// We want mapEnvironment() to work even when metric is not found, so we must not allow users to
		// set their environment tag name. They must use canincal "0" name instead.
		m.Tags[0].Name = ""
	}
	m.PreKeyIndex = -1
	tags := m.Tags
	if len(tags) > MaxTags { // prevent index out of range during mapping
		tags = tags[:MaxTags]
		err = fmt.Errorf("too many tags, limit is: %d", MaxTags)
	}
	for i := range tags {
		tag := &tags[i]
		if tag.Name == NewTagID(i) { // remove redundancy
			tag.Name = ""
		}
		if m.PreKeyTagID == TagID(i) && m.PreKeyFrom != 0 {
			m.PreKeyIndex = i
		}
		if m.PreKeyTagID == NewTagID(i) && m.PreKeyFrom != 0 {
			m.PreKeyIndex = i
		}
		if !ValidRawKind(tag.RawKind) {
			err = fmt.Errorf("invalid raw kind %q of tag %d", tag.RawKind, i)
		}
	}
	if m.PreKeyIndex == -1 && m.PreKeyTagID != "" {
		err = fmt.Errorf("invalid pre_key_tag_id: %q", m.PreKeyTagID)
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

		m.setName2Tag(tag.Name, *tag, false, false, &err)
	}
	m.setName2Tag(StringTopTagID, sTag, false, true, nil)
	m.setName2Tag(NewStringTopTagID, sTag, true, false, &err)
	hTag := MetricMetaTag{
		Name:        "",
		Description: "",
		Index:       HostTagIndex,
	}
	m.setName2Tag(NewHostTagID, hTag, true, false, &err)
	for i := 0; i < MaxTags; i++ { // separate pass to overwrite potential collisions with canonical names in the loop above
		if i < len(tags) {
			m.setName2Tag(NewTagID(i), tags[i], true, false, &err)
			m.setName2Tag(TagID(i), tags[i], false, true, nil)
			if i == 0 {
				// we clear name of env (above), it must have exactly one way to refer to it, "0"
				// but for legacy libraries, we allow to send "env" for now, TODO - remove
				m.setName2Tag("env", tags[i], false, true, nil)
			}
		} else {
			m.setName2Tag(NewTagID(i), MetricMetaTag{Index: i}, true, false, &err)
			m.setName2Tag(TagID(i), MetricMetaTag{Index: i}, false, true, nil)
			if i == 0 {
				m.setName2Tag("env", MetricMetaTag{Index: i}, false, true, nil)
			}
		}
	}
	m.RawTagMask = mask
	m.EffectiveResolution = AllowedResolution(m.Resolution)
	if m.EffectiveResolution != m.Resolution {
		err = fmt.Errorf("resolution %d must be factor of 60", m.Resolution)
	}
	if math.IsNaN(m.Weight) || m.Weight < 0 || m.Weight > math.MaxInt32 {
		err = fmt.Errorf("weight must be from %d to %d", 0, math.MaxInt32)
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
	return err
}

// Always restores maximum info, if error is returned, metric is non-canonical and should not be saved
func (m *MetricsGroup) RestoreCachedInfo() error {
	var err error
	if !ValidGroupName(m.Name) {
		err = fmt.Errorf("invalid metric name: %q", m.Name)
	}
	if math.IsNaN(m.Weight) || m.Weight < 0 || m.Weight > math.MaxInt32 {
		err = fmt.Errorf("weight must be from %d to %d", 0, math.MaxInt32)
	}
	rw := m.Weight * EffectiveWeightOne
	if rw < 1 {
		m.EffectiveWeight = 1
	}
	if rw > MaxEffectiveWeight {
		m.EffectiveWeight = MaxEffectiveWeight
	}
	m.EffectiveWeight = int64(rw)

	return err
}

func (m *MetricsGroup) MetricIn(metric string) bool {
	return strings.HasPrefix(metric, m.Name+"_")
}

func ValidMetricName(s mem.RO) bool {
	return validIdent(s)
}

func ValidGroupName(s string) bool {
	return validIdent(mem.S(s))
}

func ValidDashboardName(s string) bool {
	if len(s) == 0 || len(s) > MaxStringLen || !isLetter(s[0]) {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if !isLetter(c) && c != '_' && !(c >= '0' && c <= '9') && c != ' ' {
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
	// EMPTY:           decimal number, can be negative
	switch s {
	case "", "uint", "ip", "ip_bswap", "hex", "hex_bswap", "timestamp", "timestamp_local":
		return true
	}
	return false
}

func ParseTagIDForAPI(tagID string) int {
	i, ok := tagIDToIndexForAPI[tagID]
	if !ok {
		return -1
	}
	return i
}

func TagID(i int) string {
	return tagIDs[i]
}

func NewTagID(i int) string {
	return newTagIDs[i]
}

func AllowedResolution(r int) int {
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
// 1. trim
// 2. replace all consecutive whitespaces inside with single ASCII space
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

func AppendValidStringValueLegacy(dst []byte, src []byte) ([]byte, error) {
	return appendValidStringValueLegacy(dst, src, MaxStringLen, false)
}

func AppendValidStringValue(dst []byte, src []byte) ([]byte, error) {
	return appendValidStringValue(dst, src, MaxStringLen, false)
}

// We use this function only to validate host names. Can be slow, this is OK.
func ForceValidStringValue(src string) []byte {
	dst, _ := appendValidStringValue(nil, []byte(src), MaxStringLen, true)
	return dst
}

func appendValidStringValueLegacy(dst []byte, src []byte, maxLen int, force bool) ([]byte, error) {
	// trim
	src = bytes.TrimSpace(src) // packet is limited, so this will not be too slow
	// fast path
	if len(src) <= maxLen { // if we cut by maxLen, we will have to trim right side again, which is slow
		fastPath := true
		for _, c := range src {
			if !bytePrint(c) {
				fastPath = false
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
	for r := 0; r < len(src); {
		c, nr := utf8.DecodeRune(src[r:])
		if c == utf8.RuneError && nr <= 1 && !force {
			return dst, errBadEncoding
		}
		if !unicode.IsPrint(c) {
			c = utf8.RuneError
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
		return tagValueCodeZero // fast-path with no allocations
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

func IsValueCodeZero(s string) bool {
	return tagValueCodeZero == s
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

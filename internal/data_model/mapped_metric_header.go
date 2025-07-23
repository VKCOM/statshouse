// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"encoding/binary"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/zeebo/xxh3"
)

type HandlerArgs struct {
	MetricBytes    *tlstatshouse.MetricBytes
	Description    string
	ScrapeInterval int
	Scratch        *[]byte
}

type MapCallbackFunc func(tlstatshouse.MetricBytes, MappedMetricHeader)

type MappedMetricHeader struct {
	ReceiveTime time.Time // Saved at mapping start and used where we need time.Now. This is different to MetricBatch.T, which is sent by clients
	MetricMeta  *format.MetricMetaValue
	Key         Key
	TopValue    TagUnionBytes // reference to memory inside tlstatshouse.MetricBytes.
	HostTag     TagUnionBytes // reference to memory inside tlstatshouse.MetricBytes.

	CheckedTagIndex int  // we check tags one by one, remembering position here, between invocations of mapTags
	ValuesChecked   bool // infs, nans, etc. This might be expensive, so done only once

	OriginalTagValues [format.MaxTags][]byte
	// original strings values as sent by user. Hash of those is stable between agents independent of
	// mappings, so used as a resolution hash to deterministically place same rows into same resolution buckets

	IsTagSet  [format.MaxTags]bool // report setting tags more than once.
	IsSKeySet bool
	IsHKeySet bool

	// errors below
	IngestionStatus int32  // if error happens, this will be != 0. errors are in fast path, so there must be no allocations
	InvalidString   []byte // reference to memory inside tlstatshouse.MetricBytes. If more than 1 problem, reports the last one
	IngestionTagKey int32  // +TagIDShift, as required by "tag_id" in builtin metric. Contains error tad ID for IngestionStatus != 0, or any tag which caused uncached load IngestionStatus == 0

	// warnings below
	NotFoundTagName       []byte // reference to memory inside tlstatshouse.MetricBytes. If more than 1 problem, reports the last one
	FoundDraftTagName     []byte // reference to memory inside tlstatshouse.MetricBytes. If more than 1 problem, reports the last one
	TagSetTwiceKey        int32  // +TagIDShift, as required by "tag_id" in builtin metric. If more than 1, remembers some
	LegacyCanonicalTagKey int32  // +TagIDShift, as required by "tag_id" in builtin metric. If more than 1, remembers some
	InvalidRawValue       []byte // reference to memory inside tlstatshouse.MetricBytes. If more than 1 problem, reports the last one
	InvalidRawTagKey      int32  // key of InvalidRawValue
}

// TODO - implement InvalidRawValue and InvalidRawTagKey

func (h *MappedMetricHeader) SetTag(index int32, id int32, tagIDKey int32) {
	if index == format.HostTagIndex {
		h.HostTag.I = id
		if h.IsHKeySet {
			h.TagSetTwiceKey = tagIDKey
		}
		h.IsHKeySet = true
	} else {
		h.Key.Tags[index] = id
		if h.IsTagSet[index] {
			h.TagSetTwiceKey = tagIDKey
		}
		h.IsTagSet[index] = true
	}
}

func (h *MappedMetricHeader) SetSTag(index int32, value []byte, tagIDKey int32) {
	if index == format.HostTagIndex {
		h.HostTag.S = value
		if h.IsHKeySet {
			h.TagSetTwiceKey = tagIDKey
		}
		h.IsHKeySet = true
	} else {
		h.Key.SetSTag(int(index), string(value))
		if h.IsTagSet[index] {
			h.TagSetTwiceKey = tagIDKey
		}
		h.IsTagSet[index] = true
	}
}

func (h *MappedMetricHeader) SetInvalidString(ingestionStatus int32, tagIDKey int32, invalidString []byte) {
	h.IngestionStatus = ingestionStatus
	h.IngestionTagKey = tagIDKey
	h.InvalidString = invalidString
}

func (h *MappedMetricHeader) OriginalMarshalAppend(buffer []byte) []byte {
	// format: [metric_id] [tagsCount] [tag0] [0] [tag1] [0] [tag2] [0] ...
	// timestamp is not part of the hash.
	// metric is part of the hash so agents can use this marshalling in the future as a key to MultiItems.
	// empty tags suffix is not part of the hash so that # of tags can be increased without changing hash.
	// tagsCount is explicit, so we can add more fields to the right of tags if we need them.
	// we use separator between tags instead of tag length, so we can increase tags length beyond 1 byte without using varint

	const _ = uint(255 - len(h.OriginalTagValues)) // compile time assert to ensure that 1 byte is enough for tags count
	tagsCount := len(h.OriginalTagValues)          // ignore empty tags
	for ; tagsCount > 0 && len(h.OriginalTagValues[tagsCount-1]) == 0; tagsCount-- {
	}
	buffer = binary.LittleEndian.AppendUint32(buffer, uint32(h.MetricMeta.MetricID))
	buffer = append(buffer, byte(tagsCount))
	for _, v := range h.OriginalTagValues[:tagsCount] {
		buffer = append(buffer, v...)
		buffer = append(buffer, 0) // terminator
	}
	return buffer
}

// returns possibly reallocated scratch
func (h *MappedMetricHeader) OriginalHash(scratch []byte) ([]byte, uint64) {
	scratch = h.OriginalMarshalAppend(scratch[:0])
	return scratch, xxh3.Hash(scratch)
}

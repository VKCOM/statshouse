// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"time"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
)

type HandlerArgs struct {
	MetricBytes    *tlstatshouse.MetricBytes
	Description    string
	ScrapeInterval int
	MapCallback    MapCallbackFunc
}

type MapCallbackFunc func(tlstatshouse.MetricBytes, MappedMetricHeader)

type MappedMetricHeader struct {
	ReceiveTime time.Time // Saved at mapping start and used where we need time.Now. This is different to MetricBatch.T, which is sent by clients
	MetricMeta  *format.MetricMetaValue
	Key         Key
	SValue      []byte // reference to memory inside tlstatshouse.MetricBytes.
	HostTag     int32

	CheckedTagIndex int  // we check tags one by one, remembering position here, between invocations of mapTags
	ValuesChecked   bool // infs, nans, etc. This might be expensive, so done only once

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

func (h *MappedMetricHeader) SetTag(index int, id int32, tagIDKey int32) {
	if index == format.HostTagIndex {
		h.HostTag = id
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

func (h *MappedMetricHeader) SetSTag(index int, value string, tagIDKey int32) {
	h.Key.SetSTag(index, value)
	if h.IsTagSet[index] {
		h.TagSetTwiceKey = tagIDKey
	}
	h.IsTagSet[index] = true
}

func (h *MappedMetricHeader) SetInvalidString(ingestionStatus int32, tagIDKey int32, invalidString []byte) {
	h.IngestionStatus = ingestionStatus
	h.IngestionTagKey = tagIDKey
	h.InvalidString = invalidString
}

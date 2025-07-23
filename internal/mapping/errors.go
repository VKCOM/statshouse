// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package mapping

import (
	"fmt"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
)

func MapErrorFromHeader(m tlstatshouse.MetricBytes, h data_model.MappedMetricHeader) error {
	if h.IngestionStatus == 0 { // no errors
		return nil
	}
	ingestionTagName := format.TagIDTagToTagID(h.IngestionTagKey)
	envTag := h.Key.Tags[0] // TODO - we do not want to remember original string value somewhere yet (to print better errors here)
	switch h.IngestionStatus {
	case format.TagValueIDSrcIngestionStatusErrMetricNotFound:
		return fmt.Errorf("metric %q not found (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrNanInfValue:
		return fmt.Errorf("NaN/Inf value for metric %q (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrNanInfCounter:
		return fmt.Errorf("NaN/Inf counter for metric %q (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrNegativeCounter:
		return fmt.Errorf("negative counter for metric %q (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapOther:
		return nil // not written
	case format.TagValueIDSrcIngestionStatusErrMapInvalidRawTagValue:
		return fmt.Errorf("invalid raw tag value %q for key %q of metric %q (envTag %d)", h.InvalidString, ingestionTagName, m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapTagValueCached:
		return fmt.Errorf("error mapping tag value (cached) %q for key %q of metric %q (envTag %d)", h.InvalidString, ingestionTagName, m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapTagValue:
		return fmt.Errorf("failed to map tag value %q for key %q of metric %q (envTag %d)", h.InvalidString, ingestionTagName, m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapGlobalQueueOverload:
		return fmt.Errorf("failed to map metric %q: too many metrics in queue (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapPerMetricQueueOverload:
		return fmt.Errorf("failed to map metric %q: per-metric mapping queue overloaded (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapTagValueEncoding:
		return fmt.Errorf("not utf-8 value %q (hex) for for key %q of metric %q (envTag %d)", h.InvalidString, ingestionTagName, m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusOKLegacy:
		return nil // not written
	case format.TagValueIDSrcIngestionStatusErrMetricNonCanonical:
		return nil // not written
	case format.TagValueIDSrcIngestionStatusErrMetricDisabled:
		return fmt.Errorf("metric %q is disabled (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrLegacyProtocol:
		return nil // not written
	case format.TagValueIDSrcIngestionStatusErrMetricNameEncoding:
		return fmt.Errorf("not utf-8 metric name %q (hex) (envTag %d)", h.InvalidString, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapTagNameEncoding:
		return fmt.Errorf("not utf-8 name %q (hex) for key of metric %q (envTag %d)", h.InvalidString, m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrValueUniqueBothSet:
		return fmt.Errorf("both value and unique fields set in metric event %q (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrShardingFailed:
		return fmt.Errorf("metric %q shard is beyond configured shards (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMetricBuiltin:
		return fmt.Errorf("metric %q is builtin (envTag %d)", m.Name, envTag)
	default:
		return fmt.Errorf("unexpected error status %d with invalid string value %q for key %q of metric %q (envTag %d)", h.IngestionStatus, h.InvalidString, ingestionTagName, m.Name, envTag)
	}
}

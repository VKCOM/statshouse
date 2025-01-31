// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package receiver

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
)

func TestValidateMetric(t *testing.T) {
	m := tlstatshouse.MetricBytes{
		Name: []byte("metric"),
		Tags: []tl.DictionaryFieldStringBytes{{Key: []byte("key0"), Value: []byte("v0")}, {Key: []byte("key3"), Value: []byte("v3")}},
	}
	require.Equal(t, len(m.Tags), 2)
	require.Equal(t, string(m.Tags[0].Key), "key0")
	require.Equal(t, string(m.Tags[0].Value), "v0")
	require.Equal(t, string(m.Tags[1].Key), "key3")
	require.Equal(t, string(m.Tags[1].Value), "v3")
}

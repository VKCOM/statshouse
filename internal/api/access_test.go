// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vkcom/statshouse/internal/format"
)

func TestAccessInfo(t *testing.T) {
	t.Run("view", func(t *testing.T) {
		t.Run("default", func(t *testing.T) {
			ai := accessInfo{
				bitViewDefault: true,
			}
			require.True(t, ai.canViewMetric("foo_bar"))
		})
		t.Run("protected_default", func(t *testing.T) {
			ai := accessInfo{
				bitViewDefault:    true,
				protectedPrefixes: []string{"foo_"},
			}
			require.False(t, ai.canViewMetric("foo_bar"))
		})
		t.Run("prefix", func(t *testing.T) {
			ai := accessInfo{
				bitViewPrefix: map[string]bool{"foo_": true},
			}
			require.True(t, ai.canViewMetric("foo_bar"))
		})
		t.Run("protected_prefix", func(t *testing.T) {
			ai := accessInfo{
				bitViewPrefix:     map[string]bool{"foo_": true},
				protectedPrefixes: []string{"foo_"},
			}
			require.True(t, ai.canViewMetric("foo_bar"))
		})
		t.Run("metric", func(t *testing.T) {
			ai := accessInfo{
				bitViewMetric: map[string]bool{"foo_bar": true},
			}
			require.True(t, ai.canViewMetric("foo_bar"))
		})
		t.Run("protected_metric", func(t *testing.T) {
			ai := accessInfo{
				bitViewMetric:     map[string]bool{"foo_bar": true},
				protectedPrefixes: []string{"foo_"},
			}
			require.True(t, ai.canViewMetric("foo_bar"))
		})
	})
	t.Run("edit", func(t *testing.T) {
		t.Run("default", func(t *testing.T) {
			ai := accessInfo{
				bitEditDefault: true,
			}
			require.True(t, canBasicEdit(&ai, "foo_bar"))
		})
		t.Run("protected_default", func(t *testing.T) {
			ai := accessInfo{
				bitEditDefault:    true,
				protectedPrefixes: []string{"foo_"},
			}
			require.False(t, canBasicEdit(&ai, "foo_bar"))
		})
		t.Run("prefix", func(t *testing.T) {
			ai := accessInfo{
				bitEditPrefix: map[string]bool{"foo_": true},
			}
			require.True(t, canBasicEdit(&ai, "foo_bar"))
		})
		t.Run("protected_prefix", func(t *testing.T) {
			ai := accessInfo{
				bitEditPrefix:     map[string]bool{"foo_": true},
				protectedPrefixes: []string{"foo_"},
			}
			require.True(t, canBasicEdit(&ai, "foo_bar"))
		})
		t.Run("metric", func(t *testing.T) {
			ai := accessInfo{
				bitEditMetric: map[string]bool{"foo_bar": true},
			}
			require.True(t, canBasicEdit(&ai, "foo_bar"))
		})
		t.Run("protected_metric", func(t *testing.T) {
			ai := accessInfo{
				bitEditMetric:     map[string]bool{"foo_bar": true},
				protectedPrefixes: []string{"foo_"},
			}
			require.True(t, canBasicEdit(&ai, "foo_bar"))
		})
	})
}

func canBasicEdit(ai *accessInfo, metric string) bool {
	return ai.canEditMetric(metric, format.MetricMetaValue{Name: metric}, format.MetricMetaValue{Name: metric})
}

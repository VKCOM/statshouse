// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vkcom/statshouse/internal/format"
)

func TestAccessInfo(t *testing.T) {
	t.Run("view", func(t *testing.T) {
		t.Run("default", func(t *testing.T) {
			ai := &AccessInfo{
				bitViewDefault: true,
			}
			require.True(t, canViewMetric(ai, "foo_bar"))
		})
		t.Run("protected_default", func(t *testing.T) {
			ai := &AccessInfo{
				bitViewDefault:    true,
				protectedPrefixes: []string{"foo_"},
			}
			require.False(t, canViewMetric(ai, "foo_bar"))
		})
		t.Run("prefix", func(t *testing.T) {
			ai := &AccessInfo{
				bitViewPrefix: map[string]bool{"foo_": true},
			}
			require.True(t, canViewMetric(ai, "foo_bar"))
		})
		t.Run("protected_prefix", func(t *testing.T) {
			ai := &AccessInfo{
				bitViewPrefix:     map[string]bool{"foo_": true},
				protectedPrefixes: []string{"foo_"},
			}
			require.True(t, canViewMetric(ai, "foo_bar"))
		})
		t.Run("metric", func(t *testing.T) {
			ai := &AccessInfo{
				bitViewMetric: map[string]bool{"foo_bar": true},
			}
			require.True(t, canViewMetric(ai, "foo_bar"))
		})
		t.Run("protected_metric", func(t *testing.T) {
			ai := &AccessInfo{
				bitViewMetric:     map[string]bool{"foo_bar": true},
				protectedPrefixes: []string{"foo_"},
			}
			require.True(t, canViewMetric(ai, "foo_bar"))
		})
		t.Run("protected_metric_by_group", func(t *testing.T) {
			ai := &AccessInfo{
				bitViewPrefix: map[string]bool{"foo_": true},
			}
			require.True(t, canViewMetric(ai, "foo_bar"))
		})

		t.Run("namespaced metric", func(t *testing.T) {
			ai := &AccessInfo{
				bitViewMetric: map[string]bool{"foo_bar": true},
			}
			require.False(t, canViewMetricNamespaced(ai, "foo_bar", "abc"))
		})
		t.Run("namespaced metric", func(t *testing.T) {
			ai := &AccessInfo{
				bitViewMetric: map[string]bool{"abc:foo_bar": true},
			}
			require.True(t, canViewMetricNamespaced(ai, "foo_bar", "abc"))
		})
	})

	t.Run("edit", func(t *testing.T) {
		t.Run("default", func(t *testing.T) {
			ai := AccessInfo{
				bitEditDefault: true,
			}
			require.True(t, canBasicEdit(&ai, "foo_bar", false))
		})
		t.Run("protected_default", func(t *testing.T) {
			ai := AccessInfo{
				bitEditDefault:    true,
				protectedPrefixes: []string{"foo_"},
			}
			require.False(t, canBasicEdit(&ai, "foo_bar", false))
		})
		t.Run("prefix", func(t *testing.T) {
			ai := AccessInfo{
				bitEditPrefix: map[string]bool{"foo_": true},
			}
			require.True(t, canBasicEdit(&ai, "foo_bar", false))
		})
		t.Run("protected_prefix", func(t *testing.T) {
			ai := AccessInfo{
				bitEditPrefix:     map[string]bool{"foo_": true},
				protectedPrefixes: []string{"foo_"},
			}
			require.True(t, canBasicEdit(&ai, "foo_bar", false))
		})
		t.Run("metric", func(t *testing.T) {
			ai := AccessInfo{
				bitEditMetric: map[string]bool{"foo_bar": true},
			}
			require.True(t, canBasicEdit(&ai, "foo_bar", false))
		})
		t.Run("protected_metric", func(t *testing.T) {
			ai := AccessInfo{
				bitEditMetric:     map[string]bool{"foo_bar": true},
				protectedPrefixes: []string{"foo_"},
			}
			require.True(t, canBasicEdit(&ai, "foo_bar", false))
		})

		t.Run("protected_metric_by_group rename", func(t *testing.T) {
			ai := AccessInfo{
				bitEditPrefix: map[string]bool{"foo_": true},
			}
			require.False(t, ai.CanEditMetric(false, format.MetricMetaValue{Name: "abc"}, format.MetricMetaValue{Name: "foo_bar"}))
			require.False(t, ai.CanEditMetric(false, format.MetricMetaValue{Name: "foo_bar"}, format.MetricMetaValue{Name: "abc"}))
			require.True(t, ai.CanEditMetric(false, format.MetricMetaValue{Name: "foo_buzz"}, format.MetricMetaValue{Name: "foo_bar"}))
			ai.BitAdmin = true
			require.True(t, ai.CanEditMetric(false, format.MetricMetaValue{Name: "abc"}, format.MetricMetaValue{Name: "foo_bar"}))
			require.True(t, ai.CanEditMetric(false, format.MetricMetaValue{Name: "foo_bar"}, format.MetricMetaValue{Name: "abc"}))
		})
	})
}

func canViewMetric(ai *AccessInfo, metric string) bool {
	return ai.CanViewMetric(format.MetricMetaValue{Name: metric})
}

func canViewMetricNamespaced(ai *AccessInfo, metric, namespace string) bool {
	m := format.MetricMetaValue{Name: namespace + format.NamespaceSeparator + metric}
	return ai.CanViewMetric(m)
}

func canBasicEdit(ai *AccessInfo, metric string, create bool) bool {
	return ai.CanEditMetric(create, format.MetricMetaValue{Name: metric}, format.MetricMetaValue{Name: metric})
}

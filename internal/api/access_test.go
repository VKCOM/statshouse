// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vkcom/statshouse/internal/format"
)

var nop = &accessManager{getGroupByMetricName: func(s string) *format.MetricsGroup {
	return nil
}}

func TestAccessInfo(t *testing.T) {
	t.Run("view", func(t *testing.T) {
		t.Run("default", func(t *testing.T) {
			ai := accessInfo{
				bitViewDefault: true,
				accessManager:  nop,
			}
			require.True(t, ai.canViewMetric("foo_bar"))
		})
		t.Run("protected_default", func(t *testing.T) {
			ai := accessInfo{
				bitViewDefault:    true,
				protectedPrefixes: []string{"foo_"},
				accessManager:     nop,
			}
			require.False(t, ai.canViewMetric("foo_bar"))
		})
		t.Run("prefix", func(t *testing.T) {
			ai := accessInfo{
				bitViewPrefix: map[string]bool{"foo_": true},
				accessManager: nop,
			}
			require.True(t, ai.canViewMetric("foo_bar"))
		})
		t.Run("protected_prefix", func(t *testing.T) {
			ai := accessInfo{
				bitViewPrefix:     map[string]bool{"foo_": true},
				protectedPrefixes: []string{"foo_"},
				accessManager:     nop,
			}
			require.True(t, ai.canViewMetric("foo_bar"))
		})
		t.Run("metric", func(t *testing.T) {
			ai := accessInfo{
				bitViewMetric: map[string]bool{"foo_bar": true},
				accessManager: nop,
			}
			require.True(t, ai.canViewMetric("foo_bar"))
		})
		t.Run("protected_metric", func(t *testing.T) {
			ai := accessInfo{
				bitViewMetric:     map[string]bool{"foo_bar": true},
				protectedPrefixes: []string{"foo_"},
				accessManager:     nop,
			}
			require.True(t, ai.canViewMetric("foo_bar"))
		})
		t.Run("protected_metric_by_group", func(t *testing.T) {
			ai := accessInfo{
				bitViewPrefix: map[string]bool{"foo_": true},
				accessManager: &accessManager{getGroupByMetricName: func(s string) *format.MetricsGroup {
					return &format.MetricsGroup{
						Name:      "foo",
						Protected: true,
					}
				}},
			}
			require.True(t, ai.canViewMetric("foo_bar"))
		})

		t.Run("protected_metric_by_group", func(t *testing.T) {
			ai := accessInfo{
				bitViewDefault: true,
				accessManager: &accessManager{getGroupByMetricName: func(s string) *format.MetricsGroup {
					return &format.MetricsGroup{
						Name:      "foo",
						Protected: true,
					}
				}},
			}
			require.False(t, ai.canViewMetric("foo_bar"))
		})
	})

	t.Run("edit", func(t *testing.T) {
		t.Run("default", func(t *testing.T) {
			ai := accessInfo{
				bitEditDefault: true,
				accessManager:  nop,
			}
			require.True(t, canBasicEdit(&ai, "foo_bar", false))
		})
		t.Run("protected_default", func(t *testing.T) {
			ai := accessInfo{
				bitEditDefault:    true,
				protectedPrefixes: []string{"foo_"},
				accessManager:     nop,
			}
			require.False(t, canBasicEdit(&ai, "foo_bar", false))
		})
		t.Run("prefix", func(t *testing.T) {
			ai := accessInfo{
				bitEditPrefix: map[string]bool{"foo_": true},
				accessManager: nop,
			}
			require.True(t, canBasicEdit(&ai, "foo_bar", false))
		})
		t.Run("protected_prefix", func(t *testing.T) {
			ai := accessInfo{
				bitEditPrefix:     map[string]bool{"foo_": true},
				protectedPrefixes: []string{"foo_"},
				accessManager:     nop,
			}
			require.True(t, canBasicEdit(&ai, "foo_bar", false))
		})
		t.Run("metric", func(t *testing.T) {
			ai := accessInfo{
				bitEditMetric: map[string]bool{"foo_bar": true},
				accessManager: nop,
			}
			require.True(t, canBasicEdit(&ai, "foo_bar", false))
		})
		t.Run("protected_metric", func(t *testing.T) {
			ai := accessInfo{
				bitEditMetric:     map[string]bool{"foo_bar": true},
				protectedPrefixes: []string{"foo_"},
				accessManager:     nop,
			}
			require.True(t, canBasicEdit(&ai, "foo_bar", false))
		})
		t.Run("protected_metric_by_group", func(t *testing.T) {
			ai := accessInfo{
				bitEditPrefix: map[string]bool{"foo_": true},
				accessManager: &accessManager{getGroupByMetricName: func(s string) *format.MetricsGroup {
					return &format.MetricsGroup{
						Name:      "foo",
						Protected: true,
					}
				}},
			}
			require.True(t, canBasicEdit(&ai, "foo_bar", true))
		})

		t.Run("protected_metric_by_group", func(t *testing.T) {
			ai := accessInfo{
				bitEditDefault: true,
				accessManager: &accessManager{getGroupByMetricName: func(s string) *format.MetricsGroup {
					return &format.MetricsGroup{
						Name:      "foo",
						Protected: true,
					}
				}},
			}
			require.False(t, canBasicEdit(&ai, "foo_bar", true))
		})

		t.Run("protected_metric_by_group", func(t *testing.T) {
			ai := accessInfo{
				bitEditDefault: true,
				accessManager: &accessManager{getGroupByMetricName: func(s string) *format.MetricsGroup {
					return &format.MetricsGroup{
						Name:      "foo",
						Protected: true,
					}
				}},
			}
			require.False(t, canBasicEdit(&ai, "foo_bar", true))
		})

		t.Run("protected_metric_by_group rename", func(t *testing.T) {
			ai := accessInfo{
				bitEditPrefix: map[string]bool{"foo_": true},
				accessManager: &accessManager{getGroupByMetricName: func(s string) *format.MetricsGroup {
					if strings.HasPrefix(s, "foo_") {
						return &format.MetricsGroup{
							Name:      "foo",
							Protected: true,
						}
					}
					return nil
				}},
			}
			require.False(t, ai.canEditMetric(false, format.MetricMetaValue{Name: "abc"}, format.MetricMetaValue{Name: "foo_bar"}))
			require.False(t, ai.canEditMetric(false, format.MetricMetaValue{Name: "foo_bar"}, format.MetricMetaValue{Name: "abc"}))
			require.True(t, ai.canEditMetric(false, format.MetricMetaValue{Name: "foo_buzz"}, format.MetricMetaValue{Name: "foo_bar"}))
			ai.bitAdmin = true
			require.True(t, ai.canEditMetric(false, format.MetricMetaValue{Name: "abc"}, format.MetricMetaValue{Name: "foo_bar"}))
			require.True(t, ai.canEditMetric(false, format.MetricMetaValue{Name: "foo_bar"}, format.MetricMetaValue{Name: "abc"}))
		})
	})
}

func canBasicEdit(ai *accessInfo, metric string, create bool) bool {
	return ai.canEditMetric(create, format.MetricMetaValue{Name: metric}, format.MetricMetaValue{Name: metric})
}

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"fmt"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
	"go4.org/mem"
)

// we need access to global state to correctly assign namespace, check groups, etc.
// so we moved this code in this class for now

func (l *MetricsStorage) SaveDashboard(ctx context.Context, loader MetadataLoader, value format.DashboardMeta, create, remove bool, metadata string) (format.DashboardMeta, error) {
	if !format.ValidDashboardName(value.Name) {
		return format.DashboardMeta{}, fmt.Errorf("invalid dashboard name %w: %q", errorInvalidUserRequest, value.Name)
	}
	event, err := EventFromDashboardMeta(value, metadata, remove)
	if err != nil {
		return format.DashboardMeta{}, err
	}
	event, err = loader.SaveEntity(ctx, event, create, remove)
	if err != nil {
		return format.DashboardMeta{}, err
	}
	d, err := DashboardMetaFromEvent(event)
	if err != nil {
		return format.DashboardMeta{}, err
	}
	return *d, nil
}

func (l *MetricsStorage) SaveMetricsGroup(ctx context.Context, loader MetadataLoader, value format.MetricsGroup, create bool, metadata string) (format.MetricsGroup, error) {
	if !create {
		if l.GetGroup(value.ID) == nil {
			return format.MetricsGroup{}, fmt.Errorf("group %d not found", value.ID)
		}
	}
	builtin := value.ID < 0
	if err := value.RestoreCachedInfo(builtin); err != nil {
		return format.MetricsGroup{}, err
	}
	if builtin {
		builtinGroup, ok := format.BuiltInGroupDefault[value.ID]
		if !ok {
			return format.MetricsGroup{}, fmt.Errorf("invalid buildin group id: %d", value.ID)
		}
		value.Name = builtinGroup.Name // disallow changing name, but if we change built-in name, set it in DB
	} else {
		if !format.ValidGroupName(value.Name) {
			return format.MetricsGroup{}, fmt.Errorf("invalid group name %w: %q", errorInvalidUserRequest, value.Name)
		}
	}
	nsName, _ := format.SplitNamespace(value.Name)
	if nsName != "" {
		ns := l.GetNamespaceByName(nsName)
		if ns == nil {
			return format.MetricsGroup{}, fmt.Errorf("group is in namespace %s, which is not found", nsName)
		}
		value.NamespaceID = ns.ID
	}
	event, err := EventFromGroupMeta(value, metadata)
	if err != nil {
		return format.MetricsGroup{}, err
	}
	event, err = loader.SaveEntity(ctx, event, create, false)
	if err != nil {
		return format.MetricsGroup{}, err
	}
	g, err := GroupMetaFromEvent(event)
	if err != nil {
		return format.MetricsGroup{}, err
	}
	return *g, nil
}

func (l *MetricsStorage) SaveNamespace(ctx context.Context, loader MetadataLoader, value format.NamespaceMeta, create bool, metadata string) (format.NamespaceMeta, error) {
	builtin := value.ID < 0
	if err := value.RestoreCachedInfo(builtin); err != nil {
		return format.NamespaceMeta{}, err
	}
	if builtin {
		builtinNamespace, ok := format.BuiltInNamespaceDefault[value.ID]
		if !ok {
			return format.NamespaceMeta{}, fmt.Errorf("invalid builtin namespace id: %d", value.ID)
		}
		value.Name = builtinNamespace.Name // disallow changing name, but if we change built-in name, set it in DB
		create = value.Version == 0        // redundant, but meta engine logic requires it
	} else {
		if !format.ValidMetricName(mem.S(value.Name)) {
			return format.NamespaceMeta{}, fmt.Errorf("invalid namespace name %w: %q", errorInvalidUserRequest, value.Name)
		}
		if !create {
			existing := l.GetNamespace(value.ID)
			if existing == nil {
				return format.NamespaceMeta{}, fmt.Errorf("namespace %d not found", value.ID)
			}
			if existing.Name != value.Name {
				return format.NamespaceMeta{}, fmt.Errorf("namespace %d impossible to rename", value.ID)
			}
		}
	}
	event, err := EventFromNamespaceMeta(value, metadata)
	if err != nil {
		return format.NamespaceMeta{}, err
	}
	event, err = loader.SaveEntity(ctx, event, create, false)
	if err != nil {
		return format.NamespaceMeta{}, err
	}
	n, err := NamespaceMetaFromEvent(event)
	if err != nil {
		return format.NamespaceMeta{}, err
	}
	return *n, nil
}

func (l *MetricsStorage) SaveMetric(ctx context.Context, loader MetadataLoader, value format.MetricMetaValue, metadata string) (format.MetricMetaValue, error) {
	if _, ok := format.BuiltinMetrics[value.MetricID]; ok {
		return format.MetricMetaValue{}, fmt.Errorf("builtin metric cannot be edited")
	}
	create := value.MetricID == 0
	nsName, _ := format.SplitNamespace(value.Name)
	if nsName != "" {
		ns := l.GetNamespaceByName(nsName)
		if ns == nil {
			return format.MetricMetaValue{}, fmt.Errorf("metric is in namespace %s, which is not found", nsName)
		}
		value.NamespaceID = ns.ID
	}
	event, err := EventFromMetricMeta(value, metadata)
	if err != nil {
		return format.MetricMetaValue{}, err
	}
	event, err = loader.SaveEntity(ctx, event, create, false)
	if err != nil {
		return format.MetricMetaValue{}, wrapSaveEntityError(err)
	}
	mm, err := MetricMetaFromEvent(event)
	if err != nil {
		return format.MetricMetaValue{}, fmt.Errorf("failed to deserialize json metric: %w", err)
	}
	return *mm, nil
}

// prometheus settings, probably will be removed at some point
func (l *MetricsStorage) SaveScrapeConfig(ctx context.Context, loader MetadataLoader, version int64, config string, metadata string) (tlmetadata.Event, error) {
	event := tlmetadata.Event{
		Id:        format.PrometheusConfigID,
		Name:      "prom-config",
		EventType: format.PromConfigEvent,
		Version:   version,
		Data:      config,
	}
	event.SetMetadata(metadata)
	event, err := loader.SaveEntity(ctx, event, false, false)
	if err != nil {
		return event, fmt.Errorf("failed to change prom config: %w", err)
	}
	return event, nil
}

func (l *MetricsStorage) SaveScrapeStaticConfig(ctx context.Context, loader MetadataLoader, version int64, config string) (tlmetadata.Event, error) {
	event := tlmetadata.Event{
		Id:        format.PrometheusGeneratedConfigID,
		Name:      "prom-static-config",
		EventType: format.PromConfigEvent,
		Version:   version,
		Data:      config,
	}
	event, err := loader.SaveEntity(ctx, event, false, false)
	if err != nil {
		return event, fmt.Errorf("failed to change prom static config: %w", err)
	}
	return event, nil
}

func (l *MetricsStorage) SaveKnownTagsConfig(ctx context.Context, loader MetadataLoader, version int64, config string) (tlmetadata.Event, error) {
	event := tlmetadata.Event{
		Id:        format.KnownTagsConfigID,
		Name:      "prom-known-tags",
		EventType: format.PromConfigEvent,
		Version:   version,
		Data:      config,
	}
	event, err := loader.SaveEntity(ctx, event, false, false)
	if err != nil {
		return event, fmt.Errorf("failed to change prom known tags config: %w", err)
	}
	return event, nil
}

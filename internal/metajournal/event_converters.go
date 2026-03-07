// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/mailru/easyjson"
)

func MetricMetaFromEvent(e tlmetadata.Event) (*format.MetricMetaValue, error) {
	if e.Id < math.MinInt32 || e.Id > math.MaxInt32 {
		return nil, fmt.Errorf("metric ID %d assigned by metaengine does not fit into int32 for metric %q", e.Id, e.Name)
	}
	value := &format.MetricMetaValue{}
	err := easyjson.Unmarshal([]byte(e.Data), value) // TODO - use unsafe?
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal metric %d %s: %v", e.Id, e.Name, err)
	}
	value.NamespaceID = int32(e.NamespaceId)
	value.Version = e.Version
	value.Name = e.Name
	value.MetricID = int32(e.Id)
	value.UpdateTime = e.UpdateTime
	_ = value.RestoreCachedInfo() // TODO: older agents can get errors here, so we decided to ignore
	return value, nil
}

func EventFromMetricMeta(value format.MetricMetaValue, metadata string) (tlmetadata.Event, error) {
	event := tlmetadata.Event{
		Id:        int64(value.MetricID),
		Name:      value.Name,
		EventType: format.MetricEvent,
		Version:   value.Version,
	}
	value.MetricID = 0
	value.Name = ""
	value.Version = 0
	value.UpdateTime = 0
	metricBytes, err := easyjson.Marshal(value)
	if err != nil {
		return tlmetadata.Event{}, fmt.Errorf("failed to serialize metric %s: %w", value.Name, err)
	}
	event.Data = string(metricBytes)
	event.SetMetadata(metadata)
	event.SetNamespaceId(int64(value.NamespaceID))
	return event, nil
}

func DashboardMetaFromEvent(e tlmetadata.Event) (*format.DashboardMeta, error) {
	if e.Id < math.MinInt32 || e.Id > math.MaxInt32 {
		return nil, fmt.Errorf("dashboard ID %d assigned by metaengine does not fit into int32 for dashboard %q", e.Id, e.Name)
	}
	value := &format.DashboardMeta{}
	m := map[string]interface{}{}
	err := json.Unmarshal([]byte(e.Data), &m)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal dashboard %v %s: %v", e.Id, e.Name, err)
	}
	value.DashboardID = int32(e.Id)
	value.Name = e.Name
	value.Version = e.Version
	value.UpdateTime = e.UpdateTime
	value.DeleteTime = e.Unused
	value.JSONData = m
	e2 := e
	e2.Data = ""
	fmt.Printf("loading EventFromDashboardMeta %d: %v\n", e.Id, e2)

	return value, nil
}

func EventFromDashboardMeta(value format.DashboardMeta, metadata string, remove bool) (tlmetadata.Event, error) {
	metricBytes, err := json.Marshal(value.JSONData)
	if err != nil {
		return tlmetadata.Event{}, fmt.Errorf("faield to serialize dashboard %s: %w", value.Name, err)
	}
	e := tlmetadata.Event{
		Id:        int64(value.DashboardID),
		Name:      value.Name,
		EventType: format.DashboardEvent,
		Version:   value.Version,
		Unused:    value.DeleteTime,
		Data:      string(metricBytes),
	}
	e2 := e
	e2.Data = ""
	fmt.Printf("saving EventFromDashboardMeta %d, remove=%v: %v\n", e.Id, remove, e2)
	e.SetMetadata(metadata)
	return e, nil
}

func GroupMetaFromEvent(e tlmetadata.Event) (*format.MetricsGroup, error) {
	if e.Id < math.MinInt32 || e.Id > math.MaxInt32 {
		return nil, fmt.Errorf("group ID %d assigned by metaengine does not fit into int32 for group %q", e.Id, e.Name)
	}
	value := &format.MetricsGroup{}
	err := easyjson.Unmarshal([]byte(e.Data), value)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal group %v %s: %v", e.Id, e.Name, err)
	}
	value.Version = e.Version
	value.Name = e.Name
	value.ID = int32(e.Id)
	value.NamespaceID = int32(e.NamespaceId)
	value.UpdateTime = e.UpdateTime
	_ = value.RestoreCachedInfo(value.ID < 0)
	return value, nil
}

func EventFromGroupMeta(value format.MetricsGroup, metadata string) (tlmetadata.Event, error) {
	event := tlmetadata.Event{
		Id:        int64(value.ID),
		Name:      value.Name,
		EventType: format.MetricsGroupEvent,
		Version:   value.Version,
	}
	value.ID = 0
	value.Name = ""
	value.Version = 0
	value.UpdateTime = 0
	groupBytes, err := easyjson.Marshal(value)
	if err != nil {
		return tlmetadata.Event{}, fmt.Errorf("faield to serialize getoup %s: %w", value.Name, err)
	}
	event.Data = string(groupBytes)
	event.SetMetadata(metadata)
	event.SetNamespaceId(int64(value.NamespaceID)) // TODO - remove after all clients ignore namespace in events
	return event, nil
}

func NamespaceMetaFromEvent(e tlmetadata.Event) (*format.NamespaceMeta, error) {
	if e.Id < math.MinInt32 || e.Id > math.MaxInt32 {
		return nil, fmt.Errorf("namespace ID %d assigned by metaengine does not fit into int32 for namespace %q", e.Id, e.Name)
	}
	value := &format.NamespaceMeta{}
	err := easyjson.Unmarshal([]byte(e.Data), value)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal namespace %v %s: %v", e.Id, e.Name, err)
	}
	value.ID = int32(e.Id)
	value.Name = e.Name
	value.Version = e.Version
	value.UpdateTime = e.UpdateTime
	_ = value.RestoreCachedInfo(value.ID < 0)
	return value, nil
}

func EventFromNamespaceMeta(value format.NamespaceMeta, metadata string) (tlmetadata.Event, error) {
	event := tlmetadata.Event{
		Id:        int64(value.ID),
		Name:      value.Name,
		EventType: format.NamespaceEvent,
		Version:   value.Version,
	}
	value.ID = 0
	value.Name = ""
	value.Version = 0
	value.UpdateTime = 0
	value.DeleteTime = 0
	groupBytes, err := easyjson.Marshal(value)
	if err != nil {
		return tlmetadata.Event{}, fmt.Errorf("faield to serialize namespace %s: %w", value.Name, err)
	}
	event.Data = string(groupBytes)
	event.SetMetadata(metadata)
	// namespace does not belong to another namespace, so do not set
	return event, nil
}

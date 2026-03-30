// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metarqlite

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/metajournal"
)

//

const (
	pollInterval = time.Second * 1
)

var errInvalidKeyValue = errors.New("key value is invalid")
var errEmptyStringMapping = errors.New("empty string mapping is special should never be created")

type RQLiteLoader struct {
	loadTimeout      time.Duration
	client           *http.Client
	parent           metajournal.MetadataLoader
	mu               sync.Mutex
	lastConfigUpdate time.Time
	config           config
	addresses        []string
	journalUpdated   chan struct{}
	mappingsUpdated  chan struct{}
}

func NewRQliteLoader(addresses string, loadTimeout time.Duration, parent metajournal.MetadataLoader) *RQLiteLoader {
	result := &RQLiteLoader{
		client:          &http.Client{Timeout: loadTimeout},
		loadTimeout:     loadTimeout,
		parent:          parent,
		journalUpdated:  make(chan struct{}, 1), // only single long poll user is woken up
		mappingsUpdated: make(chan struct{}, 1), // only single long poll user is woken up
	}
	result.SetConfig(addresses)
	// we need long poll on actual Raft log ID, otherwise we will not get updates ASAP always

	return result
}

func (l *RQLiteLoader) wakeUp(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (l *RQLiteLoader) GetMetric(ctx context.Context, id int64, version int64) (ret format.MetricMetaValue, err error) {
	address, _, _ := l.getConfig()
	if address == "" {
		return l.parent.GetMetric(ctx, id, version)
	}
	event, err := l.getEntity(ctx, id, version)
	if err != nil {
		return format.MetricMetaValue{}, err
	}
	m, err := metajournal.MetricMetaFromEvent(event)
	if err != nil {
		return format.MetricMetaValue{}, err
	}
	return *m, nil
}

func (l *RQLiteLoader) GetDashboard(ctx context.Context, id int64, version int64) (ret format.DashboardMeta, err error) {
	address, _, _ := l.getConfig()
	if address == "" {
		return l.parent.GetDashboard(ctx, id, version)
	}
	event, err := l.getEntity(ctx, id, version)
	if err != nil {
		return format.DashboardMeta{}, err
	}
	d, err := metajournal.DashboardMetaFromEvent(event)
	if err != nil {
		return format.DashboardMeta{}, err
	}
	return *d, nil
}

func (l *RQLiteLoader) LoadJournal(ctx context.Context, lastVersion int64, returnIfEmpty bool) (events []tlmetadata.Event, lastKnownVersion int64, _ error) {
	address, _, _ := l.getConfig()
	if address == "" {
		return l.parent.LoadJournal(ctx, lastVersion, returnIfEmpty)
	}
	args := fmt.Sprintf(`{"lastVersion":%d, "limit":%d}`, lastVersion, 2)

	reqBody := l.makeBody([]string{
		`SELECT id, name, version, data, updated_at, type, deleted_at, namespace_id, '' FROM metrics_v5 
                     WHERE version > :lastVersion ORDER BY version asc LIMIT :limit`,
		`SELECT id, name, version, data, updated_at, type, deleted_at, namespace_id, '' FROM metrics_v5 ORDER BY id DESC LIMIT 1`,
	}, args)
	respBody, err := l.sendRequest(address, "query", false, reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to load journal: %w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected response JSON format: %w", err)
		}
	}()
	top := jtop(respBody)
	results := jarray(top["results"])
	values1 := jarray(jobject(results[0])["values"])

	for _, v := range values1 {
		e := jevent(v)
		events = append(events, e)
	}

	values2 := jarray(jobject(results[1])["values"])
	for _, v := range values2 {
		m := jevent(v)
		lastKnownVersion = m.Version
	}
	if returnIfEmpty || len(events) != 0 {
		return
	}

	timer := time.NewTimer(pollInterval)
	defer timer.Stop()

	select {
	case <-timer.C:
		return
	case <-l.journalUpdated:
		return
	}
}

func (l *RQLiteLoader) SaveEntity(ctx context.Context, event tlmetadata.Event, create bool, del bool) (ret tlmetadata.Event, err error) {
	address, _, _ := l.getConfig()
	if address == "" {
		return l.parent.SaveEntity(ctx, event, create, del)
	}
	event.UpdateTime = uint32(time.Now().Unix()) // simply overwrite

	args := fmt.Sprintf(`{"id":%d, "name":%q, "version":%d, "data":%q, "updated_at":%d, "type":%d, "deleted_at":%d, "namespace_id":%d, "metadata":%q}`,
		event.Id, event.Name, event.Version, event.Data, event.UpdateTime, event.EventType, event.Unused, event.NamespaceId, event.Metadata)
	// if version != 0 || id > 0, (type,id,version) must exist in DB, so update where type,id,version ==
	// elseif version == 0 && id < 0, id must not exist in DB, so insert directly
	// else (version == 0 && id == 0) insert into DB directly
	selectByID := `SELECT id, name, version, data, updated_at, type, deleted_at, namespace_id, '' FROM metrics_v5
                             WHERE id = :id`
	historyWithID := `INSERT INTO entity_history (entity_id, name, version, data, updated_at, type, deleted_at, namespace_id, metadata) 
                             values (:id, :name, (SELECT IFNULL(MAX(version), 0) FROM metrics_v5), :data, :updated_at, :type, :deleted_at, :namespace_id, :metadata)`

	var statements []string
	if event.Version != 0 || event.Id > 0 { // update existing, must exist
		// if UPDATE updates nothing, history insert will fail because of version collision
		statements = []string{
			selectByID,
			`UPDATE metrics_v5 SET name = :name, data = :data, updated_at = :updated_at, deleted_at = :deleted_at, namespace_id = :namespace_id,
                             version = (SELECT IFNULL(MAX(version), 0) + 1 FROM metrics_v5)
                             WHERE id = :id AND type = :type AND version = :version `,
			selectByID,
			historyWithID,
		}
	} else if event.Version == 0 && event.Id < 0 {
		statements = []string{
			selectByID,
			`INSERT INTO metrics_v5 (id, name, version, data, updated_at, type, deleted_at, namespace_id) 
                             values (:id, :name, (SELECT IFNULL(MAX(version), 0) + 1 FROM metrics_v5), :data, :updated_at, :type, :deleted_at, :namespace_id)`,
			selectByID,
			historyWithID,
		}
	} else { // version == 0 && id == 0
		statements = []string{
			`SELECT id, name, version, data, updated_at, type, deleted_at, namespace_id FROM metrics_v5
                             WHERE name = :name and type = :type`,
			`INSERT INTO  metrics_v5 (name, version, data, updated_at, type, deleted_at, namespace_id) 
                             values (:name, (SELECT IFNULL(MAX(version), 0) + 1 FROM metrics_v5), :data, :updated_at, :type, :deleted_at, :namespace_id)`,
			strings.ReplaceAll(selectByID, ":id", "last_insert_rowid()"),
			strings.ReplaceAll(historyWithID, ":id", "last_insert_rowid()"),
		}
	}
	reqBody := l.makeBody(statements, args)
	respBody, err := l.sendRequest(address, "request", true, reqBody)
	if err != nil {
		return ret, err
	}
	l.wakeUp(l.journalUpdated)
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected response JSON format: %w", err)
		}
	}()
	top := jtop(respBody)
	results := jarray(top["results"])
	values0 := jarray(jobject(results[0])["values"])

	for _, v := range values0 { // set existing in case of error
		ret = jevent(v)
	}
	values2 := jarray(jobject(results[2])["values"])
	error2 := jstring(jobject(results[2])["error"])

	if len(values2) != 0 {
		e := jevent(values2[0])
		if (event.Version != 0 || event.Id > 0) && e.Version == ret.Version {
			return ret, fmt.Errorf("stale entity with type %d, name %s, please refresh to get latest version (id %d, version %d): %s", ret.EventType, ret.Name, ret.Id, ret.Version, error2)
		}
		return e, nil
	}
	if event.Version == 0 && event.Id == 0 {
		return ret, fmt.Errorf("entity with type %d, name %s (id %d, version %d) already exists: %s", ret.EventType, ret.Name, ret.Id, ret.Version, error2)
	}
	return ret, fmt.Errorf("stale entity with type %d, name %s, please refresh to get latest version (id %d, version %d): %s", ret.EventType, ret.Name, ret.Id, ret.Version, error2)
}

func (l *RQLiteLoader) getEntity(ctx context.Context, id int64, version int64) (ret tlmetadata.Event, err error) {
	address, _, _ := l.getConfig()
	if address == "" {
		return ret, fmt.Errorf("unexpected change of config")
	}

	args := fmt.Sprintf(`{"id":%d,"version":%d}`, id, version)
	reqBody := l.makeBody([]string{
		`SELECT entity_id, name, version, data, updated_at, type, deleted_at, namespace_id, metadata FROM entity_history 
                     WHERE entity_id = :id AND version = :version`,
	}, args)
	respBody, err := l.sendRequest(address, "request", true, reqBody)
	if err != nil {
		return ret, err
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected response JSON format: %w", err)
		}
	}()
	top := jtop(respBody)
	results := jarray(top["results"])
	values0 := jarray(jobject(results[0])["values"])

	for _, v := range values0 {
		e := jevent(v)
		return e, nil
	}
	return ret, fmt.Errorf("entity with id %d, version %d not found", id, version)
}

func (l *RQLiteLoader) GetShortHistory(ctx context.Context, id int64) (ret tlmetadata.HistoryShortResponse, err error) {
	address, _, _ := l.getConfig()
	if address == "" {
		return l.parent.GetShortHistory(ctx, id)
	}

	args := fmt.Sprintf(`{"id":%d,"limit":%d}`, id, 1000)
	reqBody := l.makeBody([]string{
		`SELECT entity_id, name, version, '', updated_at, type, deleted_at, namespace_id, metadata FROM entity_history 
                     WHERE entity_id = :id ORDER BY version DESC LIMIT :limit`,
	}, args)
	respBody, err := l.sendRequest(address, "request", true, reqBody)
	if err != nil {
		return ret, err
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected response JSON format: %w", err)
		}
	}()
	top := jtop(respBody)
	results := jarray(top["results"])
	values0 := jarray(jobject(results[0])["values"])

	entityHistoryMaxResponseSize := 4_000_000
	size := 1
	for _, v := range values0 {
		e := jevent(v)
		size += len(e.Metadata) + 16
		if size > entityHistoryMaxResponseSize {
			break
		}
		ret.Events = append(ret.Events, tlmetadata.HistoryShortResponseEvent{
			Version:  e.Version,
			Metadata: e.Metadata,
		})
	}
	return ret, nil
}

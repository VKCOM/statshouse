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
	"math/rand/v2"
	"strings"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
	"go4.org/mem"
)

func (l *RQLiteLoader) GetNewMappings(ctx context.Context, lastVersion int32,
	returnIfEmpty bool) (mappings []tlstatshouse.Mapping, curV, lastV int32, err error) {
	address, _, err := l.updateConfig()
	if address == "" {
		return l.parent.GetNewMappings(ctx, lastVersion, returnIfEmpty)
	}
	if err != nil {
		return nil, 0, 0, err
	}
	args := fmt.Sprintf(`{"lastVersion":%d, "limit":%d}`, lastVersion, 50000)
	reqBody := l.makeBody([]string{
		`SELECT id, name FROM mappings where id > :lastVersion ORDER BY id asc limit :limit`,
		`SELECT id, name FROM mappings ORDER BY id desc limit 1`,
	}, args)
	respBody, err := l.sendRequest(address, "query", false, reqBody)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to load mapping: %w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected response JSON format: %w", err)
		}
	}()
	top := jtop(respBody)
	results := jarray(top["results"])
	values1 := jarray(jobject(results[0])["values"])

	curV = lastVersion
	lastV = lastVersion
	for _, v := range values1 {
		m := jmapping(v)
		mappings = append(mappings, m)
		curV = m.Value
		lastV = m.Value
	}

	values2 := jarray(jobject(results[1])["values"])
	for _, v := range values2 {
		m := jmapping(v)
		lastV = m.Value
	}
	if returnIfEmpty || len(mappings) != 0 {
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

// TODO - remove from codebase after full switch to rqlite
func (l *RQLiteLoader) PutTagMapping(ctx context.Context, tag string, id int32) error {
	address, _, err := l.updateConfig()
	if address == "" {
		return l.parent.PutTagMapping(ctx, tag, id)
	}
	if err != nil {
		return err
	}
	return errors.New("PutTagMapping not implemented - user rqlite console to manually modify mappings")
}

func (l *RQLiteLoader) GetTagMapping(ctx context.Context, tag string, metricName string, create bool) (int32, int32, time.Duration, error) {
	address, cfg, err := l.updateConfig()
	if address == "" {
		return l.parent.GetTagMapping(ctx, tag, metricName, create)
	}
	if err != nil {
		return 0, format.TagValueIDAggMappingCreatedStatusErrorPMC, pmcBigNegativeCacheTTL, err
	}
	if tag == "" {
		return 0, format.TagValueIDAggMappingCreatedStatusErrorInvariant, pmcBigNegativeCacheTTL, errEmptyStringMapping
	}
	if !format.ValidStringValue(mem.S(tag)) {
		return 0, format.TagValueIDAggMappingCreatedStatusErrorInvalidValue, pmcBigNegativeCacheTTL, errInvalidKeyValue
	}

	if !create {
		return 0, format.TagValueIDAggMappingCreatedStatusErrorInvalidValue, pmcBigNegativeCacheTTL, errors.New("GetTagMapping with create=false not implemented - all components must use subscription to all mappings")
	}
	// 1. select existing metric if subsequent creation will fail for reason it exists
	// 2. update flood limits - first initialize if not there
	// 3. then increase counter if time passed
	// 4. insert only if there is budget. on name collision statement fails, and we stop processing, so do not decrease budget in the next step.
	// 5. if budget = 0, then the next statement will also not decrease below 0

	now := time.Now().Unix()
	updateTime := int64(cfg.mappingStepSec) * (now / int64(cfg.mappingStepSec)) // 1 budget increase per step
	// TODO - clean flood_limits periodically or every time
	// ["DELETE FROM flood_limits WHERE count_free + (? - last_time_update) / ? >= ? AND count_free < ?"]
	args := fmt.Sprintf(`{"tag":%q, "metric":%q, "time_update":%d, "max_budget":%d, "step_sec":%d}`, tag, metricName, updateTime, cfg.mappingMaxBudget, cfg.mappingStepSec)

	reqBody := l.makeBody([]string{
		`SELECT id, name from mappings where name = :tag`,
		`INSERT OR IGNORE INTO flood_limits (metric_name, last_time_update, count_free) 
       	             values (:metric, :time_update, :max_budget)`,
		`UPDATE flood_limits SET last_time_update = :time_update, count_free = min(:max_budget, count_free + (:time_update - last_time_update) / :step_sec) 
                     where metric_name = :metric and count_free < :max_budget
                     RETURNING last_time_update, count_free`,
		`INSERT INTO mappings (id, name) select NULL, :tag WHERE EXISTS 
                     (SELECT 1 FROM flood_limits WHERE flood_limits.metric_name = :metric and flood_limits.count_free > 0)`,
		`UPDATE flood_limits SET count_free = count_free-1 
                     where metric_name = :metric and count_free > 0
                     RETURNING last_time_update, count_free`,
		`SELECT id, name from mappings where name = :tag`,
	}, args)
	respBody, err := l.sendRequest(address, "request", true, reqBody)
	if err != nil {
		return 0, format.TagValueIDAggMappingCreatedStatusErrorPMC, 0, err
	}
	l.wakeUp(l.mappingsUpdated)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected response JSON format: %w", err)
		}
	}()
	top := jtop(respBody)
	results := jarray(top["results"])
	values1 := jarray(jobject(results[0])["values"])
	if len(values1) == 1 {
		m := jmapping(values1[0])
		if m.Value == 0 {
			return 0, format.TagValueIDAggMappingCreatedStatusErrorInvariant, pmcBigNegativeCacheTTL, fmt.Errorf("metdata returned %q -> 0 mapping, which is invalid", tag)
		}
		return m.Value, format.TagValueIDAggMappingCreatedStatusOK, 0, nil
	}
	values5 := jarray(jobject(results[5])["values"])
	if len(values5) == 1 {
		m := jmapping(values5[0])
		if m.Value == 0 {
			return 0, format.TagValueIDAggMappingCreatedStatusErrorInvariant, pmcBigNegativeCacheTTL, fmt.Errorf("metdata returned %q -> 0 mapping, which is invalid", tag)
		}
		return m.Value, format.TagValueIDAggMappingCreatedStatusCreated, 0, nil
	}
	return format.TagValueIDMappingFlood, format.TagValueIDAggMappingCreatedStatusFlood, -1, nil // use TTL of -1 to avoid caching the "mapping"
}

func (l *RQLiteLoader) ResetFlood(ctx context.Context, metricName string, value int32) (before int32, after int32, _ error) {
	address, cfg, err := l.updateConfig()
	if address == "" {
		return l.parent.ResetFlood(ctx, metricName, value)
	}
	if err != nil {
		return 0, 0, err
	}
	now := time.Now().Unix()
	updateTime := int64(cfg.mappingStepSec) * (now / int64(cfg.mappingStepSec)) // 1 budget increase per step

	if value <= 0 {
		value = int32(cfg.mappingMaxBudget)
	}

	args := fmt.Sprintf(`{"metric":%q, "time_update":%d, "budget":%d}`, metricName, updateTime, value)
	reqBody := l.makeBody([]string{
		`SELECT count_free FROM flood_limits WHERE metric_name = :metric`,
		`INSERT OR IGNORE INTO flood_limits (metric_name, last_time_update, count_free) 
                     VALUES (:metric, :time_update, :budget)`,
		`UPDATE flood_limits SET last_time_update = :time_update, count_free = :budget 
                     WHERE metric_name = :metric`,
	}, args)
	respBody, err := l.sendRequest(address, "request", true, reqBody)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected response JSON format: %w", err)
		}
	}()
	top := jtop(respBody)
	results := jarray(top["results"])
	values0 := jarray(jobject(results[0])["values"])
	if len(values0) == 1 {
		vv := jarray(values0[0])
		before = jint32(vv[0])
	}
	return before, value, nil
}

func (l *RQLiteLoader) CreateRandomMappings(count int) ([]byte, error) {
	address, _, _ := l.getConfig()

	str := strings.Builder{}
	str.WriteString(`[["INSERT INTO mappings (name) values `)
	for i := 0; i < count; i++ {
		str.WriteString(fmt.Sprintf(`('randomlyInsertedString%d')`, rand.Uint64()))
		if i != count-1 {
			str.WriteString(",")
		}
	}
	str.WriteString(`"]]`)
	reqBody := str.String()

	respBody, err := l.sendRequest(address, "request", true, reqBody)
	return respBody, err
}

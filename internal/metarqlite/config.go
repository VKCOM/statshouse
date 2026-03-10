// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metarqlite

import (
	"fmt"
	"strings"
	"time"
)

const DefaultMetaTimeout = 10 * time.Second
const ConfigUpdateTimeout = 10 * time.Second
const maxBudget = 50
const budgetIncrementTime = 1800

type config struct {
	mappingMaxBudget int
	mappingStepSec   int
}

func (c *config) validate() {
	if c.mappingMaxBudget <= 0 {
		c.mappingMaxBudget = maxBudget
	}
	if c.mappingStepSec <= 0 {
		c.mappingStepSec = budgetIncrementTime
	}
	c.mappingMaxBudget = max(1, min(1000, c.mappingMaxBudget))
	c.mappingStepSec = max(60, min(86400, c.mappingStepSec))
}

func (l *RQLiteLoader) SetConfig(addresses string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.addresses = l.addresses[:0]
	for _, a := range strings.Split(addresses, ",") {
		a = strings.TrimSpace(a)
		if a != "" {
			l.addresses = append(l.addresses, a)
		}
	}
}

func (l *RQLiteLoader) getConfig() (address string, _ config, lastConfigUpdate time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.addresses) > 0 {
		address = l.addresses[0]
	}
	return address, l.config, l.lastConfigUpdate
}

func (l *RQLiteLoader) updateConfig() (address string, cfg config, _ error) {
	var lastConfigUpdate time.Time
	address, cfg, lastConfigUpdate = l.getConfig()
	if address == "" {
		return
	}
	cfg.validate()
	if time.Since(lastConfigUpdate) < ConfigUpdateTimeout {
		return
	}
	reqBody := `[
		["SELECT CAST(data AS TEXT) FROM property WHERE name = 'mapping-max-budget'"],
		["SELECT CAST(data AS TEXT) FROM property WHERE name = 'mapping-step-sec'"]
	]`
	respBody, err := l.sendRequest(address, "query", true, reqBody)
	if err != nil {
		return address, cfg, err
	}
	var newcfg config
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
		newcfg.mappingMaxBudget = int(jint32(vv[0]))
	}
	values1 := jarray(jobject(results[1])["values"])
	if len(values1) == 1 {
		vv := jarray(values0[1])
		newcfg.mappingStepSec = int(jint32(vv[0]))
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastConfigUpdate = time.Now()
	l.config = newcfg // store raw not validate values
	newcfg.validate()
	return address, newcfg, nil
}

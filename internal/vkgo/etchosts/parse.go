// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package etchosts

import (
	"sync"
	"time"
)

const (
	defaultCachePeriod = time.Second
	defaultHostsFile   = "/etc/hosts"
)

var initDefaultInstanceOnce sync.Once
var defaultInstance *HostsCache

func initDefaultInstance() {
	initDefaultInstanceOnce.Do(func() {
		defaultInstance = NewHostsCache(defaultHostsFile, defaultCachePeriod)
	})
}

func Resolve(hostname string) string {
	initDefaultInstance()
	return defaultInstance.Resolve(hostname)
}

func OnParse(f callback) {
	initDefaultInstance()
	defaultInstance.OnParse(f)
}

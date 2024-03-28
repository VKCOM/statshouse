// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

const ServerTimingHeaderKey = "Server-Timing"

// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Server-Timing
// we always have duration
// we never have description
type ServerTimingHeader struct {
	Timings map[string]time.Duration
	mutex   sync.Mutex
	started time.Time
}

func (header *ServerTimingHeader) Report(name string, dur time.Duration) {
	header.mutex.Lock()
	defer header.mutex.Unlock()
	header.Timings[name] += dur
}

func (header *ServerTimingHeader) String() string {
	header.mutex.Lock()
	defer header.mutex.Unlock()
	header.Timings["total"] = time.Since(header.started)
	strs := make([]string, len(header.Timings))
	i := 0
	for name, dur := range header.Timings {
		strs[i] = name + ";dur=" + strconv.FormatInt(dur.Milliseconds(), 10)
		i += 1
	}
	return strings.Join(strs, ", ")
}

// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

const ServerTimingHeaderKey = "Server-Timing"

// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Server-Timing
// we always have duration
// we never have description
type ServerTimingHeader struct {
	Timings map[string][]time.Duration
	mutex   sync.Mutex
	started time.Time
}

func (header *ServerTimingHeader) Report(name string, dur time.Duration) {
	if header.Timings == nil {
		return
	}
	header.mutex.Lock()
	defer header.mutex.Unlock()
	if t, ok := header.Timings[name]; ok {
		header.Timings[name] = append(t, dur)
	} else {
		header.Timings[name] = []time.Duration{dur}
	}
}

func (header *ServerTimingHeader) String() string {
	if header.Timings == nil {
		return ""
	}
	header.mutex.Lock()
	defer header.mutex.Unlock()
	header.Timings["total"] = []time.Duration{time.Since(header.started)}

	n := 0
	for _, t := range header.Timings {
		n += len(t)
	}
	strs := make([]string, n)

	i := 0
	for name, durs := range header.Timings {
		if len(durs) == 1 {
			strs[i] = fmt.Sprintf("%s;dur=%d", name, durs[0].Milliseconds())
			i += 1
			continue
		}
		for j, dur := range durs {
			strs[i] = fmt.Sprintf("%s-%d;dur=%d", name, j, dur.Milliseconds())
			i += 1
		}
	}
	return strings.Join(strs, ", ")
}

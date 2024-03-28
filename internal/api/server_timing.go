// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"strconv"
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

type TimingBuilder struct {
	Started time.Time
	Name    string
	header  *ServerTimingHeader
}

func NewServerTimingHeader() *ServerTimingHeader {
	header := new(ServerTimingHeader)
	header.Timings = make(map[string]time.Duration)
	header.started = time.Now()
	return header
}

func (header *ServerTimingHeader) Report(name string, dur time.Duration) {
	header.mutex.Lock()
	defer header.mutex.Unlock()
	if previous, present := header.Timings[name]; present {
		header.Timings[name] = dur
	} else {
		header.Timings[name] = previous + dur
	}
}

func (header *ServerTimingHeader) Start(name string) TimingBuilder {
	return TimingBuilder{
		time.Now(),
		name,
		header,
	}
}

func (builder TimingBuilder) Stop() {
	if builder.header == nil {
		return
	}
	elapsed := time.Since(builder.Started)
	builder.header.Report(builder.Name, elapsed)
}

func (header *ServerTimingHeader) String() string {
	value := ""
	header.mutex.Lock()
	defer header.mutex.Unlock()
	header.Timings["total"] = time.Since(header.started)
	for name, dur := range header.Timings {
		value += name + ";dur=" + strconv.FormatInt(dur.Milliseconds(), 10) + ", "
	}
	return value
}

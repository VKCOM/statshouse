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
type ServerTimingHeader struct {
	Timings map[string]*timing
	mutex   sync.Mutex
	started time.Time
}

type timing struct {
	Duration *time.Duration
	Desc     *string
}

type TimingBuilder struct {
	Started time.Time
	Name    string
	Desc    *string
	header  *ServerTimingHeader
}

func NewServerTimingHeader() *ServerTimingHeader {
	header := new(ServerTimingHeader)
	header.Timings = make(map[string]*timing)
	header.started = time.Now()
	return header
}

func (header *ServerTimingHeader) Report(name string, dur time.Duration, desc *string) {
	header.mutex.Lock()
	defer header.mutex.Unlock()
	if previous := header.Timings[name]; previous == nil {
		header.Timings[name] = &timing{
			&dur,
			desc,
		}
	} else {
		// if we call Start/Stop twice for same name we save sum of durations, but keep desc from first call
		*previous.Duration += dur
	}
}

func (header *ServerTimingHeader) Start(name string, desc *string) TimingBuilder {
	return TimingBuilder{
		time.Now(),
		name,
		desc,
		header,
	}
}

func (builder TimingBuilder) Stop() {
	if builder.header == nil {
		return
	}
	elapsed := time.Since(builder.Started)
	builder.header.Report(builder.Name, elapsed, builder.Desc)
}

func (header *ServerTimingHeader) String() string {
	value := ""
	header.mutex.Lock()
	defer header.mutex.Unlock()
	total := time.Since(header.started)
	header.Timings["total"] = &timing{
		&total,
		nil,
	}
	for name, timing := range header.Timings {
		value += name
		if timing.Desc != nil {
			value += `;desc="` + *timing.Desc + `"`
		}
		if timing.Duration != nil {
			value += ";dur=" + strconv.FormatInt(timing.Duration.Milliseconds(), 10)
		}
		value += ", "
	}
	return value
}

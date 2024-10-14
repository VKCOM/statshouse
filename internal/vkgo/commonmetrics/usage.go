// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package commonmetrics

import (
	"fmt"
	"log"
	"time"

	"github.com/prometheus/procfs"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/vkgo/commonmetrics/internal/env"
)

var (
	memUsageMetricName = env.FullMetricName("common_usage_mem")
	cpuUsageMetricName = env.FullMetricName("common_usage_cpu")
)

var errorReported = false

type usage struct {
	lastUserTime   time.Duration
	lastSystemTime time.Duration
	proc           procfs.Proc
}

func (u *usage) stat() (procfs.ProcStat, error) {
	if u.lastUserTime == 0 && u.lastSystemTime == 0 {
		fs, err := procfs.NewDefaultFS()
		if err != nil {
			return procfs.ProcStat{}, fmt.Errorf("failed to create procfs: %w", err)
		}
		proc, err := fs.Self()
		if err != nil {
			return procfs.ProcStat{}, fmt.Errorf("failed to navigate to self in procfs: %w", err)
		}
		u.proc = proc
	}
	return u.proc.Stat()
}

func (u *usage) report(client *statshouse.Client) {
	stat, err := u.stat()
	var (
		rssBytes             int
		userTime, systemTime time.Duration
	)
	if err != nil {
		userTime = u.lastUserTime
		systemTime = u.lastSystemTime
		if !errorReported {
			errorReported = true
			log.Printf("[commonmetrics] reading /proc/self/stat failed; reporting 0 CPU and memory usage: %v", err)
		}
	} else {
		rssBytes = stat.ResidentMemory()
		userTime = statTimeToDuration(stat.UTime)
		systemTime = statTimeToDuration(stat.STime)
	}
	client.Value(memUsageMetricName, AttachBase(statshouse.Tags{}), float64(rssBytes))
	client.Value(cpuUsageMetricName, AttachBase(statshouse.Tags{4: CPUUser}), (userTime - u.lastUserTime).Seconds())
	client.Value(cpuUsageMetricName, AttachBase(statshouse.Tags{4: CPUSystem}), (systemTime - u.lastSystemTime).Seconds())
	u.lastUserTime = userTime
	u.lastSystemTime = systemTime
}

func statTimeToDuration(t uint) time.Duration {
	// Copied verbatim from internals of the procfs package:
	//
	// Originally, this USER_HZ value was dynamically retrieved via a sysconf call
	// which required cgo. However, that caused a lot of problems regarding
	// cross-compilation. Alternatives such as running a binary to determine the
	// value, or trying to derive it in some other way were all problematic.  After
	// much research it was determined that USER_HZ is actually hardcoded to 100 on
	// all Go-supported platforms as of the time of this writing. This is why we
	// decided to hardcode it here as well. It is not impossible that there could
	// be systems with exceptions, but they should be very exotic edge cases, and
	// in that case, the worst outcome will be two misreported metrics.
	//
	// See also the following discussions:
	//
	// - https://github.com/prometheus/node_exporter/issues/52
	// - https://github.com/prometheus/procfs/pull/2
	// - http://stackoverflow.com/questions/17410841/how-does-user-hz-solve-the-jiffy-scaling-issue
	const userHZ = 100
	const multiplierToMs = 1000 / userHZ

	ms := t * multiplierToMs
	return time.Duration(ms) * time.Millisecond
}

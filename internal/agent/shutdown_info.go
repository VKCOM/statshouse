// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
)

const shutdownInfoFileName = "shutdown_stats.tmp"

var globalStartTime = time.Now() // good enough for us

// actual metrics reported depend on contents of .tmp file
func ShutdownInfoReport(sh2 *Agent, componentTag int32, cacheDir string, startDiscCache time.Time) {
	si := tlstatshouse.ShutdownInfo{}
	if cacheDir != "" {
		fn := filepath.Join(cacheDir, shutdownInfoFileName)
		data, err := os.ReadFile(fn)
		if err != nil {
			log.Printf("error reading %q, no shutdown metrics will be written", fn)
		} else if _, err := si.ReadBoxed(data); err != nil {
			log.Printf("error parsing %q, no shutdown metrics will be written", fn)
		}
		_ = os.Remove(fn) // We do not want duplicates. If we crash before saving metrics, we better lose them.
	}
	finishShutdownTime := time.Unix(0, si.FinishShutdownTime)
	if dur := globalStartTime.Sub(finishShutdownTime); si.FinishShutdownTime > 0 && dur > 0 && dur < time.Hour {
		// arbitrary check that if start took more than 1 hour, this was not restart, and we do not want such case in our averages
		sh2.AddValueCounter(0, format.BuiltinMetricMetaRestartTimings,
			[]int32{0, componentTag, format.TagValueIDRestartTimingsPhaseInactive},
			dur.Seconds(), 1)
	}
	if dur := time.Duration(si.StopRecentSenders); dur > 0 {
		sh2.AddValueCounter(0, format.BuiltinMetricMetaRestartTimings,
			[]int32{0, componentTag, format.TagValueIDRestartTimingsPhaseStopRecentSenders},
			dur.Seconds(), 1)
	}
	if dur := time.Duration(si.StopReceivers); dur > 0 {
		sh2.AddValueCounter(0, format.BuiltinMetricMetaRestartTimings,
			[]int32{0, componentTag, format.TagValueIDRestartTimingsPhaseStopReceivers},
			dur.Seconds(), 1)
	}
	if dur := time.Duration(si.StopFlusher); dur > 0 {
		sh2.AddValueCounter(0, format.BuiltinMetricMetaRestartTimings,
			[]int32{0, componentTag, format.TagValueIDRestartTimingsPhaseStopFlusher},
			dur.Seconds(), 1)
	}
	if dur := time.Duration(si.StopFlushing); dur > 0 {
		sh2.AddValueCounter(0, format.BuiltinMetricMetaRestartTimings,
			[]int32{0, componentTag, format.TagValueIDRestartTimingsPhaseStopFlushing},
			dur.Seconds(), 1)
	}
	if dur := time.Duration(si.StopPreprocessor); dur > 0 {
		sh2.AddValueCounter(0, format.BuiltinMetricMetaRestartTimings,
			[]int32{0, componentTag, format.TagValueIDRestartTimingsPhaseStopPreprocessor},
			dur.Seconds(), 1)
	}
	if dur := time.Duration(si.StopInserters); dur > 0 {
		sh2.AddValueCounter(0, format.BuiltinMetricMetaRestartTimings,
			[]int32{0, componentTag, format.TagValueIDRestartTimingsPhaseStopInserters},
			dur.Seconds(), 1)
	}
	if dur := time.Duration(si.StopRPCServer); dur > 0 {
		sh2.AddValueCounter(0, format.BuiltinMetricMetaRestartTimings,
			[]int32{0, componentTag, format.TagValueIDRestartTimingsPhaseStopRPCServer},
			dur.Seconds(), 1)
	}
	if dur := time.Duration(si.SaveMappings); dur > 0 {
		sh2.AddValueCounter(0, format.BuiltinMetricMetaRestartTimings,
			[]int32{0, componentTag, format.TagValueIDRestartTimingsPhaseSaveMappings},
			dur.Seconds(), 1)
	}
	if dur := time.Duration(si.SaveJournal); dur > 0 {
		sh2.AddValueCounter(0, format.BuiltinMetricMetaRestartTimings,
			[]int32{0, componentTag, format.TagValueIDRestartTimingsPhaseSaveJournal},
			dur.Seconds(), 1)
	}
	finishLoadingTime := time.Now()
	if dur := startDiscCache.Sub(globalStartTime); dur > 0 {
		sh2.AddValueCounter(0, format.BuiltinMetricMetaRestartTimings,
			[]int32{0, componentTag, format.TagValueIDRestartTimingsPhaseStartDiskCache},
			dur.Seconds(), 1)
	}
	if dur := finishLoadingTime.Sub(startDiscCache); dur > 0 {
		sh2.AddValueCounter(0, format.BuiltinMetricMetaRestartTimings,
			[]int32{0, componentTag, format.TagValueIDRestartTimingsPhaseStartService},
			dur.Seconds(), 1)
	}

	startShutdownTime := time.Unix(0, si.StartShutdownTime)
	if dur := finishLoadingTime.Sub(startShutdownTime); si.StartShutdownTime > 0 && dur > 0 && dur < time.Hour {
		// arbitrary check that if start took more than 1 hour, this was not restart, and we do not want such case in our averages
		sh2.AddValueCounter(0, format.BuiltinMetricMetaRestartTimings,
			[]int32{0, componentTag, format.TagValueIDRestartTimingsPhaseTotal},
			dur.Seconds(), 1)
		log.Printf("restart finished in %v (since shutdown start time recorded by previous instance)", dur)
	} else {
		log.Printf("start finished in %v (since this main() launched)", time.Since(globalStartTime))
	}
}

func ShutdownInfoSave(cacheDir string, si tlstatshouse.ShutdownInfo) {
	if cacheDir != "" {
		_ = os.WriteFile(filepath.Join(cacheDir, shutdownInfoFileName), si.WriteBoxed(nil), os.ModePerm)
	}
}

func ShutdownInfoDuration(ct *time.Time) time.Duration {
	now := time.Now()
	dur := now.Sub(*ct)
	if dur <= 0 {
		dur = 1 // If some restart stage is very quick, we want to record it. But value 0 is not written to statshouse.
	}
	*ct = now
	return dur
}

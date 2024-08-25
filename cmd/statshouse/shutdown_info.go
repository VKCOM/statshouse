package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
)

func shutdownInfoReport(sh2 *agent.Agent, storageDir string, startDiscCache time.Time) {
	si := tlstatshouse.ShutdownInfo{}
	if storageDir != "" {
		fn := filepath.Join(storageDir, "shutdown.tl")
		data, err := os.ReadFile(fn)
		if err != nil {
			logErr.Printf("error reading %q, no shutdown metrics will be written", fn)
		} else if _, err := si.ReadBoxed(data); err != nil {
			logErr.Printf("error parsing %q, no shutdown metrics will be written", fn)
		}
	}
	finishShutdownTime := time.Unix(0, si.FinishShutdownTime)
	if dur := globalStartTime.Sub(finishShutdownTime); si.FinishShutdownTime > 0 && dur > 0 && dur < time.Hour {
		// arbitrary check that if start took more than 1 hour, this was not restart, and we do not want such case in our averages
		sh2.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDRestartTimings,
			Keys: [16]int32{0, format.TagValueIDComponentAgent, format.TagValueIDRestartTimingsPhaseInactive}},
			dur.Seconds(), 1, nil)
	}
	if dur := time.Duration(si.StopRecentSenders); dur > 0 {
		sh2.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDRestartTimings,
			Keys: [16]int32{0, format.TagValueIDComponentAgent, format.TagValueIDRestartTimingsPhaseStopRecentSenders}},
			dur.Seconds(), 1, nil)
	}
	if dur := time.Duration(si.StopReceivers); dur > 0 {
		sh2.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDRestartTimings,
			Keys: [16]int32{0, format.TagValueIDComponentAgent, format.TagValueIDRestartTimingsPhaseStopReceivers}},
			dur.Seconds(), 1, nil)
	}
	if dur := time.Duration(si.StopFlusher); dur > 0 {
		sh2.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDRestartTimings,
			Keys: [16]int32{0, format.TagValueIDComponentAgent, format.TagValueIDRestartTimingsPhaseStopFlusher}},
			dur.Seconds(), 1, nil)
	}
	if dur := time.Duration(si.StopFlushing); dur > 0 {
		sh2.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDRestartTimings,
			Keys: [16]int32{0, format.TagValueIDComponentAgent, format.TagValueIDRestartTimingsPhaseStopFlushing}},
			dur.Seconds(), 1, nil)
	}
	if dur := time.Duration(si.StopPreprocessor); dur > 0 {
		sh2.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDRestartTimings,
			Keys: [16]int32{0, format.TagValueIDComponentAgent, format.TagValueIDRestartTimingsPhaseStopPreprocessor}},
			dur.Seconds(), 1, nil)
	}
	finishLoadingTime := time.Now()
	if dur := startDiscCache.Sub(globalStartTime); dur > 0 {
		sh2.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDRestartTimings,
			Keys: [16]int32{0, format.TagValueIDComponentAgent, format.TagValueIDRestartTimingsPhaseStartDiskCache}},
			dur.Seconds(), 1, nil)
	}
	if dur := finishLoadingTime.Sub(startDiscCache); dur > 0 {
		sh2.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDRestartTimings,
			Keys: [16]int32{0, format.TagValueIDComponentAgent, format.TagValueIDRestartTimingsPhaseStartReceivers}},
			dur.Seconds(), 1, nil)
	}

	startShutdownTime := time.Unix(0, si.StartShutdownTime)
	if dur := finishLoadingTime.Sub(startShutdownTime); si.StartShutdownTime > 0 && dur > 0 && dur < time.Hour {
		// arbitrary check that if start took more than 1 hour, this was not restart, and we do not want such case in our averages
		sh2.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDRestartTimings,
			Keys: [16]int32{0, format.TagValueIDComponentAgent, format.TagValueIDRestartTimingsPhaseTotal}},
			dur.Seconds(), 1, nil)
		logOk.Printf("agent finished restart in %v", dur)
	} else {
		logOk.Printf("agent finished loading in %v", time.Since(globalStartTime))
	}
}

func shutdownInfoSave(storageDir string, si tlstatshouse.ShutdownInfo) {
	if storageDir != "" {
		_ = os.WriteFile(filepath.Join(argv.cacheDir, "shutdown.tl"), si.WriteBoxed(nil), os.ModePerm)
	}
}

func shutdownInfoDuration(ct *time.Time) time.Duration {
	now := time.Now()
	dur := now.Sub(*ct)
	if dur <= 0 {
		dur = 1 // If some restart stage is very quick, we want to record it. But value 0 is not written to statshouse.
	}
	*ct = now
	return dur
}

// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package commonmetrics

import (
	"sync"

	"github.com/VKCOM/statshouse-go"
	"github.com/VKCOM/statshouse/internal/vkgo/build"
)

const (
	HeartbeatStart    = "start"
	HeartbeatWarmup   = "warmup"
	HeartbeatRunning  = "running"
	HeartbeatShutdown = "shutdown"
)

const (
	LogTrace = "trace"
	LogDebug = "debug"
	LogInfo  = "info"
	LogWarn  = "warn"
	LogError = "error"
	LogPanic = "panic"
	LogFatal = "fatal"
)

const (
	ProtocolHTTP = "http"
	ProtocolRPC  = "rpc"
)

const (
	CPUUser   = "user"
	CPUSystem = "system"
)

type Base struct {
	Environment string
	Service     string
	Cluster     string
	DataCenter  string
}

var (
	base                 = Base{}
	regularMeasurementID int
	globalUsage          = &usage{}
)

var (
	startOnce sync.Once
	stopOnce  sync.Once
)

func AttachBase(tags statshouse.Tags) statshouse.Tags {
	AttachBaseS(tags[:])
	return tags
}

func AttachBaseS(tags []string) []string {
	tags[0] = base.Environment
	tags[1] = base.Service
	tags[2] = base.Cluster
	tags[3] = base.DataCenter
	return tags
}

func Start(b Base, toBeHashing ...hashingAim) {
	startOnce.Do(func() {
		if b.Service == "" {
			b.Service = build.AppName()
		}
		base = b
		if base.Environment == "" {
			base.Environment = parsers.get(Environment)()
		}
		if base.Service == "" {
			base.Service = parsers.get(Service)()
		}
		if base.Cluster == "" {
			base.Cluster = parsers.get(Cluster)()
		}
		if base.DataCenter == "" {
			base.DataCenter = parsers.get(DataCenter)()
		}

		if len(toBeHashing) != 0 {
			SetConfigHash(toBeHashing...)
		}

		uptimeMetrics.start()

		regularMeasurementID = statshouse.StartRegularMeasurement(func(client *statshouse.Client) {
			uptimeMetrics.heartbeat(client)
			globalUsage.report(client)
		})
	})
}

func Stop() {
	stopOnce.Do(func() {
		statshouse.StopRegularMeasurement(regularMeasurementID)
		uptimeMetrics.shutdown()
	})
}

type Method struct {
	Group string
	Name  string
}

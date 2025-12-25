// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
)

type Config struct {
	AggregatorAddresses []string
	// Sampling Algorithm
	SampleBudget         int // for all shards, in bytes
	OverrideSampleBudget int
	ShardSampleBudget    map[int]int // pre shard overrides, if not set buget is equal to SampleBudget
	HistoricWindow       uint
	MaxHistoricDiskSize  int64 // for all shards, in bytes
	SampleKeepSingle     bool
	SampleNamespaces     bool
	SampleGroups         bool
	SampleKeys           bool

	// How much strings (per key) is stored and sent to aggregator
	StringTopCapacity  int
	StringTopCountSend int

	// Liveness detector to switch between original and spare
	LivenessResponsesWindowLength    int
	LivenessResponsesWindowSuccesses int
	KeepAliveSuccessTimeout          time.Duration // LivenessResponsesWindowLength subsequent keepalives must takes < this

	SaveSecondsImmediately bool // If false, will only go to disk if first send fails
	SendMoreBytes          int
	StatsHouseEnv          string
	Cluster                string
	LegacyApplyValues      bool

	MappingCacheSize int64
	MappingCacheTTL  int

	// "remote write" was never used (so never tested) and was dropped
	RemoteWriteEnabled bool
	RemoteWriteAddr    string
	RemoteWritePath    string

	AutoCreate           bool
	DisableNoSampleAgent bool

	HardwareMetricResolution     int
	HardwareSlowMetricResolution int
}

func DefaultConfig() Config {
	return Config{
		SampleBudget:                     150000,
		HistoricWindow:                   6 * 3600, // TODO - after V3 tables dropped, change to 24 hours
		MaxHistoricDiskSize:              20 << 30, // enough for default SampleBudget per MaxHistoricWindow
		SampleNamespaces:                 false,
		SampleGroups:                     false,
		SampleKeys:                       false,
		StringTopCapacity:                100,
		StringTopCountSend:               20,
		LivenessResponsesWindowLength:    5,
		LivenessResponsesWindowSuccesses: 3,
		KeepAliveSuccessTimeout:          time.Second * 5, // aggregator puts keep-alive requests in a bucket most soon to be inserted, so this is larger than strictly required
		SaveSecondsImmediately:           false,
		SendMoreBytes:                    0,
		StatsHouseEnv:                    "production",

		MappingCacheSize: 100 << 20,
		MappingCacheTTL:  86400,

		RemoteWriteEnabled:           false,
		RemoteWriteAddr:              ":13380",
		RemoteWritePath:              "/write",
		AutoCreate:                   true,
		DisableNoSampleAgent:         false,
		HardwareMetricResolution:     5,
		HardwareSlowMetricResolution: 15,
	}
}

func (c *Config) setShardBudget(param string) error {
	parts := strings.Split(param, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid input format for --shard-sample-budget, expected {shard}:{budget}, got %s", param)
	}
	shard, err := strconv.Atoi(parts[0])
	if err != nil {
		return fmt.Errorf("invalid shard value in --shard-sample-budget, expected integer, got %s: %v", parts[0], err)
	}
	if shard < 1 {
		return fmt.Errorf("shard value must be >= 0, got %d", shard)
	}
	budget, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("invalid budget value in --shard-sample-budget, expected integer, got %s: %v", parts[1], err)
	}
	if budget < 1 {
		return fmt.Errorf("budget value must be >= 0, got %d", budget)
	}
	if c.ShardSampleBudget == nil {
		c.ShardSampleBudget = make(map[int]int)
	}
	c.ShardSampleBudget[shard] = budget
	return nil
}

func (c *Config) Bind(f *flag.FlagSet, d Config) {
	f.IntVar(&c.SampleBudget, "sample-budget", d.SampleBudget, "Statshouse will sample all buckets to contain max this number of bytes.")
	f.IntVar(&c.OverrideSampleBudget, "override-sample-budget", d.OverrideSampleBudget, "Overrides basic sample-budget.")
	f.Func("shard-sample-budget", "1:200 override budget for 1 shard with 200, shards start with 1", c.setShardBudget)
	f.UintVar(&c.HistoricWindow, "historic-window", d.HistoricWindow, "If aggregators are unavailable, buckets created outside this window will be discarded.")
	f.Int64Var(&c.MaxHistoricDiskSize, "max-disk-size", d.MaxHistoricDiskSize, "Statshouse will use no more than this amount of disk space for storing historic data.")
	var skipShards int // TODO - remove from config
	f.IntVar(&skipShards, "skip-shards", 0, "Deprecated, not used.")

	f.IntVar(&c.StringTopCapacity, "string-top-capacity", d.StringTopCapacity, "How many different strings per key is stored in string tops.")
	f.IntVar(&c.StringTopCountSend, "string-top-send", d.StringTopCountSend, "How many different strings per key is sent in string tops.")

	f.IntVar(&c.LivenessResponsesWindowLength, "liveness-window", d.LivenessResponsesWindowLength, "windows size (seconds) to use for liveness checks. Aggregator is live again if all keepalives in window are successes.")
	f.IntVar(&c.LivenessResponsesWindowSuccesses, "liveness-success", d.LivenessResponsesWindowSuccesses, "For liveness checks. Aggregator is dead if less responses in window are successes.")
	f.DurationVar(&c.KeepAliveSuccessTimeout, "keep-alive-timeout", d.KeepAliveSuccessTimeout, "For liveness checks. Successful keepalive must take less.")

	f.BoolVar(&c.SaveSecondsImmediately, "save-seconds-immediately", d.SaveSecondsImmediately, "Save data to disk as soon as second is ready. When false, data is saved after first unsuccessful send.")
	f.IntVar(&c.SendMoreBytes, "send-more-bytes", d.SendMoreBytes, "To test network without changing budgets.")
	f.StringVar(&c.StatsHouseEnv, "statshouse-env", d.StatsHouseEnv, "Fill key0 with this value in built-in statistics. 'production', 'staging1', 'staging2', 'staging3' values are allowed.")

	f.Int64Var(&c.MappingCacheSize, "mappings-cache-size", d.MappingCacheSize, "Mappings cache size both in memory and on disk.")
	f.IntVar(&c.MappingCacheTTL, "mappings-cache-ttl", d.MappingCacheTTL, "Mappings cache item TTL since last used.")

	f.BoolVar(&c.RemoteWriteEnabled, "remote-write-enabled", d.RemoteWriteEnabled, "Serve prometheus remote write endpoint (deprecated).")
	f.StringVar(&c.RemoteWriteAddr, "remote-write-addr", d.RemoteWriteAddr, "Prometheus remote write listen address (deprecated).")
	f.StringVar(&c.RemoteWritePath, "remote-write-path", d.RemoteWritePath, "Prometheus remote write path (deprecated).")

	f.BoolVar(&c.AutoCreate, "auto-create", d.AutoCreate, "Enable metric auto-create.")
	f.BoolVar(&c.DisableNoSampleAgent, "disable-nosample-agent", d.DisableNoSampleAgent, "Disable NoSampleAgent metric option.")
	f.BoolVar(&c.SampleKeepSingle, "sample-keep-single", d.SampleKeepSingle, "Statshouse won't sample single series.")
	f.BoolVar(&c.SampleNamespaces, "sample-namespaces", d.SampleNamespaces, "Statshouse will sample at namespace level.")
	f.BoolVar(&c.SampleGroups, "sample-groups", d.SampleGroups, "Statshouse will sample at group level.")
	f.BoolVar(&c.SampleKeys, "sample-keys", d.SampleKeys, "Statshouse will sample at key level.")
	f.BoolVar(&c.LegacyApplyValues, "legacy-apply-values", d.LegacyApplyValues, "Statshouse will sample at key level.")

	f.IntVar(&c.HardwareMetricResolution, "hardware-metric-resolution", d.HardwareMetricResolution, "Statshouse hardware metric resolution")
	f.IntVar(&c.HardwareSlowMetricResolution, "hardware-slow-metric-resolution", d.HardwareSlowMetricResolution, "Statshouse slow hardware metric resolution")
}

func (c *Config) updateFromRemoteDescription(description string) error {
	var f flag.FlagSet
	f.Usage = func() {} // don't print usage on unknown flags
	f.Init("", flag.ContinueOnError)
	c.Bind(&f, *c)
	s := strings.Split(description, "\n")
	for i := 0; i < len(s); i++ {
		t := strings.TrimSpace(s[i])
		if len(t) == 0 || strings.HasPrefix(t, "#") {
			continue
		}
		_ = f.Parse([]string{t})
	}
	return c.ValidateConfigSource()
}

func (c *Config) ValidateConfigSource() error {
	if c.SampleBudget < 1 {
		return fmt.Errorf("sample-budget (%d) must be >= 1", c.SampleBudget)
	}
	if c.OverrideSampleBudget > 0 {
		c.SampleBudget = c.OverrideSampleBudget
	}
	if c.HistoricWindow > data_model.MaxHistoricWindow {
		return fmt.Errorf("historic-window (%d) must be <= %d", c.HistoricWindow, data_model.MaxHistoricWindow)
	}

	if c.StringTopCapacity < data_model.MinStringTopCapacity {
		return fmt.Errorf("--string-top-capacity (%d) must be >= %d", c.StringTopCapacity, data_model.MinStringTopCapacity)
	}
	if c.StringTopCountSend < data_model.MinStringTopSend {
		return fmt.Errorf("--string-top-send (%d) must be >= %d", c.StringTopCountSend, data_model.MinStringTopSend)
	}

	if c.LivenessResponsesWindowLength < 1 {
		return fmt.Errorf("--liveness-window (%d) must be >= 1", c.LivenessResponsesWindowLength)
	}
	if c.LivenessResponsesWindowLength > data_model.MaxLivenessResponsesWindowLength {
		return fmt.Errorf("--liveness-window (%d) must be <= %d", c.LivenessResponsesWindowLength, data_model.MaxLivenessResponsesWindowLength)
	}
	if c.LivenessResponsesWindowSuccesses < 1 {
		return fmt.Errorf("--liveness-success (%d) must be >= 1", c.LivenessResponsesWindowSuccesses)
	}
	if c.LivenessResponsesWindowSuccesses > c.LivenessResponsesWindowLength {
		return fmt.Errorf("--liveness-success (%d) must be <= --liveness-window (%d)", c.LivenessResponsesWindowSuccesses, c.LivenessResponsesWindowLength)
	}
	if c.KeepAliveSuccessTimeout < time.Second {
		return fmt.Errorf("--keep-alive-timeout (%s) must be >= 1s", c.KeepAliveSuccessTimeout)
	}
	if format.AllowedResolution(c.HardwareMetricResolution) != c.HardwareMetricResolution {
		return fmt.Errorf("--hardware-metric-resolution (%d) but must be 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30 or 60", c.HardwareMetricResolution)
	}
	if format.AllowedResolution(c.HardwareSlowMetricResolution) != c.HardwareSlowMetricResolution {
		return fmt.Errorf("--hardware-slow-metric-resolution (%d) but must be 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30 or 60", c.HardwareSlowMetricResolution)
	}

	return nil
}

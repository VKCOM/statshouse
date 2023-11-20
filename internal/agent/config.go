// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
)

type Config struct {
	AggregatorAddresses []string
	// Sampling Algorithm
	SampleBudget        int   // for all shards, in bytes
	MaxHistoricDiskSize int64 // for all shards, in bytes

	// How much strings (per key) is stored and sent to aggregator
	StringTopCapacity  int
	StringTopCountSend int

	// Liveness detector to switch between original and spare
	LivenessResponsesWindowLength    int
	LivenessResponsesWindowSuccesses int
	KeepAliveSuccessTimeout          time.Duration // LivenessResponsesWindowLength subsequent keepalives must takes < this

	SaveSecondsImmediately bool // If false, will only go to disk if first send fails
	StatsHouseEnv          string
	Cluster                string
	SkipShards             int // if cluster is extended, first shard might be almost full, so we can skip them for some time.

	RemoteWriteEnabled bool
	RemoteWriteAddr    string
	RemoteWritePath    string

	AutoCreate bool
}

func DefaultConfig() Config {
	return Config{
		SampleBudget:                     150000,
		MaxHistoricDiskSize:              20 << 30, // enough for default SampleBudget per MaxHistoricWindow
		StringTopCapacity:                100,
		StringTopCountSend:               20,
		LivenessResponsesWindowLength:    5,
		LivenessResponsesWindowSuccesses: 3,
		KeepAliveSuccessTimeout:          time.Second * 5, // aggregator puts keep-alive requests in a bucket most soon to be inserted, so this is larger than strictly required
		SaveSecondsImmediately:           false,
		StatsHouseEnv:                    "production",
		RemoteWriteEnabled:               false,
		RemoteWriteAddr:                  ":13380",
		RemoteWritePath:                  "/write",
		AutoCreate:                       true,
	}
}

func (c *Config) Bind(f *flag.FlagSet, d Config) {
	f.IntVar(&c.SampleBudget, "sample-budget", d.SampleBudget, "Statshouse will sample all buckets to contain max this number of bytes.")
	f.Int64Var(&c.MaxHistoricDiskSize, "max-disk-size", d.MaxHistoricDiskSize, "Statshouse will use no more than this amount of disk space for storing historic data.")
	f.IntVar(&c.SkipShards, "skip-shards", d.SkipShards, "Skip first shards during sharding. When extending cluster, helps prevent filling disks of already full shards.")

	f.IntVar(&c.StringTopCapacity, "string-top-capacity", d.StringTopCapacity, "How many different strings per key is stored in string tops.")
	f.IntVar(&c.StringTopCountSend, "string-top-send", d.StringTopCountSend, "How many different strings per key is sent in string tops.")

	f.IntVar(&c.LivenessResponsesWindowLength, "liveness-window", d.LivenessResponsesWindowLength, "windows size (seconds) to use for liveness checks. Aggregator is live again if all keepalives in window are successes.")
	f.IntVar(&c.LivenessResponsesWindowSuccesses, "liveness-success", d.LivenessResponsesWindowSuccesses, "For liveness checks. Aggregator is dead if less responses in window are successes.")
	f.DurationVar(&c.KeepAliveSuccessTimeout, "keep-alive-timeout", d.KeepAliveSuccessTimeout, "For liveness checks. Successful keepalive must take less.")

	f.BoolVar(&c.SaveSecondsImmediately, "save-seconds-immediately", d.SaveSecondsImmediately, "Save data to disk as soon as second is ready. When false, data is saved after first unsuccessful send.")
	f.StringVar(&c.StatsHouseEnv, "statshouse-env", d.StatsHouseEnv, "Fill key0 with this value in built-in statistics. Only 'production' and 'staging' values are allowed.")

	f.BoolVar(&c.RemoteWriteEnabled, "remote-write-enabled", d.RemoteWriteEnabled, "Serve prometheus remote write endpoint.")
	f.StringVar(&c.RemoteWriteAddr, "remote-write-addr", d.RemoteWriteAddr, "Prometheus remote write listen address.")
	f.StringVar(&c.RemoteWritePath, "remote-write-path", d.RemoteWritePath, "Prometheus remote write path.")
	f.BoolVar(&c.AutoCreate, "auto-create", d.AutoCreate, "Enable metric auto-create.")
}

func (c *Config) updateFromRemoteDescription(description string) error {
	var f flag.FlagSet
	f.Init("", flag.ContinueOnError)
	c.Bind(&f, *c)
	s := strings.Split(description, "\n")
	for i := 0; i < len(s); {
		t := strings.TrimSpace(s[i])
		if len(t) == 0 || strings.HasPrefix(t, "#") {
			s = append(s[0:i], s[i+1:]...)
		} else {
			s[i] = t
			i++
		}
	}
	err := f.Parse(s)
	if err != nil {
		return err
	}
	return c.ValidateConfigSource()
}

func (c *Config) ValidateConfigSource() error {
	if c.SampleBudget < 1 {
		return fmt.Errorf("sample-budget (%d) must be >= 1", c.SampleBudget)
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

	return nil
}

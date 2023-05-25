// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
)

type Config struct {
	AggregatorAddresses []string
	// ShardReplica Sampling Algorithm
	SampleBudget        int   // for all shards, in bytes
	SampleGroups        bool  // use group weights. Experimental, will be turned on unconditionally later
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
	SkipFirstNShards       int // if cluster is extended, first shard might be almost full, so we can skip them for some time.

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
		SkipFirstNShards:                 -1,
		RemoteWriteEnabled:               false,
		RemoteWriteAddr:                  ":13380",
		RemoteWritePath:                  "/write",
		AutoCreate:                       true,
	}
}

func hasKeyGetSuffix(line string, prefix string) (string, bool) {
	if len(line) >= 2 && line[0] == '-' && line[1] == '-' { // transform -- into -
		line = line[1:]
	}
	if strings.HasPrefix(line, prefix) {
		return line[len(prefix):], true
	}
	return "", false
}

func parseIntLine(line string, str string, value *int) error {
	i, err := strconv.Atoi(str)
	if err != nil {
		return fmt.Errorf("error parsing %s: %w", line, err)
	}
	*value = i
	return nil
}

func parseInt64Line(line string, str string, value *int64) error {
	i, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing %s: %w", line, err)
	}
	*value = int64(i)
	return nil
}

func parseDurationLine(line string, str string, value *time.Duration) error {
	i, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing %s: %w", line, err)
	}
	*value = time.Duration(i)
	return nil
}

func parseBoolLine(line string, str string, value *bool) error {
	if str != "true" && str != "false" {
		return fmt.Errorf("error parsing %s: must be true or false", line)
	}
	*value = str == "true"
	return nil
}

func (c *Config) updateFromRemoteDescription(description string) error {
	for _, line := range strings.Split(description, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// all non-empty non-comment lines must have valid settings
		if suffix, ok := hasKeyGetSuffix(line, "-sample-budget="); ok {
			if err := parseIntLine(line, suffix, &c.SampleBudget); err != nil {
				return err
			}
			continue
		}
		if suffix, ok := hasKeyGetSuffix(line, "-sample-groups="); ok {
			if err := parseBoolLine(line, suffix, &c.SampleGroups); err != nil {
				return err
			}
			continue
		}
		if suffix, ok := hasKeyGetSuffix(line, "-max-disk-size="); ok {
			if err := parseInt64Line(line, suffix, &c.MaxHistoricDiskSize); err != nil {
				return err
			}
			continue
		}
		if suffix, ok := hasKeyGetSuffix(line, "-skip-shards="); ok {
			if err := parseIntLine(line, suffix, &c.SkipFirstNShards); err != nil {
				return err
			}
			continue
		}
		if suffix, ok := hasKeyGetSuffix(line, "-string-top-capacity="); ok {
			if err := parseIntLine(line, suffix, &c.StringTopCapacity); err != nil {
				return err
			}
			continue
		}
		if suffix, ok := hasKeyGetSuffix(line, "-string-top-send="); ok {
			if err := parseIntLine(line, suffix, &c.StringTopCountSend); err != nil {
				return err
			}
			continue
		}
		if suffix, ok := hasKeyGetSuffix(line, "-liveness-window="); ok {
			if err := parseIntLine(line, suffix, &c.LivenessResponsesWindowLength); err != nil {
				return err
			}
			continue
		}
		if suffix, ok := hasKeyGetSuffix(line, "-liveness-success="); ok {
			if err := parseIntLine(line, suffix, &c.LivenessResponsesWindowSuccesses); err != nil {
				return err
			}
			continue
		}
		if suffix, ok := hasKeyGetSuffix(line, "-keep-alive-timeou="); ok {
			if err := parseDurationLine(line, suffix, &c.KeepAliveSuccessTimeout); err != nil {
				return err
			}
			continue
		}
		if suffix, ok := hasKeyGetSuffix(line, "-save-seconds-immediately="); ok {
			if err := parseBoolLine(line, suffix, &c.SaveSecondsImmediately); err != nil {
				return err
			}
			continue
		}
		return fmt.Errorf("error parsing %s: options not supported", line)
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

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/VKCOM/statshouse/internal/data_model"
)

type ConfigAggregatorRemote struct {
	InsertBudget         int         // for single replica, in bytes per contributor, when many contributors
	ShardInsertBudget    map[int]int // pre shard overrides, if not set buget is equal to InsertBudget
	StringTopCountInsert int
	SampleNamespaces     bool
	SampleGroups         bool
	SampleKeys           bool
	DenyOldAgents        bool
	MirrorChWrite        bool
	WriteToV3First       bool
	V2InsertSettings     string
	V3InsertSettings     string
	MappingCacheSize     int64
	MappingCacheTTL      int
	MapStringTop         bool
	BufferedInsertAgeSec int // age in seconds of data that should be sent to buffer table

	configTagsMapper2
}

type ConfigAggregator struct {
	ShortWindow        int
	RecentInserters    int
	HistoricInserters  int
	InsertHistoricWhen int

	KHAddr         string
	KHUser         string
	KHPassword     string
	KHPasswordFile string

	CardinalityWindow int
	MaxCardinality    int

	ConfigAggregatorRemote

	SimulateRandomErrors float64

	MetadataNet     string
	MetadataAddr    string
	MetadataActorID int64

	Cluster             string
	PreviousNumShards   int // if agents come with this # of shards, do not error as misconfiguration
	ShardByMetricShards int
	ExternalPort        string

	LocalReplica int

	AutoCreate                 bool
	AutoCreateDefaultNamespace bool
	DisableRemoteConfig        bool
}

func DefaultConfigAggregator() ConfigAggregator {
	return ConfigAggregator{
		ShortWindow:          data_model.MaxShortWindow,
		RecentInserters:      4,
		HistoricInserters:    1,
		InsertHistoricWhen:   2,
		CardinalityWindow:    3600,
		MaxCardinality:       50000, // will be divided by NumShardReplicas on each aggregator
		SimulateRandomErrors: 0,
		Cluster:              "statlogs2",
		MetadataNet:          "tcp4",
		MetadataAddr:         "127.0.0.1:2442",

		ConfigAggregatorRemote: ConfigAggregatorRemote{
			InsertBudget:         400,
			StringTopCountInsert: 20,
			SampleNamespaces:     false,
			SampleGroups:         false,
			SampleKeys:           false,
			DenyOldAgents:        true,
			MirrorChWrite:        true,
			WriteToV3First:       false,
			MappingCacheSize:     1 << 30,
			MappingCacheTTL:      86400 * 7,
			MapStringTop:         false, // disabled by default because API doesn't support it yet

			configTagsMapper2: configTagsMapper2{
				MaxUnknownTagsInBucket:    1024,
				MaxCreateTagsPerIteration: 128,
				MaxLoadTagsPerIteration:   128,
				TagHitsToCreate:           10,
				MaxUnknownTagsToKeep:      1_000_000,
				MaxSendTagsToAgent:        256,
			},
		},
	}
}

func (c *ConfigAggregatorRemote) setShardBudget(param string) error {
	parts := strings.Split(param, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid input format for --shard-insert-budget, expected {shard}:{budget}, got %s", param)
	}
	shard, err := strconv.Atoi(parts[0])
	if err != nil {
		return fmt.Errorf("invalid shard value in --shard-insert-budget, expected integer, got %s: %v", parts[0], err)
	}
	budget, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("invalid budget value in --shard-insert-budget, expected integer, got %s: %v", parts[1], err)
	}
	if c.ShardInsertBudget == nil {
		c.ShardInsertBudget = make(map[int]int)
	}
	c.ShardInsertBudget[shard] = budget
	return nil
}

func (c *ConfigAggregatorRemote) Bind(f *flag.FlagSet, d ConfigAggregatorRemote, legacyVerb bool) {
	f.IntVar(&c.InsertBudget, "insert-budget", d.InsertBudget, "Aggregator will sample data before inserting into clickhouse. Bytes per contributor when # >> 100.")
	f.Func("shard-insert-budget", "1:200 override budget for 1 shard with 200, shards start with 1", c.setShardBudget)
	f.IntVar(&c.StringTopCountInsert, "string-top-insert", d.StringTopCountInsert, "How many different strings per key is inserted by aggregator in string tops.")
	if !legacyVerb {
		f.BoolVar(&c.SampleNamespaces, "sample-namespaces", d.SampleNamespaces, "Statshouse will sample at namespace level.")
		f.BoolVar(&c.SampleGroups, "sample-groups", d.SampleGroups, "Statshouse will sample at group level.")
		f.BoolVar(&c.SampleKeys, "sample-keys", d.SampleKeys, "Statshouse will sample at key level.")
		f.BoolVar(&c.DenyOldAgents, "deny-old-agents", d.DenyOldAgents, "Statshouse will ignore data from outdated agents")
		f.BoolVar(&c.MirrorChWrite, "mirror-ch-writes", d.MirrorChWrite, "Write metrics into both v3 and v2 tables")
		f.BoolVar(&c.WriteToV3First, "write-to-v3-first", d.WriteToV3First, "Write metrics into v3 table first")
		f.StringVar(&c.V2InsertSettings, "v2-insert-settings", d.V2InsertSettings, "Settings when inserting into v2 table")
		f.StringVar(&c.V3InsertSettings, "v3-insert-settings", d.V3InsertSettings, "Settings when inserting into v2 table")
		f.Int64Var(&c.MappingCacheSize, "mappings-cache-size-agg", d.MappingCacheSize, "Mappings cache size both in memory and on disk for aggregator.")
		f.IntVar(&c.MappingCacheTTL, "mappings-cache-ttl-agg", d.MappingCacheTTL, "Mappings cache item TTL since last used for aggregator.")
		f.BoolVar(&c.MapStringTop, "map-string-top", d.MapStringTop, "Map string top")
		f.IntVar(&c.BufferedInsertAgeSec, "buffered-insert-age-sec", d.BufferedInsertAgeSec, "Age in seconds of data that should be inserted via buffer table")

		f.IntVar(&c.MaxUnknownTagsInBucket, "mapping-queue-max-unknown-tags-in-bucket", d.MaxUnknownTagsInBucket, "Max unknown tags per bucket to add to mapping queue.")
		f.IntVar(&c.MaxCreateTagsPerIteration, "mapping-queue-create-tags-per-iteration", d.MaxCreateTagsPerIteration, "Mapping queue will create no more tags per iteration (roughly second).")
		f.IntVar(&c.MaxLoadTagsPerIteration, "mapping-queue-load-tags-per-iteration", d.MaxLoadTagsPerIteration, "Mapping queue will load no more tags per iteration (roughly second).")
		f.IntVar(&c.TagHitsToCreate, "mapping-queue-hits-to-create", d.TagHitsToCreate, "Tag mapping will be created if it is used in so many different seconds.")
		f.IntVar(&c.MaxUnknownTagsToKeep, "mapping-queue-max-unknown-tags-to-keep", d.MaxUnknownTagsToKeep, "Mapping queue will remember and collect hits on so many different strings.")
		f.IntVar(&c.MaxSendTagsToAgent, "mapping-queue-max-send-tags-to-agent", d.MaxUnknownTagsInBucket, "Max tags to send in response to agent.")
	}
}

func ValidateConfigAggregator(c ConfigAggregator) error {
	if c.ShortWindow > data_model.MaxShortWindow {
		return fmt.Errorf("short-window (%d) cannot be > %d", c.ShortWindow, data_model.MaxShortWindow)
	}
	if c.ShortWindow < 2 {
		return fmt.Errorf("short-window (%d) cannot be < 2", c.ShortWindow)
	}

	if c.CardinalityWindow < data_model.MinCardinalityWindow {
		return fmt.Errorf("--cardinality-window (%d) must be >= %d", c.CardinalityWindow, data_model.MinCardinalityWindow)
	}
	if c.MaxCardinality < data_model.MinMaxCardinality {
		return fmt.Errorf("--max-cardinality (%d) must be >= %d", c.MaxCardinality, data_model.MinMaxCardinality)
	}

	if c.InsertHistoricWhen < 1 {
		return fmt.Errorf("--insert-historic-when (%d) must be >= 1", c.InsertHistoricWhen)
	}
	if c.RecentInserters < 1 {
		return fmt.Errorf("--recent-inserters (%d) must be >= 1", c.RecentInserters)
	}
	if c.HistoricInserters < 1 {
		return fmt.Errorf("--historic-inserters (%d) must be >= 1", c.HistoricInserters)
	}
	if c.HistoricInserters > 4 { // Otherwise batching during historic inserts will become too small
		return fmt.Errorf("--historic-inserters (%d) must be <= 4", c.HistoricInserters)
	}

	return c.ConfigAggregatorRemote.Validate()
}

func (c *ConfigAggregatorRemote) Validate() error {
	if c.InsertBudget < 1 {
		return fmt.Errorf("insert-budget (%d) must be >= 1", c.InsertBudget)
	}
	if c.StringTopCountInsert < data_model.MinStringTopInsert {
		return fmt.Errorf("--string-top-insert (%d) must be >= %d", c.StringTopCountInsert, data_model.MinStringTopInsert)
	}

	return nil
}

func (c *ConfigAggregatorRemote) updateFromRemoteDescription(description string) error {
	var f flag.FlagSet
	f.Init("", flag.ContinueOnError)
	c.Bind(&f, *c, false)
	var s []string
	for _, v := range strings.Split(description, "\n") {
		v = strings.TrimSpace(v)
		if len(v) != 0 && !strings.HasPrefix(v, "#") {
			s = append(s, v)
		}
	}
	err := f.Parse(s)
	if err != nil {
		return err
	}
	return c.Validate()
}

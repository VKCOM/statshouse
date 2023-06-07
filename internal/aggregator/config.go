// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"fmt"

	"github.com/vkcom/statshouse/internal/data_model"
)

type ConfigAggregator struct {
	ShortWindow        int
	RecentInserters    int
	HistoricInserters  int
	InsertHistoricWhen int

	KHAddr string

	InsertBudget    int // for single replica, in bytes per contributor, when many contributors
	InsertBudget100 int // for single replica, in bytes per contributor, when 100 contributors

	CardinalityWindow int
	MaxCardinality    int

	StringTopCountInsert int

	SimulateRandomErrors float64

	MetadataNet     string
	MetadataAddr    string
	MetadataActorID uint64

	Cluster           string
	PreviousNumShards int
	ExternalPort      string

	AutoCreate bool
}

func DefaultConfigAggregator() ConfigAggregator {
	return ConfigAggregator{
		ShortWindow:          data_model.MaxShortWindow,
		RecentInserters:      4,
		HistoricInserters:    1,
		InsertHistoricWhen:   2,
		InsertBudget:         400,
		InsertBudget100:      2500,
		CardinalityWindow:    3600,
		MaxCardinality:       50000, // will be divided by NumShardReplicas on each aggregator
		StringTopCountInsert: 20,
		SimulateRandomErrors: 0,
		Cluster:              "statlogs2",
		MetadataNet:          "tcp4",
		MetadataAddr:         "127.0.0.1:2442",
	}
}

func ValidateConfigAggregator(c ConfigAggregator) error {
	if c.ShortWindow > data_model.MaxShortWindow {
		return fmt.Errorf("short-window (%d) cannot be > %d", c.ShortWindow, data_model.MaxShortWindow)
	}
	if c.ShortWindow < 2 {
		return fmt.Errorf("short-window (%d) cannot be < 2", c.ShortWindow)
	}

	if c.InsertBudget < 1 {
		return fmt.Errorf("insert-budget (%d) must be >= 1", c.InsertBudget)
	}
	if c.InsertBudget100 < 1 {
		return fmt.Errorf("insert-budget-100 (%d) must be >= 1", c.InsertBudget100)
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

	if c.StringTopCountInsert < data_model.MinStringTopInsert {
		return fmt.Errorf("--string-top-insert (%d) must be >= %d", c.StringTopCountInsert, data_model.MinStringTopInsert)
	}

	return nil
}

// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package dac

import (
	"github.com/spf13/pflag"
)

type Options struct {
	V1Addrs             []string
	v1Debug             bool
	v1MaxConns          int
	v1Password          string
	v1User              string
	V2Addrs             []string
	v2Debug             bool
	v2MaxLightFastConns int
	v2MaxHeavyFastConns int
	v2MaxHeavySlowConns int
	v2MaxLightSlowConns int
	v2Password          string
	v2User              string
}

func (argv *Options) Bind(f *pflag.FlagSet) {
	var chMaxQueries int // not used any more, TODO - remove?
	pflag.IntVar(&chMaxQueries, "clickhouse-max-queries", 32, "maximum number of concurrent ClickHouse queries")
	pflag.StringSliceVar(&argv.V1Addrs, "clickhouse-v1-addrs", nil, "comma-separated list of ClickHouse-v1 addresses")
	pflag.BoolVar(&argv.v1Debug, "clickhouse-v1-debug", false, "ClickHouse-v1 debug mode")
	pflag.IntVar(&argv.v1MaxConns, "clickhouse-v1-max-conns", 16, "maximum number of ClickHouse-v1 connections (fast and slow)")
	pflag.StringVar(&argv.v1Password, "clickhouse-v1-password", "", "ClickHouse-v1 password")
	pflag.StringVar(&argv.v1User, "clickhouse-v1-user", "", "ClickHouse-v1 user")
	pflag.StringSliceVar(&argv.V2Addrs, "clickhouse-v2-addrs", nil, "comma-separated list of ClickHouse-v2 addresses")
	pflag.BoolVar(&argv.v2Debug, "clickhouse-v2-debug", false, "ClickHouse-v2 debug mode")
	pflag.IntVar(&argv.v2MaxLightFastConns, "clickhouse-v2-max-conns", 24, "maximum number of ClickHouse-v2 connections (light fast)")
	pflag.IntVar(&argv.v2MaxLightSlowConns, "clickhouse-v2-max-light-slow-conns", 12, "maximum number of ClickHouse-v2 connections (light slow)")
	pflag.IntVar(&argv.v2MaxHeavyFastConns, "clickhouse-v2-max-heavy-conns", 5, "maximum number of ClickHouse-v2 connections (heavy fast)")
	pflag.IntVar(&argv.v2MaxHeavySlowConns, "clickhouse-v2-max-heavy-slow-conns", 1, "maximum number of ClickHouse-v2 connections (heavy slow)")

	pflag.StringVar(&argv.v2Password, "clickhouse-v2-password", "", "ClickHouse-v2 password")
	pflag.StringVar(&argv.v2User, "clickhouse-v2-user", "", "ClickHouse-v2 user")
}

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package util

import (
	"context"
	"fmt"
	"time"
	_ "unsafe" // to access clickhouse.bind

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/atomic"
	"go.uber.org/multierr"

	"github.com/vkcom/statshouse/internal/vkgo/semaphore"
)

type connPool struct {
	next  atomic.Int64
	conns []clickhouse.Conn
	sem   *semaphore.Weighted
}

type ClickHouse struct {
	pools [4]connPool
}

const (
	fastLight = 0
	fastHeavy = 1
	slowLight = 2
	slowHeavy = 3 // fix Close after adding new modes
)

func OpenClickHouse(fastSlowMaxConns, lightHeavyMaxConns int, addrs []string, user string, password string, debug bool, dialTimeout time.Duration) (*ClickHouse, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("at least one ClickHouse address must be specified")
	}

	open := func(a []string) (clickhouse.Conn, error) { // to capture all vars
		return clickhouse.Open(&clickhouse.Options{
			Addr: a,
			Auth: clickhouse.Auth{
				Username: user,
				Password: password,
			},
			Debug: debug,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			DialTimeout:      dialTimeout,
			MaxOpenConns:     fastSlowMaxConns,
			ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		})
	}

	result := &ClickHouse{}
	result.pools[fastLight].sem = semaphore.NewWeighted(int64(fastSlowMaxConns))
	result.pools[fastHeavy].sem = semaphore.NewWeighted(int64(lightHeavyMaxConns))
	result.pools[slowLight].sem = semaphore.NewWeighted(int64(fastSlowMaxConns))
	result.pools[slowHeavy].sem = semaphore.NewWeighted(int64(lightHeavyMaxConns))

	for i := range addrs {
		// clickhouse uses the first accessible address
		// we could use better permutation, but the simple one is good enough
		a := append(append(make([]string, 0), addrs[i:]...), addrs[:i]...)
		fastL, err := open(a)
		if err != nil {
			return nil, fmt.Errorf("failed to open ClickHouse database: %w", err)
		}
		fastH, err := open(a)
		if err != nil {
			return nil, fmt.Errorf("failed to open ClickHouse database: %w", err)
		}
		slowL, err := open(a)
		if err != nil {
			return nil, fmt.Errorf("failed to open ClickHouse database: %w", err)
		}
		slowH, err := open(a)
		if err != nil {
			return nil, fmt.Errorf("failed to open ClickHouse database: %w", err)
		}
		result.pools[fastLight].conns = append(result.pools[fastLight].conns, fastL)
		result.pools[fastHeavy].conns = append(result.pools[fastHeavy].conns, fastH)
		result.pools[slowLight].conns = append(result.pools[slowLight].conns, slowL)
		result.pools[slowHeavy].conns = append(result.pools[slowHeavy].conns, slowH)
	}

	return result, nil
}

func (ch *ClickHouse) Close() error {
	var retErr error
	for i := 0; i <= slowHeavy; i++ {
		for _, conn := range ch.pools[i].conns {
			err := conn.Close()
			if err != nil {
				retErr = multierr.Append(retErr, err)
			}
		}
	}

	return retErr
}

func (ch *ClickHouse) SemaphoreCountSlowLight() int64 {
	cur, _ := ch.pools[slowLight].sem.Observe()
	return cur
}

func (ch *ClickHouse) SemaphoreCountSlowHeavy() int64 {
	cur, _ := ch.pools[slowHeavy].sem.Observe()
	return cur
}

func (ch *ClickHouse) SemaphoreCountFastLight() int64 {
	cur, _ := ch.pools[fastLight].sem.Observe()
	return cur
}

func (ch *ClickHouse) SemaphoreCountFastHeavy() int64 {
	cur, _ := ch.pools[fastHeavy].sem.Observe()
	return cur
}

func queryKind(isFast, isLight bool) int {
	if isFast {
		if isLight {
			return fastLight
		}
		return fastHeavy
	}
	if isLight {
		return slowLight
	}
	return slowHeavy
}

func (ch *ClickHouse) Select(isFast, isLight bool, ctx context.Context, dest interface{}, query string, args ...interface{}) (clickhouse.ProfileInfo, error) {
	var profile clickhouse.ProfileInfo
	ctx = clickhouse.Context(ctx, clickhouse.WithProfileInfo(func(info *clickhouse.ProfileInfo) {
		profile = *info
	}))
	idx := queryKind(isFast, isLight)
	conns := ch.pools[idx].conns
	sem := ch.pools[idx].sem
	next := &ch.pools[idx].next
	err := sem.Acquire(ctx, 1)
	if err != nil {
		return profile, err
	}
	defer sem.Release(1)
	cid := int(next.Inc()) % len(conns)
	conn := conns[cid]
	return profile, conn.Select(ctx, dest, query, args...)
}

func BindQuery(query string, args ...any) (string, error) {
	return clickHouseBind(time.UTC, query, args...)
}

//go:linkname clickHouseBind github.com/ClickHouse/clickhouse-go/v2.bind
func clickHouseBind(tz *time.Location, query string, args ...interface{}) (string, error)

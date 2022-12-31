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

	"github.com/vkcom/statshouse/internal/vkgo/semaphore"
)

type ClickHouse struct {
	fastNext atomic.Int64
	slowNext atomic.Int64

	fast []clickhouse.Conn
	slow []clickhouse.Conn

	fastSem *semaphore.Weighted
	slowSem *semaphore.Weighted
}

func OpenClickHouse(maxConns int, addrs []string, user string, password string, debug bool, dialTimeout time.Duration) (*ClickHouse, error) {
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
			MaxOpenConns:     maxConns,
			ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		})
	}

	result := &ClickHouse{
		fastSem: semaphore.NewWeighted(int64(maxConns)),
		slowSem: semaphore.NewWeighted(int64(maxConns)),
	}

	for i := range addrs {
		// clickhouse uses the first accessible address
		// we could use better permutation, but the simple one is good enough
		a := append(append(make([]string, 0), addrs[i:]...), addrs[:i]...)
		slow, err := open(a)
		if err != nil {
			return nil, fmt.Errorf("failed to open ClickHouse database: %w", err)
		}
		fast, err := open(a)
		if err != nil {
			return nil, fmt.Errorf("failed to open ClickHouse database: %w", err)
		}
		result.slow = append(result.slow, slow)
		result.fast = append(result.fast, fast)
	}

	return result, nil
}

func (ch *ClickHouse) Close() error {
	var retErr error
	for _, c := range ch.fast {
		if err := c.Close(); err != nil {
			retErr = err
		}
	}
	for _, c := range ch.slow {
		if err := c.Close(); err != nil {
			retErr = err
		}
	}

	return retErr
}

func (ch *ClickHouse) SemaphoreCountSlow() int64 {
	cur, _ := ch.slowSem.Observe()
	return cur
}

func (ch *ClickHouse) SemaphoreCountFast() int64 {
	cur, _ := ch.fastSem.Observe()
	return cur
}

func (ch *ClickHouse) Select(isFast bool, ctx context.Context, dest interface{}, query string, args ...interface{}) (clickhouse.ProfileInfo, error) {
	var profile clickhouse.ProfileInfo
	ctx = clickhouse.Context(ctx, clickhouse.WithProfileInfo(func(info *clickhouse.ProfileInfo) {
		profile = *info
	}))
	conns := ch.slow
	sem := ch.slowSem
	next := &ch.slowNext
	if isFast {
		conns = ch.fast
		sem = ch.fastSem
		next = &ch.fastNext
	}
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

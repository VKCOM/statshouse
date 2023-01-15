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

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
	_ "github.com/ClickHouse/clickhouse-go/v2" // to access clickhouse.bind
	"github.com/vkcom/statshouse/internal/vkgo/semaphore"
	"pgregory.net/rand"
)

type connPool struct {
	rnd     *rand.Rand
	servers []*chpool.Pool
	sem     *semaphore.Weighted
}

type ClickHouse struct {
	pools [4]*connPool
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

	result := &ClickHouse{[4]*connPool{
		{rand.New(), make([]*chpool.Pool, 0, len(addrs)), semaphore.NewWeighted(int64(fastSlowMaxConns))},   // fastLight
		{rand.New(), make([]*chpool.Pool, 0, len(addrs)), semaphore.NewWeighted(int64(lightHeavyMaxConns))}, // fastHeavy
		{rand.New(), make([]*chpool.Pool, 0, len(addrs)), semaphore.NewWeighted(int64(fastSlowMaxConns))},   // slowLight
		{rand.New(), make([]*chpool.Pool, 0, len(addrs)), semaphore.NewWeighted(int64(lightHeavyMaxConns))}, // slowHeavy
	}}
	for _, addr := range addrs {
		for _, pool := range result.pools {
			server, err := chpool.Dial(context.Background(), chpool.Options{
				MaxConns: int32(fastSlowMaxConns),
				ClientOptions: ch.Options{
					Address:          addr,
					User:             user,
					Password:         password,
					Compression:      ch.CompressionLZ4,
					DialTimeout:      dialTimeout,
					HandshakeTimeout: 5 * time.Second,
				}})
			if err != nil {
				result.Close()
				return nil, err
			}
			pool.servers = append(pool.servers, server)
		}
	}

	return result, nil
}

func (ch *ClickHouse) Close() {
	for _, a := range ch.pools {
		for _, b := range a.servers {
			b.Close()
		}
	}
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

func (ch *ClickHouse) Select(ctx context.Context, isFast, isLight bool, query ch.Query) (profile proto.Profile, err error) {
	query.OnProfile = func(_ context.Context, p proto.Profile) error {
		profile = p
		return nil
	}
	pool := ch.pools[queryKind(isFast, isLight)]
	servers := append(make([]*chpool.Pool, 0, len(pool.servers)), pool.servers...)
	for safetyCounter := 0; safetyCounter < len(pool.servers); safetyCounter++ {
		var i int
		i, err = pickRandomServer(servers, pool.rnd)
		if err != nil {
			return profile, err
		}
		err = pool.sem.Acquire(ctx, 1)
		if err != nil {
			return profile, err
		}
		err = servers[i].Do(ctx, query)
		pool.sem.Release(1)
		if err == nil {
			return // succeeded
		}
		if ctx.Err() != nil {
			return // failed
		}
		// keep searching alive server
		servers = append(servers[:i], servers[i+1:]...)
	}
	return profile, fmt.Errorf("all ClickHouse servers are dead")
}

func pickRandomServer(s []*chpool.Pool, r *rand.Rand) (int, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("all ClickHouse servers are dead")
	}
	if len(s) == 1 {
		return 0, nil
	}
	i1 := r.Intn(len(s))
	i2 := r.Intn(len(s) - 1)
	if i2 >= i1 {
		i2++
	}
	if s[i1].Stat().AcquiredResources() < s[i2].Stat().AcquiredResources() {
		return i1, nil
	} else {
		return i2, nil
	}
}

func BindQuery(query string, args ...any) (string, error) {
	return clickHouseBind(time.UTC, query, args...)
}

//go:linkname clickHouseBind github.com/ClickHouse/clickhouse-go/v2.bind
func clickHouseBind(tz *time.Location, query string, args ...interface{}) (string, error)

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package util

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
	_ "unsafe" // to access clickhouse.bind

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
	_ "github.com/ClickHouse/clickhouse-go/v2" // to access clickhouse.bind
	"pgregory.net/rand"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/util/queue"
)

type connPool struct {
	rnd     *rand.Rand
	servers []*chpool.Pool
	sem     *queue.Queue

	userActive map[string]int
	mx         sync.Mutex
	userWait   map[string]int
	waitMx     sync.Mutex
}

type ClickHouse struct {
	pools [4]*connPool
}

type QueryMetaInto struct {
	IsFast  bool
	IsLight bool
	User    string
	Metric  int32
	Table   string
	Kind    string
}

type QueryHandleInfo struct {
	Duration time.Duration
	Profile  proto.Profile
}

type ChConnOptions struct {
	Addrs             []string
	User              string
	Password          string
	DialTimeout       time.Duration
	FastLightMaxConns int
	FastHeavyMaxConns int
	SlowLightMaxConns int
	SlowHeavyMaxConns int
}

const (
	fastLight = 0
	fastHeavy = 1
	slowLight = 2
	slowHeavy = 3 // fix Close after adding new modes
)

func OpenClickHouse(opt ChConnOptions) (*ClickHouse, error) {
	if len(opt.Addrs) == 0 {
		return nil, fmt.Errorf("at least one ClickHouse address must be specified")
	}

	result := &ClickHouse{[4]*connPool{
		{rand.New(), make([]*chpool.Pool, 0, len(opt.Addrs)), queue.NewQueue(int64(opt.FastLightMaxConns)), map[string]int{}, sync.Mutex{}, map[string]int{}, sync.Mutex{}}, // fastLight
		{rand.New(), make([]*chpool.Pool, 0, len(opt.Addrs)), queue.NewQueue(int64(opt.FastHeavyMaxConns)), map[string]int{}, sync.Mutex{}, map[string]int{}, sync.Mutex{}}, // fastHeavy
		{rand.New(), make([]*chpool.Pool, 0, len(opt.Addrs)), queue.NewQueue(int64(opt.SlowLightMaxConns)), map[string]int{}, sync.Mutex{}, map[string]int{}, sync.Mutex{}}, // slowLight
		{rand.New(), make([]*chpool.Pool, 0, len(opt.Addrs)), queue.NewQueue(int64(opt.SlowHeavyMaxConns)), map[string]int{}, sync.Mutex{}, map[string]int{}, sync.Mutex{}}, // slowHeavy
	}}
	for _, addr := range opt.Addrs {
		for _, pool := range result.pools {
			server, err := chpool.New(context.Background(), chpool.Options{
				MaxConns: int32(pool.sem.MaxActiveQuery),
				ClientOptions: ch.Options{
					Address:          addr,
					User:             opt.User,
					Password:         opt.Password,
					Compression:      ch.CompressionLZ4,
					DialTimeout:      opt.DialTimeout,
					HandshakeTimeout: 10 * time.Second,
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

func (c *connPool) countOfReqLocked(m map[string]int) int {
	r := 0
	for _, v := range m {
		r += v
	}
	return r
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

func QueryKind(isFast, isLight bool) int {
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

func (ch *ClickHouse) Select(ctx context.Context, meta QueryMetaInto, query ch.Query) (info QueryHandleInfo, err error) {
	query.OnProfile = func(_ context.Context, p proto.Profile) error {
		info.Profile = p
		return nil
	}
	kind := QueryKind(meta.IsFast, meta.IsLight)
	pool := ch.pools[kind]
	servers := append(make([]*chpool.Pool, 0, len(pool.servers)), pool.servers...)
	for safetyCounter := 0; safetyCounter < len(pool.servers); safetyCounter++ {
		var i int
		i, err = pickRandomServer(servers, pool.rnd)
		if err != nil {
			return info, err
		}
		startTime := time.Now()
		pool.waitMx.Lock()
		pool.userWait[meta.User]++
		uniqWait := len(pool.userWait)
		allWait := pool.countOfReqLocked(pool.userWait)
		statshouse.Metric("statshouse_unique_wait_test", statshouse.Tags{1: strconv.FormatInt(int64(kind), 10), 2: "uniq"}).Value(float64(uniqWait))
		statshouse.Metric("statshouse_unique_wait_test", statshouse.Tags{1: strconv.FormatInt(int64(kind), 10), 2: "all"}).Value(float64(allWait))
		pool.waitMx.Unlock()
		err = pool.sem.Acquire(ctx, meta.User)
		waitLockDuration := time.Since(startTime)
		pool.waitMx.Lock()
		pool.userWait[meta.User]--
		if c := pool.userWait[meta.User]; c == 0 {
			delete(pool.userWait, meta.User)
		}
		pool.waitMx.Unlock()
		statshouse.Metric("statshouse_wait_lock", statshouse.Tags{1: strconv.FormatInt(int64(kind), 10)}).Value(waitLockDuration.Seconds())
		if err != nil {
			return info, err
		}
		pool.mx.Lock()
		pool.userActive[meta.User]++
		uniq := len(pool.userActive)
		all := pool.countOfReqLocked(pool.userActive)
		pool.mx.Unlock()
		statshouse.Metric("statshouse_unique_test", statshouse.Tags{1: strconv.FormatInt(int64(kind), 10), 2: "uniq"}).Value(float64(uniq))
		statshouse.Metric("statshouse_unique_test", statshouse.Tags{1: strconv.FormatInt(int64(kind), 10), 2: "all"}).Value(float64(all))

		start := time.Now()
		err = servers[i].Do(ctx, query)
		info.Duration = time.Since(start)
		pool.mx.Lock()
		pool.userActive[meta.User]--
		if c := pool.userActive[meta.User]; c == 0 {
			delete(pool.userActive, meta.User)
		}
		pool.mx.Unlock()
		pool.sem.Release()
		if err == nil {
			return // succeeded
		}
		if ctx.Err() != nil {
			return // failed
		}
		log.Printf("ClickHouse server is dead #%d: %v", i, err)
		// keep searching alive server
		servers = append(servers[:i], servers[i+1:]...)
	}
	return info, err
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

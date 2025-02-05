// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package chutil

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
	poolName       string
	rnd            *rand.Rand
	maxActiveQuery int
	servers        []*chpool.Pool
	sem            *queue.Queue
}

type ClickHouse struct {
	opt        ChConnOptions
	mx         sync.RWMutex
	namedPools map[string][6]*connPool
}

type QueryMetaInto struct {
	IsFast     bool
	IsLight    bool
	IsHardware bool

	User   string
	Metric int32
	Table  string
}

type QueryHandleInfo struct {
	Duration time.Duration
	Profile  proto.Profile
}

type ConnLimits struct {
	User              string `json:"user"`
	FastLightMaxConns int    `json:"fast_light_max_conns"`
	FastHeavyMaxConns int    `json:"fast_heavy_max_conns"`
	SlowLightMaxConns int    `json:"slow_light_max_conns"`
	SlowHeavyMaxConns int    `json:"slow_heavy_max_conns"`

	SlowHardwareMaxConns int `json:"slow_hardware_max_conns"`
	FastHardwareMaxConns int `json:"fast_hardware_max_conns"`
}

type ChConnOptions struct {
	ConnLimits
	Addrs       []string
	User        string
	Password    string
	DialTimeout time.Duration
}

const (
	fastLight       = 0 // // Must be synced with format.TagValueIDAPILaneFastLightv2
	fastHeavy       = 1
	slowLight       = 2
	slowHeavy       = 3
	slowHardware    = 4
	fastHardware    = 5
	defaultUserName = "@default_user"
)

func newConnPool(poolName string, maxConn int) *connPool {
	return &connPool{poolName, rand.New(), maxConn, make([]*chpool.Pool, 0), queue.NewQueue(int64(maxConn))}
}

func OpenClickHouse(opt ChConnOptions) (*ClickHouse, error) {
	if len(opt.Addrs) == 0 {
		return nil, fmt.Errorf("at least one ClickHouse address must be specified")
	}
	opt.ConnLimits.User = defaultUserName
	result := &ClickHouse{
		opt:        opt,
		namedPools: map[string][6]*connPool{},
	}
	err := result.SetLimits(nil)
	return result, err
}

func (ch1 *ClickHouse) SetLimits(limits []ConnLimits) error {
	ch1.mx.Lock()
	defer ch1.mx.Unlock()
	ch1.namedPools = map[string][6]*connPool{}
	limits = append(limits, ch1.opt.ConnLimits) // to avoid overriding default limits
	for _, limit := range limits {
		user := limit.User
		ch1.namedPools[user] = [6]*connPool{
			newConnPool(user, limit.FastLightMaxConns),
			newConnPool(user, limit.FastHeavyMaxConns),
			newConnPool(user, limit.SlowLightMaxConns),
			newConnPool(user, limit.SlowHeavyMaxConns),
			newConnPool(user, limit.SlowHardwareMaxConns),
			newConnPool(user, limit.FastHardwareMaxConns),
		}

		for _, addr := range ch1.opt.Addrs {
			for _, pool := range ch1.namedPools[user] {
				server, err := chpool.New(context.Background(), chpool.Options{
					MaxConns: int32(pool.maxActiveQuery),
					ClientOptions: ch.Options{
						Address:          addr,
						User:             ch1.opt.User,
						Password:         ch1.opt.Password,
						Compression:      ch.CompressionLZ4,
						DialTimeout:      ch1.opt.DialTimeout,
						HandshakeTimeout: 10 * time.Second,
					}})
				if err != nil {
					ch1.Close()
					return err
				}
				pool.servers = append(pool.servers, server)
			}
		}
	}

	return nil
}

func (ch1 *ClickHouse) Close() {
	ch1.mx.Lock()
	defer ch1.mx.Unlock()
	for _, pool := range ch1.namedPools {
		for _, lane := range pool {
			for _, b := range lane.servers {
				b.Close()
			}
		}
	}
}

func (ch1 *ClickHouse) SemaphoreCountSlowLight() int64 {
	ch1.mx.RLock()
	defer ch1.mx.RUnlock()
	cur, _ := ch1.namedPools[defaultUserName][slowLight].sem.Observe()
	return cur
}

func (ch1 *ClickHouse) SemaphoreCountSlowHeavy() int64 {
	ch1.mx.RLock()
	defer ch1.mx.RUnlock()
	cur, _ := ch1.namedPools[defaultUserName][slowHeavy].sem.Observe()
	return cur
}

func (ch1 *ClickHouse) SemaphoreCountFastLight() int64 {
	ch1.mx.RLock()
	defer ch1.mx.RUnlock()
	cur, _ := ch1.namedPools[defaultUserName][fastLight].sem.Observe()
	return cur
}

func (ch1 *ClickHouse) SemaphoreCountFastHeavy() int64 {
	ch1.mx.RLock()
	defer ch1.mx.RUnlock()
	cur, _ := ch1.namedPools[defaultUserName][fastHeavy].sem.Observe()
	return cur
}

func (ch1 *ClickHouse) SemaphoreCountFastHardware() int64 {
	ch1.mx.RLock()
	defer ch1.mx.RUnlock()
	cur, _ := ch1.namedPools[defaultUserName][fastHardware].sem.Observe()
	return cur
}

func (ch1 *ClickHouse) SemaphoreCountSlowHardware() int64 {
	ch1.mx.RLock()
	defer ch1.mx.RUnlock()
	cur, _ := ch1.namedPools[defaultUserName][slowHardware].sem.Observe()
	return cur
}

func QueryKind(isFast, isLight, isHardware bool) int {
	if isHardware {
		if isFast {
			return fastHardware
		}
		return slowHardware
	}
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
func (ch1 *ClickHouse) Select(ctx context.Context, meta QueryMetaInto, query ch.Query) (info QueryHandleInfo, err error) {
	pool := ch1.resolvePoolBy(meta)
	return pool.selectCH(ctx, meta, query)
}

func (ch1 *ClickHouse) resolvePoolBy(meta QueryMetaInto) *connPool {
	ch1.mx.RLock()
	defer ch1.mx.RUnlock()
	kind := QueryKind(meta.IsFast, meta.IsLight, meta.IsHardware)
	if pool, ok := ch1.namedPools[meta.User]; ok {
		pool := pool[kind]
		if pool.maxActiveQuery > 0 {
			return pool
		}
		return ch1.namedPools[defaultUserName][kind]
	}
	return ch1.namedPools[defaultUserName][kind]
}

func (pool *connPool) selectCH(ctx context.Context, meta QueryMetaInto, query ch.Query) (info QueryHandleInfo, err error) {
	query.OnProfile = func(_ context.Context, p proto.Profile) error {
		info.Profile = p
		return nil
	}
	kind := QueryKind(meta.IsFast, meta.IsLight, meta.IsHardware)
	servers := append(make([]*chpool.Pool, 0, len(pool.servers)), pool.servers...)
	for safetyCounter := 0; safetyCounter < len(pool.servers); safetyCounter++ {
		var i int
		i, err = pickRandomServer(servers, pool.rnd)
		if err != nil {
			return info, err
		}
		startTime := time.Now()

		err = pool.sem.Acquire(ctx, meta.User)
		waitLockDuration := time.Since(startTime)

		statshouse.Value("statshouse_wait_lock", statshouse.Tags{1: strconv.FormatInt(int64(kind), 10), 2: meta.User, 3: pool.poolName}, waitLockDuration.Seconds())
		if err != nil {
			return info, err
		}

		start := time.Now()
		err = servers[i].Do(ctx, query)
		info.Duration = time.Since(start)
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

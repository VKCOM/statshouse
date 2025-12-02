// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package chutil

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	_ "unsafe" // to access clickhouse.bind

	chgo "github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
	_ "github.com/ClickHouse/clickhouse-go/v2" // to access clickhouse.bind
	"pgregory.net/rand"

	"github.com/VKCOM/statshouse-go"

	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/util/queue"
)

type connPool struct {
	poolName            string
	rnd                 *rand.Rand
	maxActiveQuery      int
	maxShardActiveQuery int
	servers             []serverCH
	sem                 *queue.Queue
	shardSems           []*queue.Queue // shard_id -> semaphore
	avgQueryDurationNS  atomic.Int64   // exponentially smoothed average in nanoseconds
	shardAvgQueryNS     []atomic.Int64 // avgQueryDurationNS per-shard
}

type serverCH struct {
	addr string
	pool *chpool.Pool
	rate *RateLimit
}

type ClickHouse struct {
	opt        ChConnOptions
	mx         sync.RWMutex
	namedPools map[string][6]*connPool
	rateLimits []*RateLimit
}

type QueryMetaInto struct {
	IsFast     bool
	IsLight    bool
	IsHardware bool

	User           string
	Metric         *format.MetricMetaValue
	NewSharding    bool
	Table          string
	DisableCHAddrs []string
}

type QueryHandleInfo struct {
	WaitLockDuration             time.Duration
	QueryDuration                time.Duration
	Profile                      proto.Profile
	OSCPUVirtualTimeMicroseconds uint64
	Host                         string
	Shard                        int
	ErrorCode                    int
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
	RateLimitConfig
	Addrs               []string
	User                string
	Password            string
	DialTimeout         time.Duration
	SelectTimeout       time.Duration
	ShardByMetricShards int
	MaxShardConnsRatio  int
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

func newConnPool(poolName string, maxConn int, maxShardConn, shardCount int) *connPool {
	return &connPool{
		poolName:            poolName,
		rnd:                 rand.New(),
		maxActiveQuery:      maxConn,
		maxShardActiveQuery: maxShardConn,
		servers:             make([]serverCH, 0),
		sem:                 queue.NewQueue(int64(maxConn)),
		shardSems:           make([]*queue.Queue, shardCount),
		shardAvgQueryNS:     make([]atomic.Int64, shardCount),
	}
}

func newConnPoolWithShards(poolName string, maxConn int, shardCount int, maxShardConnsRatio int) *connPool {
	shardMaxConn := max(maxConn*maxShardConnsRatio/100, 1)
	pool := newConnPool(poolName, maxConn, shardMaxConn, shardCount)
	for i := 0; i < shardCount; i++ {
		pool.shardSems[i] = queue.NewQueue(int64(shardMaxConn))
	}
	return pool
}

func OpenClickHouse(opt ChConnOptions) (*ClickHouse, error) {
	if len(opt.Addrs) == 0 {
		return nil, fmt.Errorf("at least one ClickHouse address must be specified")
	}
	opt.ConnLimits.User = defaultUserName
	result := &ClickHouse{
		opt:        opt,
		namedPools: map[string][6]*connPool{},
		rateLimits: make([]*RateLimit, 0, len(opt.Addrs)),
	}
	for i := 0; i < len(opt.Addrs); i++ {
		result.rateLimits = append(result.rateLimits, NewRateLimit(opt.RateLimitConfig, i/3+1, i%3+1))
		result.rateLimits[i].Start()
	}
	err := result.SetLimits(nil, opt.MaxShardConnsRatio, opt.RateLimitConfig)
	return result, err
}

func (ch1 *ClickHouse) SetLimits(limits []ConnLimits, maxShardConnsRatio int, rtCfg RateLimitConfig) error {
	ch1.mx.Lock()
	defer ch1.mx.Unlock()
	ch1.namedPools = map[string][6]*connPool{}
	limits = append(limits, ch1.opt.ConnLimits) // to avoid overriding default limits

	shardCount := len(ch1.opt.Addrs) / 3
	ch1.opt.MaxShardConnsRatio = maxShardConnsRatio
	for i := 0; i < len(ch1.opt.Addrs); i++ {
		ch1.rateLimits[i].SetConfig(rtCfg)
	}
	for _, limit := range limits {
		user := limit.User
		ch1.namedPools[user] = [6]*connPool{
			newConnPoolWithShards(user, limit.FastLightMaxConns, shardCount, maxShardConnsRatio),
			newConnPoolWithShards(user, limit.FastHeavyMaxConns, shardCount, maxShardConnsRatio),
			newConnPoolWithShards(user, limit.SlowLightMaxConns, shardCount, maxShardConnsRatio),
			newConnPoolWithShards(user, limit.SlowHeavyMaxConns, shardCount, maxShardConnsRatio),
			newConnPoolWithShards(user, limit.SlowHardwareMaxConns, shardCount, maxShardConnsRatio),
			newConnPoolWithShards(user, limit.FastHardwareMaxConns, shardCount, maxShardConnsRatio),
		}

		for i, addr := range ch1.opt.Addrs {
			for _, pool := range ch1.namedPools[user] {
				server, err := chpool.New(context.Background(), chpool.Options{
					MaxConns: int32(pool.maxActiveQuery),
					ClientOptions: chgo.Options{
						Address:          addr,
						User:             ch1.opt.User,
						Password:         ch1.opt.Password,
						Compression:      chgo.CompressionLZ4,
						DialTimeout:      ch1.opt.DialTimeout,
						HandshakeTimeout: 10 * time.Second,
					}})
				if err != nil {
					ch1.Close() //TODO: Deadlock?
					return err
				}
				pool.servers = append(pool.servers, serverCH{
					addr: addr,
					pool: server,
					rate: ch1.rateLimits[i],
				})
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
				b.pool.Close()
			}
		}
	}
	for _, rl := range ch1.rateLimits {
		rl.Close()
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

func (ch1 *ClickHouse) ShardSemaphoreCountSlowLight() []int64 {
	ch1.mx.RLock()
	defer ch1.mx.RUnlock()

	sems := ch1.namedPools[defaultUserName][slowLight].shardSems
	counts := make([]int64, len(sems))
	for i, sem := range sems {
		counts[i], _ = sem.Observe()
	}
	return counts
}

func (ch1 *ClickHouse) ShardSemaphoreCountSlowHeavy() []int64 {
	ch1.mx.RLock()
	defer ch1.mx.RUnlock()

	sems := ch1.namedPools[defaultUserName][slowHeavy].shardSems
	counts := make([]int64, len(sems))
	for i, sem := range sems {
		counts[i], _ = sem.Observe()
	}
	return counts
}

func (ch1 *ClickHouse) ShardSemaphoreCountFastLight() []int64 {
	ch1.mx.RLock()
	defer ch1.mx.RUnlock()

	sems := ch1.namedPools[defaultUserName][fastLight].shardSems
	counts := make([]int64, len(sems))
	for i, sem := range sems {
		counts[i], _ = sem.Observe()
	}
	return counts
}

func (ch1 *ClickHouse) ShardSemaphoreCountFastHeavy() []int64 {
	ch1.mx.RLock()
	defer ch1.mx.RUnlock()

	sems := ch1.namedPools[defaultUserName][fastHeavy].shardSems
	counts := make([]int64, len(sems))
	for i, sem := range sems {
		counts[i], _ = sem.Observe()
	}
	return counts
}

func (ch1 *ClickHouse) ShardSemaphoreCountFastHardware() []int64 {
	ch1.mx.RLock()
	defer ch1.mx.RUnlock()

	sems := ch1.namedPools[defaultUserName][fastHardware].shardSems
	counts := make([]int64, len(sems))
	for i, sem := range sems {
		counts[i], _ = sem.Observe()
	}
	return counts
}

func (ch1 *ClickHouse) ShardSemaphoreCountSlowHardware() []int64 {
	ch1.mx.RLock()
	defer ch1.mx.RUnlock()

	sems := ch1.namedPools[defaultUserName][slowHardware].shardSems
	counts := make([]int64, len(sems))
	for i, sem := range sems {
		counts[i], _ = sem.Observe()
	}
	return counts
}

func (ch1 *ClickHouse) RateLimitStatistics() []RateLimitMetric {
	items := make([]RateLimitMetric, 0, len(ch1.rateLimits))
	for _, limit := range ch1.rateLimits {
		items = append(items, limit.GetMetrics())
	}
	return items
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
func (ch1 *ClickHouse) Select(ctx context.Context, meta QueryMetaInto, query chgo.Query) (info QueryHandleInfo, err error) {
	pool := ch1.resolvePoolBy(meta)
	return pool.selectCH(ctx, ch1, meta, query)
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

func (pool *connPool) selectCH(ctx context.Context, ch *ClickHouse, meta QueryMetaInto, query chgo.Query) (info QueryHandleInfo, err error) {
	query.OnProfile = func(_ context.Context, p proto.Profile) error {
		info.Profile = p
		return nil
	}
	query.OnProfileEvents = func(_ context.Context, ev []proto.ProfileEvent) error {
		for _, e := range ev {
			if e.Name == "OSCPUVirtualTimeMicroseconds" {
				info.OSCPUVirtualTimeMicroseconds = uint64(e.Value)
				break
			}
		}
		return nil
	}
	kind := QueryKind(meta.IsFast, meta.IsLight, meta.IsHardware)
	shard := -1
	if meta.NewSharding {
		shardCnt := ch.opt.ShardByMetricShards
		if shardCnt == 0 {
			shardCnt = len(pool.servers) / 3
		}
		shard = meta.Metric.Shard(shardCnt)
		if shard >= shardCnt { // if meta.Metric is nil here (should be checked above), we will not enter this if so meta.Metric.Name is safe
			return info, fmt.Errorf("metric %q fixed shard %d too large (total shards %d)", meta.Metric.Name, shard, shardCnt)
		}
	}
	sem := pool.sem
	var servers []serverCH
	if shard < 0 {
		servers = append(make([]serverCH, 0, len(pool.servers)), pool.servers...)
	} else {
		i := shard * 3
		servers = append(make([]serverCH, 0, 3), pool.servers[i:i+3]...)
		sem = pool.shardSems[shard]
		adjustSemCapacity(servers, sem, pool.maxShardActiveQuery)
	}
	for safetyCounter := 0; safetyCounter < len(servers); safetyCounter++ {
		var i int
		i, err = pickHealthServer(servers, pool.rnd)
		if err != nil {
			return info, err
		}
		if !slices.Contains(meta.DisableCHAddrs, servers[i].addr) {
			startTime := time.Now()

			err = sem.Acquire(ctx, meta.User)
			info.WaitLockDuration = time.Since(startTime)
			info.Host = servers[i].addr
			info.Shard = shard + 1

			statshouse.Value("statshouse_wait_lock", statshouse.Tags{1: strconv.FormatInt(int64(kind), 10), 2: meta.User, 3: pool.poolName, 5: strconv.Itoa(shard + 1)}, info.WaitLockDuration.Seconds())
			if err != nil {
				info.ErrorCode = format.TagValueIDAPIResponseExceptionSemError
				return info, err
			}

			// ctx might cancel during sem.Acquire
			select {
			case <-ctx.Done():
				sem.Release()
				info.ErrorCode = format.TagValueIDAPIResponseExceptionSemTimeout
				return info, ctx.Err()
			default:
			}
			if deadline, ok := ctx.Deadline(); ok {
				remaining := time.Until(deadline)
				if avg := pool.getAvgQueryDuration(shard); avg > 0 {
					const safetyNum = 12 // 20% upside
					const safetyDen = 10
					if remaining*safetyDen < avg*safetyNum {
						sem.Release()
						info.ErrorCode = format.TagValueIDAPIResponseExceptionSemTooLate
						return info, context.DeadlineExceeded
					}
				}
			}

			start := time.Now()
			done := make(chan struct{})
			go func() {
				defer close(done)
				queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				err = servers[i].rate.DoInflight(func() error { return servers[i].pool.Do(queryCtx, query) })
				info.QueryDuration = time.Since(start)
				sem.Release()
				if queryCtx.Err() != nil {
					statshouse.Value(format.BuiltinMetricMetaAPISelectDuration.Name, statshouse.Tags{
						1:  strconv.Itoa(kind),
						2:  strconv.Itoa(int(meta.Metric.MetricID)),
						3:  meta.Table,
						5:  "error",
						7:  meta.User,
						8:  strconv.Itoa(shard),
						10: strconv.Itoa(format.TagValueIDAPIResponseExceptionLongCHTimeout)}, time.Since(start).Seconds())
				}
			}()

			select {
			case <-ctx.Done():
				servers[i].rate.RecordEvent(Event{
					Timestamp: start,
					Status:    StatusError,
					Duration:  time.Since(start),
				})
				info.ErrorCode = format.TagValueIDAPIResponseExceptionCHTimeout
				return info, ctx.Err() // failed
			case <-done:
			}
			if err == nil {
				servers[i].rate.RecordEvent(Event{
					Timestamp: start,
					Status:    StatusSuccess,
					Duration:  info.QueryDuration,
				})
				pool.recordQueryDuration(info.QueryDuration, shard)
				servers = slices.Delete(servers, i, i+1)
				info.ErrorCode = 0
				break // succeeded
			}
			servers[i].rate.RecordEvent(Event{
				Timestamp: start,
				Status:    StatusError,
				Duration:  info.QueryDuration,
			})
			info.ErrorCode = format.TagValueIDAPIResponseExceptionCHUnknown
			if code, ok := chgo.AsException(err); ok {
				info.ErrorCode = int(code.Code)
			}
		}
		// keep searching alive server
		servers = slices.Delete(servers, i, i+1)
	}
	if err != nil {
		return info, err
	}
	pickCheckServer(servers, query, pool.rnd, ch.opt.SelectTimeout)
	return info, nil
}

func pickRandomServer(s []serverCH, r *rand.Rand) (int, error) {
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
	if s[i1].pool.Stat().AcquiredResources() < s[i2].pool.Stat().AcquiredResources() {
		return i1, nil
	} else {
		return i2, nil
	}
}

func pickHealthServer(s []serverCH, r *rand.Rand) (int, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("all ClickHouse servers are dead")
	}
	if len(s) == 1 {
		return 0, nil
	}
	i1 := -1
	i2 := -1
	var minInflight = ^uint64(0)
	for i, item := range s {
		if count, ok := item.rate.GetInflightCount(); ok {
			if minInflight > count {
				minInflight = count
				i1 = i
				i2 = -1
			} else if minInflight == count {
				i2 = i
			}
		}
	}
	if i1 == -1 {
		return pickRandomServer(s, r)
	}
	if i2 == -1 {
		return i1, nil
	}
	if r.Intn(2) == 0 {
		return i1, nil
	}
	return i2, nil
}

func pickCheckServer(s []serverCH, query chgo.Query, r *rand.Rand, timeout time.Duration) {
	var checks []serverCH
	for i := 0; i < len(s); i++ {
		if s[i].rate.ShouldCheck() {
			checks = append(checks, s[i])
		}
	}
	ind, err := pickRandomServer(checks, r)
	if err != nil {
		return
	}
	checks[ind].rate.RecordCheck(func() Event {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := checks[ind].pool.Do(ctx, query); err != nil {
			return Event{Status: StatusError}
		}
		return Event{Status: StatusSuccess}
	})
}

// shardLoadReductionRatio 30% reduction step per failed shard host (3 hosts total, 10% load remains if all fail)
const shardLoadReductionRatio = 30

func adjustSemCapacity(s []serverCH, sem *queue.Queue, maxSize int) {
	if len(s) != 3 {
		return
	}
	loadFactor := 100
	for i := 0; i < len(s); i++ {
		if _, ok := s[i].rate.GetInflightCount(); !ok {
			loadFactor -= shardLoadReductionRatio
		}
	}
	sem.AdjustCapacity(uint64(maxSize * loadFactor / 100))
}

func (pool *connPool) getAvgQueryDuration(shard int) time.Duration {
	if shard >= 0 && shard < len(pool.shardAvgQueryNS) {
		if ns := pool.shardAvgQueryNS[shard].Load(); ns > 0 {
			return time.Duration(ns)
		}
		return 0
	}
	ns := pool.avgQueryDurationNS.Load()
	if ns <= 0 {
		return 0
	}
	return time.Duration(ns)
}

func (pool *connPool) recordQueryDuration(d time.Duration, shard int) {
	const alphaDen = 8
	update := func(a *atomic.Int64) {
		for {
			old := a.Load()
			var next int64
			if old <= 0 {
				next = int64(d)
			} else {
				next = old + (int64(d)-old)/alphaDen
			}
			if a.CompareAndSwap(old, next) {
				return
			}
		}
	}
	if shard >= 0 && shard < len(pool.shardAvgQueryNS) {
		update(&pool.shardAvgQueryNS[shard])
		return
	}
	update(&pool.avgQueryDurationNS)
}

func BindQuery(query string, args ...any) (string, error) {
	return clickHouseBind(time.UTC, query, args...)
}

//go:linkname clickHouseBind github.com/ClickHouse/clickhouse-go/v2.bind
func clickHouseBind(tz *time.Location, query string, args ...interface{}) (string, error)

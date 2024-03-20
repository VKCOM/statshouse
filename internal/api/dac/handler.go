// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package dac

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "unsafe" // to access clickhouse.bind

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
	_ "github.com/ClickHouse/clickhouse-go/v2" // to access clickhouse.bind
	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/api/model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/util"
	"github.com/vkcom/statshouse/internal/util/queue"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
	"pgregory.net/rand"
)

const (
	fastLight = 0
	fastHeavy = 1
	slowLight = 2
	slowHeavy = 3 // fix Close after adding new modes

	_0s  = 0
	_1s  = 1
	_5s  = 5
	_15s = 15
	_1m  = 60
	_5m  = 5 * _1m
	_15m = 15 * _1m
	_1h  = 60 * _1m
	_4h  = 4 * _1h
	_24h = 24 * _1h
	_7d  = 7 * _24h
	_1M  = 31 * _24h

	_1mTableSH1          = "statshouse_value_dist"
	_1hTableSH1          = "statshouse_value_dist_1h"
	_1hTableStringTopSH1 = "stats_1h_agg_stop_dist"
	_1dTableUniquesSH1   = "stats_1d_agg_dist"
	_1sTableSH2          = "statshouse_value_1s_dist"
	_1mTableSH2          = "statshouse_value_1m_dist"
	_1hTableSH2          = "statshouse_value_1h_dist"
)

var (
	LodTables = map[string]map[int64]string{
		Version1: {
			_1M:  _1hTableSH1,
			_7d:  _1hTableSH1,
			_24h: _1hTableSH1,
			_4h:  _1hTableSH1,
			_1h:  _1hTableSH1,
			_15m: _1mTableSH1,
			_5m:  _1mTableSH1,
			_1m:  _1mTableSH1,
		},
		Version2: {
			_1M:  _1hTableSH2,
			_7d:  _1hTableSH2,
			_24h: _1hTableSH2,
			_4h:  _1hTableSH2,
			_1h:  _1hTableSH2,
			_15m: _1mTableSH2,
			_5m:  _1mTableSH2,
			_1m:  _1mTableSH2,
			_15s: _1sTableSH2,
			_5s:  _1sTableSH2,
			_1s:  _1sTableSH2,
		},
	}

	LodLevels = map[string][]LodSwitch{
		Version1: {{
			RelSwitch: 33 * _24h,
			Levels:    []int64{_7d, _24h, _4h, _1h},
			Tables:    LodTables[Version1],
		}, {
			RelSwitch: _0s,
			Levels:    []int64{_7d, _24h, _4h, _1h, _15m, _5m, _1m},
			Tables:    LodTables[Version1],
		}},
		// Subtract from relSwitch to facilitate calculation of derivative.
		// Subtrahend should be multiple of the next lodSwitch minimum level.
		Version2: {{
			RelSwitch: 33*_24h - 2*_1m,
			Levels:    []int64{_7d, _24h, _4h, _1h},
			Tables:    LodTables[Version2],
		}, {
			RelSwitch: 52*_1h - 2*_1s,
			Levels:    []int64{_7d, _24h, _4h, _1h, _15m, _5m, _1m},
			Tables:    LodTables[Version2],
		}, {
			RelSwitch: _0s,
			Levels:    []int64{_7d, _24h, _4h, _1h, _15m, _5m, _1m, _15s, _5s, _1s},
			Tables:    LodTables[Version2],
		}},
	}

	LodLevelsV1StringTop = []LodSwitch{{
		RelSwitch: _0s,
		Levels:    []int64{_7d, _24h, _4h, _1h},
		Tables: map[int64]string{
			_7d:  _1hTableStringTopSH1,
			_24h: _1hTableStringTopSH1,
			_4h:  _1hTableStringTopSH1,
			_1h:  _1hTableStringTopSH1,
		},
	}}

	LodLevelsV1Unique = []LodSwitch{{
		RelSwitch: _0s,
		Levels:    []int64{_7d, _24h},
		Tables: map[int64]string{
			_7d:  _1dTableUniquesSH1,
			_24h: _1dTableUniquesSH1,
		},
	}}

	LodLevelsV2Monthly = []LodSwitch{{
		RelSwitch: _0s,
		Levels:    []int64{_1M},
		Tables: map[int64]string{
			_1M: _1hTableSH2,
		},
	}}

	LodLevelsV1Monthly = []LodSwitch{{
		RelSwitch: _0s,
		Levels:    []int64{_1M},
		Tables: map[int64]string{
			_1M: _1hTableSH1,
		},
	}}

	LodLevelsV1MonthlyUnique = []LodSwitch{{
		RelSwitch: _0s,
		Levels:    []int64{_1M},
		Tables: map[int64]string{
			_1M: _1dTableUniquesSH1,
		},
	}}

	LodLevelsV1MonthlyStringTop = []LodSwitch{{
		RelSwitch: _0s,
		Levels:    []int64{_1M},
		Tables: map[int64]string{
			_1M: _1hTableStringTopSH1,
		},
	}}
)

type Handler struct {
	ch        map[string]*util.ClickHouse
	location  *time.Location
	utcOffset int64
	verbose   bool
}

type LodSwitch struct {
	RelSwitch  int64 // must be properly aligned
	Levels     []int64
	Tables     map[int64]string
	HasPreKey  bool
	PreKeyOnly bool
}

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

type SelectRow struct {
	ValID int32
	Val   string
	Cnt   float64
}

type tagValuesSelectCols struct {
	meta  TagValuesQueryMeta
	valID proto.ColInt32
	val   proto.ColStr
	cnt   proto.ColFloat64
	res   proto.Results
}

func newTagValuesSelectCols(meta TagValuesQueryMeta) *tagValuesSelectCols {
	// NB! Keep columns selection order and names is sync with sql.go code
	c := &tagValuesSelectCols{meta: meta}
	if meta.StringValue {
		c.res = append(c.res, proto.ResultColumn{Name: "_string_value", Data: &c.val})
	} else {
		c.res = append(c.res, proto.ResultColumn{Name: "_value", Data: &c.valID})
	}
	c.res = append(c.res, proto.ResultColumn{Name: "_count", Data: &c.cnt})
	return c
}

func (c *tagValuesSelectCols) rowAt(i int) SelectRow {
	row := SelectRow{Cnt: c.cnt[i]}
	if c.meta.StringValue {
		pos := c.val.Pos[i]
		row.Val = string(c.val.Buf[pos.Start:pos.End])
	} else {
		row.ValID = c.valID[i]
	}
	return row
}

func NewHandler(argv Options, location *time.Location, utcOffset int64, verbose bool) (Handler, func(), error) {
	var chV1 *util.ClickHouse
	if len(argv.V1Addrs) > 0 {
		var err error
		chV1, err = util.OpenClickHouse(util.ChConnOptions{
			Addrs:             argv.V1Addrs,
			User:              argv.v1User,
			Password:          argv.v1Password,
			DialTimeout:       chDialTimeout,
			FastLightMaxConns: argv.v1MaxConns,
			FastHeavyMaxConns: argv.v1MaxConns,
			SlowLightMaxConns: argv.v1MaxConns,
			SlowHeavyMaxConns: argv.v1MaxConns,
		})
		if err != nil {
			return Handler{}, nil, fmt.Errorf("failed to open ClickHouse-v1: %w", err)
		}
	}
	chV2, err := util.OpenClickHouse(util.ChConnOptions{
		Addrs:             argv.V2Addrs,
		User:              argv.v2User,
		Password:          argv.v2Password,
		DialTimeout:       chDialTimeout,
		FastLightMaxConns: argv.v2MaxLightFastConns,
		FastHeavyMaxConns: argv.v2MaxHeavyFastConns,
		SlowLightMaxConns: argv.v2MaxLightSlowConns,
		SlowHeavyMaxConns: argv.v2MaxHeavySlowConns,
	})
	if err != nil {
		chV1.Close()
		return Handler{}, nil, fmt.Errorf("failed to open ClickHouse-v2: %w", err)
	}
	// TODO
	h := Handler{
		location:  location,
		utcOffset: utcOffset,
		verbose:   verbose,
		ch: map[string]*util.ClickHouse{
			Version1: chV1,
			Version2: chV2,
		},
	}
	close := func() {
		if chV1 != nil {
			chV1.Close()
		}
		if chV2 != nil {
			chV2.Close()
		}
	}
	return h, close, nil
}

func (h *Handler) LoadPoints(ctx context.Context, pq *PreparedPointsQuery, lod LodInfo, ret [][]TsSelectRow, retStartIx int) (int, error) {
	query, args, err := loadPointsQuery(pq, lod, h.utcOffset)
	if err != nil {
		return 0, err
	}
	rows := 0
	cols := newPointsSelectCols(args, true)
	isFast := lod.IsFast()
	isLight := pq.IsLight()
	metric := pq.MetricID
	table := lod.Table
	kind := pq.Kind
	start := time.Now()
	err = h.DoSelect(ctx, util.QueryMetaInto{
		IsFast:  isFast,
		IsLight: isLight,
		User:    pq.User,
		Metric:  metric,
		Table:   table,
		Kind:    string(kind),
	}, pq.Version, ch.Query{
		Body:   query,
		Result: cols.res,
		OnResult: func(_ context.Context, block proto.Block) error {
			for i := 0; i < block.Rows; i++ {
				replaceInfNan(&cols.cnt[i])
				for j := 0; j < len(cols.val); j++ {
					replaceInfNan(&cols.val[j][i])
				}
				row := cols.rowAt(i)
				ix, err := lod.IndexOf(row.Time)
				if err != nil {
					return err
				}
				ix += retStartIx
				ret[ix] = append(ret[ix], row)
			}
			rows += block.Rows
			return nil
		}})
	duration := time.Since(start)
	if err != nil {
		return 0, err
	}
	if rows == maxSeriesRows {
		return rows, errTooManyRows // prevent cache being populated by incomplete data
	}
	if h.verbose {
		log.Printf("[debug] loaded %v rows from %v (%v timestamps, %v to %v step %v) for %q in %v",
			rows,
			lod.Table,
			(lod.ToSec-lod.FromSec)/lod.StepSec,
			time.Unix(lod.FromSec, 0),
			time.Unix(lod.ToSec, 0),
			time.Duration(lod.StepSec)*time.Second,
			pq.User,
			duration,
		)
	}
	return rows, nil
}

func (h *Handler) LoadPoint(ctx context.Context, pq *PreparedPointsQuery, lod LodInfo) ([]PSelectRow, error) {
	query, args, err := loadPointQuery(pq, lod, h.utcOffset)
	if err != nil {
		return nil, err
	}
	ret := make([]PSelectRow, 0)
	rows := 0
	cols := newPointsSelectCols(args, false)
	isFast := lod.IsFast()
	isLight := pq.IsLight()
	metric := pq.MetricID
	table := lod.Table
	kind := pq.Kind
	err = h.DoSelect(ctx, util.QueryMetaInto{
		IsFast:  isFast,
		IsLight: isLight,
		User:    pq.User,
		Metric:  metric,
		Table:   table,
		Kind:    string(kind),
	}, pq.Version, ch.Query{
		Body:   query,
		Result: cols.res,
		OnResult: func(_ context.Context, block proto.Block) error {
			for i := 0; i < block.Rows; i++ {
				//todo check
				replaceInfNan(&cols.cnt[i])
				for j := 0; j < len(cols.val); j++ {
					replaceInfNan(&cols.val[j][i])
				}
				row := cols.rowAtPoint(i)
				ret = append(ret, row)
			}
			rows += block.Rows
			return nil
		}})
	if err != nil {
		return nil, err
	}

	if rows == maxSeriesRows {
		return ret, errTooManyRows // prevent cache being populated by incomplete data
	}
	if h.verbose {
		log.Printf("[debug] loaded %v rows from %v (%v to %v) for %q in",
			rows,
			lod.Table,
			time.Unix(lod.FromSec, 0),
			time.Unix(lod.ToSec, 0),
			pq.User,
		)
	}

	return ret, nil
}

func (h *Handler) LoadTagValues(ctx context.Context, ai model.AccessInfo, pq *PreparedTagValuesQuery, lods []LodInfo) ([]SelectRow, error) {
	tagInfo := make(map[SelectRow]float64)
	if pq.Version == Version1 && pq.TagID == format.EnvTagID {
		tagInfo[SelectRow{ValID: format.TagValueIDProductionLegacy}] = 100 // we only support production tables for v1
	} else {
		for _, lod := range lods {
			query, args, err := TagValuesQuery(pq, lod) // we set limit to numResult+1
			if err != nil {
				return nil, err
			}
			cols := newTagValuesSelectCols(args)
			isFast := lod.FromSec+fastQueryTimeInterval >= lod.ToSec
			err = h.DoSelect(ctx, util.QueryMetaInto{
				IsFast:  isFast,
				IsLight: true,
				User:    ai.User,
				Metric:  pq.MetricID,
				Table:   lod.Table,
				Kind:    "get_mapping",
			}, pq.Version, ch.Query{
				Body:   query,
				Result: cols.res,
				OnResult: func(_ context.Context, b proto.Block) error {
					for i := 0; i < b.Rows; i++ {
						tag := cols.rowAt(i)
						tagInfo[SelectRow{ValID: tag.ValID, Val: tag.Val}] += tag.Cnt
					}
					return nil
				}})
			if err != nil {
				return nil, err
			}
		}
	}
	data := make([]SelectRow, 0, len(tagInfo))
	for k, count := range tagInfo {
		data = append(data, SelectRow{ValID: k.ValID, Val: k.Val, Cnt: count})
	}
	sort.Slice(data, func(i int, j int) bool { return data[i].Cnt > data[j].Cnt })
	return data, nil
}

type CacheInvalidateLogRow struct {
	T  int64 `ch:"time"` // time of insert
	At int64 `ch:"key1"` // seconds inserted (changed), which should be invalidated
}

const (
	invalidateLinger       = 15 * time.Second // try to work around ClickHouse table replication race
	cacheInvalidateMaxRows = 100_000
)

func (h *Handler) LoadCacheInvalidateLogRows(ctx context.Context, from int64, seen map[CacheInvalidateLogRow]struct{}) (int64, map[int64][]int64, map[CacheInvalidateLogRow]struct{}, error) {
	uncertain := time.Now().Add(-invalidateLinger).Unix()
	if from > uncertain {
		from = uncertain
	}

	queryBody, err := util.BindQuery(fmt.Sprintf(`
SELECT
  toInt64(time) AS time, toInt64(key1) AS key1
FROM
  %s
WHERE
  metric == ? AND time >= ?
GROUP BY
  time, key1
ORDER BY
  time, key1
LIMIT
  ?
SETTINGS
  optimize_aggregation_in_order = 1
`, _1sTableSH2), format.BuiltinMetricIDContributorsLog, from, cacheInvalidateMaxRows)
	if err != nil {
		return from, nil, nil, err
	}
	// TODO - write metric with len(rows)
	// TODO - code that works if we hit limit above

	var (
		time    proto.ColInt64
		key1    proto.ColInt64
		todo    = map[int64][]int64{}
		newSeen = map[CacheInvalidateLogRow]struct{}{}
	)
	err = h.DoSelect(ctx, util.QueryMetaInto{
		IsFast:  true,
		IsLight: true,
		User:    "cache-update",
		Metric:  format.BuiltinMetricIDContributorsLog,
		Table:   _1sTableSH2,
		Kind:    "cache-update",
	}, Version2, ch.Query{
		Body: queryBody,
		Result: proto.Results{
			{Name: "time", Data: &time},
			{Name: "key1", Data: &key1},
		},
		OnResult: func(_ context.Context, b proto.Block) error {
			for i := 0; i < b.Rows; i++ {
				r := CacheInvalidateLogRow{
					T:  time[i],
					At: key1[i],
				}
				newSeen[r] = struct{}{}
				from = r.T
				if _, ok := seen[r]; ok {
					continue
				}
				for lodLevel := range LodTables[Version2] {
					t := roundTime(r.At, lodLevel, h.utcOffset)
					w := todo[lodLevel]
					if len(w) == 0 || w[len(w)-1] != t {
						todo[lodLevel] = append(w, t)
					}
				}
			}
			return nil
		}})
	return from, todo, newSeen, err
}

func mathDiv(a int64, b int64) int64 {
	quo := a / b
	if (a >= 0) == (b >= 0) || a%b == 0 {
		return quo
	}
	return quo - 1
}

func roundTime(t int64, step int64, utcOffset int64) int64 {
	return mathDiv(t+utcOffset, step)*step - utcOffset
}

func (h *Handler) WriteActiveQuieries(client *statshouse.Client) {
	for versionTag, ch := range h.ch {
		fastLight := client.Metric(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneFastLight), 4: srvfunc.HostnameForStatshouse()})
		fastLight.Value(float64(ch.SemaphoreCountFastLight()))

		fastHeavy := client.Metric(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneFastHeavy), 4: srvfunc.HostnameForStatshouse()})
		fastHeavy.Value(float64(ch.SemaphoreCountFastHeavy()))

		slowLight := client.Metric(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneSlowLight), 4: srvfunc.HostnameForStatshouse()})
		slowLight.Value(float64(ch.SemaphoreCountSlowLight()))

		slowHeavy := client.Metric(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneSlowHeavy), 4: srvfunc.HostnameForStatshouse()})
		slowHeavy.Value(float64(ch.SemaphoreCountSlowHeavy()))
	}
}

func (h *Handler) DoSelect(ctx context.Context, meta util.QueryMetaInto, version string, query ch.Query) error {
	if version == Version1 && h.ch[version] == nil {
		return fmt.Errorf("legacy ClickHouse database is disabled")
	}

	model.SaveDebugQuery(ctx, query.Body)

	start := time.Now()
	model.ReportQueryKind(ctx, meta.IsFast, meta.IsLight)
	info, err := h.ch[version].Select(ctx, meta, query)
	duration := time.Since(start)
	if h.verbose {
		log.Printf("[debug] SQL for %q done in %v, err: %v", meta.User, duration, err)
	}

	ChSelectMetricDuration(info.Duration, meta.Metric, meta.User, meta.Table, meta.Kind, meta.IsFast, meta.IsLight, err)
	ChSelectProfile(meta.IsFast, meta.IsLight, info.Profile, err)

	return err
}

func ChSelectMetricDuration(duration time.Duration, metricID int32, user, table, kind string, isFast, isLight bool, err error) {
	ok := "ok"
	if err != nil {
		ok = "error"
	}
	statshouse.Metric(
		format.BuiltinMetricNameAPISelectDuration,
		statshouse.Tags{
			1: modeStr(isFast, isLight),
			2: strconv.Itoa(int(metricID)),
			3: table,
			4: kind,
			5: ok,
			6: getStatTokenName(user),
			7: user,
		},
	).Value(duration.Seconds())
}

func getStatTokenName(user string) string {
	if strings.Contains(user, "@") {
		return userTokenName
	}
	return user
}

func ChSelectProfile(isFast, isLight bool, info proto.Profile, err error) {
	chSelectPushMetric(format.BuiltinMetricNameAPISelectBytes, isFast, isLight, float64(info.Bytes), err)
	chSelectPushMetric(format.BuiltinMetricNameAPISelectRows, isFast, isLight, float64(info.Rows), err)
}

func chSelectPushMetric(metric string, isFast, isLight bool, data float64, err error) {
	m := statshouse.Metric(
		metric,
		statshouse.Tags{
			1: modeStr(isFast, isLight),
		},
	)
	m.Value(data)
	if err != nil {
		m.StringTop(err.Error())
	}
}

func modeStr(isFast, isLight bool) string {
	mode := "slow"
	if isFast {
		mode = "fast"
	}
	if isLight {
		mode += "light"
	} else {
		mode += "heavy"
	}
	return mode
}

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
		statshouse.Metric("statshouse_wait_lock", statshouse.Tags{1: strconv.FormatInt(int64(kind), 10), 2: meta.User}).Value(waitLockDuration.Seconds())
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

type pointsSelectCols struct {
	time         proto.ColInt64
	step         proto.ColInt64
	cnt          proto.ColFloat64
	val          []proto.ColFloat64
	tag          []proto.ColInt32
	tagIx        []int
	tagStr       proto.ColStr
	minMaxHostV1 [2]proto.ColUInt8 // "min" at [0], "max" at [1]
	minMaxHostV2 [2]proto.ColInt32 // "min" at [0], "max" at [1]
	shardNum     proto.ColUInt32
	res          proto.Results
}

func newPointsSelectCols(meta PointsQueryMeta, useTime bool) *pointsSelectCols {
	// NB! Keep columns selection order and names is sync with sql.go code
	c := &pointsSelectCols{
		val:   make([]proto.ColFloat64, meta.Vals),
		tag:   make([]proto.ColInt32, 0, len(meta.Tags)),
		tagIx: make([]int, 0, len(meta.Tags)),
	}
	if useTime {
		c.res = proto.Results{
			{Name: "_time", Data: &c.time},
			{Name: "_stepSec", Data: &c.step},
		}
	}
	for _, id := range meta.Tags {
		switch id {
		case format.StringTopTagID:
			c.res = append(c.res, proto.ResultColumn{Name: "key_s", Data: &c.tagStr})
		case format.ShardTagID:
			c.res = append(c.res, proto.ResultColumn{Name: "key_shard_num", Data: &c.shardNum})
		default:
			c.tag = append(c.tag, proto.ColInt32{})
			c.res = append(c.res, proto.ResultColumn{Name: "key" + id, Data: &c.tag[len(c.tag)-1]})
			c.tagIx = append(c.tagIx, format.TagIndex(id))
		}
	}
	c.res = append(c.res, proto.ResultColumn{Name: "_count", Data: &c.cnt})
	for i := 0; i < meta.Vals; i++ {
		c.res = append(c.res, proto.ResultColumn{Name: "_val" + strconv.Itoa(i), Data: &c.val[i]})
	}
	if meta.MinMaxHost {
		if meta.Version == Version1 {
			c.res = append(c.res, proto.ResultColumn{Name: "_minHost", Data: &c.minMaxHostV1[0]})
			c.res = append(c.res, proto.ResultColumn{Name: "_maxHost", Data: &c.minMaxHostV1[1]})
		} else {
			c.res = append(c.res, proto.ResultColumn{Name: "_minHost", Data: &c.minMaxHostV2[0]})
			c.res = append(c.res, proto.ResultColumn{Name: "_maxHost", Data: &c.minMaxHostV2[1]})
		}
	}
	return c
}

func (c *pointsSelectCols) rowAt(i int) TsSelectRow {
	row := TsSelectRow{
		Time:     c.time[i],
		StepSec:  c.step[i],
		TsValues: TsValues{CountNorm: c.cnt[i]},
	}
	for j := 0; j < len(c.val); j++ {
		row.Val[j] = c.val[j][i]
	}
	for j := range c.tag {
		row.Tag[c.tagIx[j]] = c.tag[j][i]
	}
	if c.tagStr.Pos != nil && i < len(c.tagStr.Pos) {
		copy(row.TagStr[:], c.tagStr.Buf[c.tagStr.Pos[i].Start:c.tagStr.Pos[i].End])
	}
	if len(c.minMaxHostV2[0]) != 0 {
		row.Host[0] = c.minMaxHostV2[0][i]
	}
	if len(c.minMaxHostV2[1]) != 0 {
		row.Host[1] = c.minMaxHostV2[1][i]
	}
	if c.shardNum != nil {
		row.ShardNum = c.shardNum[i]
	}
	return row
}

func (c *pointsSelectCols) rowAtPoint(i int) PSelectRow {
	row := PSelectRow{
		TsValues: TsValues{CountNorm: c.cnt[i]},
	}
	for j := 0; j < len(c.val); j++ {
		row.Val[j] = c.val[j][i]
	}
	for j := range c.tag {
		row.Tag[c.tagIx[j]] = c.tag[j][i]
	}
	if c.tagStr.Pos != nil && i < len(c.tagStr.Pos) {
		copy(row.TagStr[:], c.tagStr.Buf[c.tagStr.Pos[i].Start:c.tagStr.Pos[i].End])
	}
	if len(c.minMaxHostV2[0]) != 0 {
		row.Host[0] = c.minMaxHostV2[0][i]
	}
	if len(c.minMaxHostV2[1]) != 0 {
		row.Host[1] = c.minMaxHostV2[1][i]
	}
	return row
}

func replaceInfNan(v *float64) {
	if math.IsNaN(*v) {
		*v = -1.111111 // Motivation - 99.9% of our graphs are >=0, -1.111111 will stand out. But we do not expect NaNs.
		return
	}
	if math.IsInf(*v, 1) {
		*v = -2.222222 // Motivation - as above, distinct value for debug
		return
	}
	if math.IsInf(*v, -1) {
		*v = -3.333333 // Motivation - as above, distinct value for debug
		return
	}
	// Motivation - we store some values as float32 anyway. Also, most code does not work well, if close to float64 limits
	if *v > math.MaxFloat32 {
		*v = math.MaxFloat32
		return
	}
	if *v < -math.MaxFloat32 {
		*v = -math.MaxFloat32
		return
	}
}

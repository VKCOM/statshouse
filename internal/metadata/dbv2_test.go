// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/sqlite"

	binlog2 "github.com/VKCOM/statshouse/internal/vkgo/binlog"
	"github.com/VKCOM/statshouse/internal/vkgo/binlog/fsbinlog"

	"github.com/stretchr/testify/require"
)

type Logger struct{}

func (*Logger) Tracef(format string, args ...interface{}) {
	fmt.Printf("Trace: "+format+"\n", args...)
}
func (*Logger) Debugf(format string, args ...interface{}) {
	fmt.Printf("Debug: "+format+"\n", args...)
}
func (*Logger) Infof(format string, args ...interface{}) {
	fmt.Printf("Info: "+format+"\n", args...)
}
func (*Logger) Warnf(format string, args ...interface{}) {
	fmt.Printf("Warn: "+format+"\n", args...)
}
func (*Logger) Errorf(format string, args ...interface{}) {
	fmt.Printf("Error: "+format+"\n", args...)
}

func defaultOptions() *Options {
	return &Options{
		MaxBudget:   500,
		StepSec:     60 * 60,
		BudgetBonus: 100,
	}
}

func initD1b(t *testing.T, dir string, dbFile string, createBl bool, options *Options) (*DBV2, binlog2.Binlog) {
	if options == nil {
		options = defaultOptions()
	}
	boptions := fsbinlog.Options{
		PrefixPath: dir,
		Magic:      3456,
	}
	if createBl {
		_, err := fsbinlog.CreateEmptyFsBinlog(boptions)
		require.NoError(t, err)
	}
	bl, err := fsbinlog.NewFsBinlog(&Logger{}, boptions)
	require.NoError(t, err)

	db, err := OpenDB(dir+"/"+dbFile, *options, bl)
	require.NoError(t, err)
	return db, bl
}

const metadata = "meta"

func Test_SaveMetric(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	e, err := db.SaveEntity(context.Background(), "a", 0, 0, "{}", true, 0, format.MetricEvent, metadata)
	require.NoError(t, err)
	require.Equal(t, metadata, e.Metadata)
	_, err = db.SaveEntity(context.Background(), "a", 0, 0, "{}", true, 0, format.MetricEvent, metadata)
	require.Error(t, err)
}

func Test_GetOldVersion(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	e, err := db.SaveEntity(context.Background(), "a", 0, 0, "{}", true, 0, format.MetricEvent, metadata)
	require.NoError(t, err)
	require.Equal(t, metadata, e.Metadata)
	_, err = db.SaveEntity(context.Background(), "b", e.Id, e.Version, "{}", false, 0, format.MetricEvent, metadata)
	require.NoError(t, err)
	eActual, err := db.GetEntityVersioned(context.Background(), e.Id, e.Version)
	require.NoError(t, err)
	require.Equal(t, e, eActual)
}

func Test_GetShortInfo(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	e, err := db.SaveEntity(context.Background(), "a", 0, 0, "{}", true, 0, format.MetricEvent, metadata)
	require.NoError(t, err)
	require.Equal(t, metadata, e.Metadata)
	e1, err := db.SaveEntity(context.Background(), "b", e.Id, e.Version, "{}", false, 0, format.MetricEvent, "b")
	require.NoError(t, err)
	history, err := db.GetHistoryShort(context.Background(), e.Id)
	require.NoError(t, err)
	require.Len(t, history.Events, 2)
	require.Equal(t, tlmetadata.HistoryShortResponseEvent{
		Version:  e1.Version,
		Metadata: e1.Metadata,
	}, history.Events[0])
	require.Equal(t, tlmetadata.HistoryShortResponseEvent{
		Version:  e.Version,
		Metadata: e.Metadata,
	}, history.Events[1])
}

func Test_RenameMetric(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	e, err := db.SaveEntity(context.Background(), "a", 0, 0, "{}", true, 0, format.MetricEvent, "")
	require.NoError(t, err)
	updates, err := db.JournalEvents(context.Background(), 0, 100)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	m := updates[0]
	require.Equal(t, "a", m.Name)
	require.Equal(t, "{}", m.Data)
	e1, err := db.SaveEntity(context.Background(), "b", e.Id, e.Version, "{}", false, 0, format.MetricEvent, "")
	require.NoError(t, err)
	require.Equal(t, e.Id, e1.Id)
	updates, err = db.JournalEvents(context.Background(), e.Version, 100)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	m = updates[0]
	require.Equal(t, e.Id, m.Id)
	require.Equal(t, "b", m.Name)
	require.Equal(t, "{}", m.Data)
}

func Test_SaveMetric_WithInvalidVersion(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	_, err := db.SaveEntity(context.Background(), "a", 0, 0, "{}", true, 0, format.MetricEvent, "")
	require.NoError(t, err)
	updates, err := db.JournalEvents(context.Background(), 0, 100)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	m := updates[0]
	require.Equal(t, "a", m.Name)
	require.Equal(t, "{}", m.Data)
	_, err = db.SaveEntity(context.Background(), m.Name, m.Id, m.Version+1, m.Data, false, 0, format.MetricEvent, "")
	require.Equal(t, errInvalidMetricVersion, err)
}

func Test_SaveMetric_WithBadName(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	_, err := db.SaveEntity(context.Background(), "a", 0, 0, "{}", true, 0, format.MetricEvent, "")
	require.NoError(t, err)
	updates, err := db.JournalEvents(context.Background(), 0, 100)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	m := updates[0]
	require.Equal(t, "a", m.Name)
	require.Equal(t, "{}", m.Data)
	_, err = db.SaveEntity(context.Background(), "a", 0, 0, "{}", true, 0, format.MetricEvent, "")
	require.ErrorIs(t, err, errMetricIsExist)
}

func Test_SaveMetric_WithBadNamespace(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	_, err := db.SaveEntity(context.Background(), "a"+format.NamespaceSeparator+"a", 0, 0, "{}", true, 0, format.MetricEvent, "")
	require.ErrorIs(t, err, errNamespaceNotExists)
}

func Test_CreateMetricInNamespaceWithGoodName(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	namespace, err := db.SaveEntity(context.Background(), "a", 0, 0, "{}", true, 0, format.NamespaceEvent, "")
	require.NoError(t, err)
	e, err := db.SaveEntity(context.Background(), "a"+format.NamespaceSeparator+"a", 0, 0, "{}", true, 0, format.MetricEvent, "")
	require.NoError(t, err)
	require.Equal(t, namespace.Id, e.NamespaceId)
}

func Test_RenameNamespace(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	namespace, err := db.SaveEntity(context.Background(), "a", 0, 0, "{}", true, 0, format.NamespaceEvent, "")
	require.NoError(t, err)
	_, err = db.SaveEntity(context.Background(), "b", namespace.Id, namespace.Version, "{}", false, 0, format.NamespaceEvent, "")
	require.Error(t, err)
}

func Test_SaveMetric_Delete(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	_, err := db.SaveEntity(context.Background(), "a", 0, 0, "{}", true, 0, format.MetricEvent, "")
	require.NoError(t, err)
	updates, err := db.JournalEvents(context.Background(), 0, 100)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	m := updates[0]
	require.Equal(t, "a", m.Name)
	require.Equal(t, "{}", m.Data)
	r, err := db.SaveEntity(context.Background(), m.Name, m.Id, m.Version, m.Data, false, 1, format.MetricEvent, "")
	require.NoError(t, err)
	updates, err = db.JournalEvents(context.Background(), m.Version, 100)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	m = updates[0]
	require.Equal(t, "a", m.Name)
	require.Equal(t, `{}`, m.Data)
	require.Equal(t, r.Id, m.Id)
	require.Greater(t, m.Unused, uint32(0))
}

func Test_SaveEntity_PredefinedEntity(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	_, err := db.SaveEntity(context.Background(), "a", -1, 0, "{}", false, 0, format.MetricEvent, "")
	require.NoError(t, err)
	updates, err := db.JournalEvents(context.Background(), 0, 100)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	m := updates[0]
	require.Equal(t, "a", m.Name)
	require.Equal(t, "{}", m.Data)
	require.Equal(t, int64(-1), m.Id)

	_, err = db.SaveEntity(context.Background(), m.Name, m.Id, m.Version, `{"a": 1}`, false, 0, format.MetricEvent, "")
	require.NoError(t, err)
	updates, err = db.JournalEvents(context.Background(), m.Version, 100)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	m = updates[0]
	require.Equal(t, "a", m.Name)
	require.Equal(t, `{"a": 1}`, m.Data)
	require.Equal(t, int64(-1), m.Id)
}

func Test_RenameGroup(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	group, err := db.SaveEntity(context.Background(), "abc_", 0, 0, "{}", true, 0, format.MetricsGroupEvent, "")
	require.NoError(t, err)
	_, err = db.SaveEntity(context.Background(), "abca_", group.Id, group.Version, "{}", false, 0, format.MetricsGroupEvent, "")
	require.NoError(t, err)
}

//now any non-empty group name works
//func Test_SaveGroup_With_Bad_Name(t *testing.T) {
//	path := t.TempDir()
//	db, _ := initD1b(t, path, "db", true, nil)
//	_, err := db.SaveEntity(context.Background(), "abc_", 0, 0, "{}", true, false, format.MetricsGroupEvent, "")
//	require.NoError(t, err)
//	_, err = db.SaveEntity(context.Background(), "abc", 0, 0, "{}", true, false, format.MetricsGroupEvent, "")
//	require.Error(t, err)
//}
//func Test_SaveGroup_With_Bad_Name1(t *testing.T) {
//	path := t.TempDir()
//	db, _ := initD1b(t, path, "db", true, nil)
//	_, err := db.SaveEntity(context.Background(), "abc_", 0, 0, "{}", true, false, format.MetricsGroupEvent, "")
//	require.NoError(t, err)
//	_, err = db.SaveEntity(context.Background(), "abc_d", 0, 0, "{}", true, false, format.MetricsGroupEvent, "")
//	require.Error(t, err)
//
//}

func unpackGetMappingUnion(u tlmetadata.GetMappingResponse, err error) (int32, error) {
	if err != nil {
		return 0, err
	}
	resp, ok := u.AsCreated()
	if !ok {
		resp1, ok := u.AsGetMappingResponse()
		if !ok {
			return 0, fmt.Errorf("bad response")
		}
		return resp1.Id, nil
	}
	return resp.Id, nil
}

func unpackInvertMappingUnion(u tlmetadata.GetInvertMappingResponse, err error) (string, error) {
	if err != nil {
		return "", err
	}
	resp, ok := u.AsGetInvertMappingResponse()
	if !ok {
		return "", fmt.Errorf("bad response")
	}
	return resp.Key, nil
}

func TestDB_GetOrCreateMapping(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)

	t.Run("create mapping", func(t *testing.T) {
		mapping, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc", "k"))
		require.NoError(t, err)
		require.Greater(t, mapping, int32(0))
	})
	t.Run("get mapping", func(t *testing.T) {
		mapping, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc1", "k1"))
		require.NoError(t, err)
		require.Greater(t, mapping, int32(0))
		mapping1, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc1", "k1"))
		require.NoError(t, err)
		require.Equal(t, mapping, mapping1)
	})

	t.Run("put mapping", func(t *testing.T) {
		err := db.PutMapping(context.Background(), []string{"k20"}, []int32{4423})
		require.NoError(t, err)
		mapping1, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "ab", "k20"))
		require.NoError(t, err)
		require.Equal(t, int32(4423), mapping1)
		err = db.PutMapping(context.Background(), []string{"k20"}, []int32{4424})
		require.NoError(t, err)
		mapping1, err = unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "ab", "k20"))
		require.NoError(t, err)
		require.Equal(t, int32(4424), mapping1)
	})

	t.Run("get mapping 1", func(t *testing.T) {
		mapping, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc4", "k9"))
		require.NoError(t, err)
		require.Greater(t, mapping, int32(0))
		mapping1, _, err := db.GetMappingByValue(context.Background(), "k9")
		require.NoError(t, err)
		require.Equal(t, mapping, mapping1)
		mapping2, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc4", "k9"))
		require.NoError(t, err)
		require.Equal(t, mapping, mapping2)
	})

	t.Run("exceed flood limit", func(t *testing.T) {
		db.maxBudget = 2
		db.budgetBonus = 2
		db.stepSec = 5
		var now time.Time
		db.now = func() time.Time {
			return now
		}
		now = time.Unix(1641027722, 0)
		mapping, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc2", "k3"))
		require.NoError(t, err)
		require.Greater(t, mapping, int32(0))
		now = time.Unix(1641027723, 0)
		mapping1, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc2", "k4"))
		require.NoError(t, err)
		require.Greater(t, mapping1, mapping)
		now = time.Unix(1641027724, 49)
		resp, err := db.GetOrCreateMapping(context.Background(), "abc2", "k5")
		require.NoError(t, err)
		require.True(t, resp.IsFloodLimitError())
	})
	t.Run("flood limit was freshed", func(t *testing.T) {
		db.maxBudget = 2
		db.budgetBonus = 2
		db.stepSec = 5
		var now time.Time
		db.now = func() time.Time {
			return now
		}
		now = time.Unix(1641027722, 0)
		mapping, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc3", "k6"))
		require.NoError(t, err)
		require.Greater(t, mapping, int32(0))
		now = time.Unix(1641027723, 0)
		mapping1, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc3", "k7"))
		require.NoError(t, err)
		require.Greater(t, mapping1, mapping)
		now = time.Unix(1641027724, 0)
		_, err = unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc3", "k8"))
		require.Error(t, err)
		now = time.Unix(1641027725, 0)
		mapping2, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc3", "k8"))
		require.NoError(t, err)
		require.Greater(t, mapping2, mapping1)
	})
	t.Run("skip flood limit", func(t *testing.T) {
		db.maxBudget = 2
		db.budgetBonus = 2
		db.stepSec = 5
		db.globalBudget = 2
		var now time.Time
		db.now = func() time.Time {
			return now
		}
		now = time.Unix(1641027722, 0)
		mapping, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc53", "k9"))
		require.NoError(t, err)
		require.Greater(t, mapping, int32(0))
		now = time.Unix(1641027723, 0)
		mapping1, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc53", "k10"))
		require.NoError(t, err)
		require.Greater(t, mapping1, mapping)
		now = time.Unix(1641027724, 49)
		mapping2, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc53", "k11"))
		require.NoError(t, err)
		require.Greater(t, mapping2, mapping1)
	})
}

func TestDB_GetNewMappings(t *testing.T) {
	ctx := context.Background()
	newDB := func(t *testing.T) *DBV2 {
		path := t.TempDir()
		db, _ := initD1b(t, path, "db", true, nil)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})
		return db
	}

	t.Run("empty database", func(t *testing.T) {
		db := newDB(t)
		mappings, maxID, err := db.GetNewMappings(ctx, 0, 10, []int32{})
		require.NoError(t, err)
		require.Empty(t, mappings)
		require.Equal(t, int32(0), maxID)
	})

	t.Run("filters by id and respects page limit", func(t *testing.T) {
		db := newDB(t)
		require.NoError(t, db.PutMapping(ctx, []string{"tag1", "tag2", "tag3"}, []int32{1, 3, 5}))

		mappings, maxID, err := db.GetNewMappings(ctx, 1, 10, []int32{})
		require.NoError(t, err)
		require.Equal(t, int32(5), maxID)
		require.Equal(t, []tlstatshouse.Mapping{
			{Value: 3, Str: "tag2"},
			{Value: 5, Str: "tag3"},
		}, mappings)

		pageLimited, maxIDLimited, err := db.GetNewMappings(ctx, 0, 1, []int32{})
		require.NoError(t, err)
		require.Equal(t, int32(5), maxIDLimited)
		require.Equal(t, []tlstatshouse.Mapping{
			{Value: 1, Str: "tag1"},
		}, pageLimited)

		emptyResp, maxIDSame, err := db.GetNewMappings(ctx, 5, 10, []int32{})
		require.NoError(t, err)
		require.Empty(t, emptyResp)
		require.Equal(t, int32(5), maxIDSame)
	})
}

func TestDB_ResetFlood(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	t.Run("exceed flood limit", func(t *testing.T) {
		db.maxBudget = 1
		db.budgetBonus = 2
		db.stepSec = 5
		var now time.Time
		db.now = func() time.Time {
			return now
		}
		now = time.Unix(1641027722, 0)
		mapping, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc2", "k3"))
		require.NoError(t, err)
		require.Greater(t, mapping, int32(0))
		now = time.Unix(1641027724, 0)
		resp, err := db.GetOrCreateMapping(context.Background(), "abc2", "k5")
		require.NoError(t, err)
		require.True(t, resp.IsFloodLimitError())
		before, after, err := db.ResetFlood(context.Background(), "abc2", 0)
		require.NoError(t, err)
		require.Equal(t, db.maxBudget, after)
		require.Equal(t, int64(0), before)
		mapping1, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc2", "k5"))
		require.NoError(t, err)
		require.Greater(t, mapping1, mapping)
	})
}

func TestDB_ResetFlood_With_Limit(t *testing.T) {
	path := t.TempDir()
	const limit = 9900
	db, _ := initD1b(t, path, "db", true, nil)
	checkLimit := func(limit int64) {
		var actualLimit int64
		err := db.eng.Do(context.Background(), "test", func(conn sqlite.Conn, bytes []byte) ([]byte, error) {
			rows := conn.Query("test", "SELECT count_free FROM flood_limits WHERE metric_name = $m", sqlite.BlobString("$m", "abc2"))
			if rows.Next() {
				actualLimit, _ = rows.ColumnInt64(0)
			}
			return nil, rows.Error()
		})
		require.NoError(t, err)
		require.Equal(t, int64(limit), actualLimit)
	}
	t.Run("exceed flood limit", func(t *testing.T) {
		before, after, err := db.ResetFlood(context.Background(), "abc2", limit)
		require.NoError(t, err)
		require.Equal(t, db.maxBudget, before)
		require.Equal(t, int64(limit), after)
		checkLimit(limit)
	})
	t.Run("create mapping when limit greater than max", func(t *testing.T) {
		resp, err := db.GetOrCreateMapping(context.Background(), "abc2", "aaaa")
		require.NoError(t, err)
		require.True(t, resp.IsCreated())
		checkLimit(limit - 1)
	})
}

func TestDB_GetKeyMapping(t *testing.T) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)

	t.Run("get mapping", func(t *testing.T) {
		const k = "k1"
		mapping, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc1", k))
		require.NoError(t, err)
		require.Greater(t, mapping, int32(0))
		key, isExists, err := db.GetMappingByID(context.Background(), mapping)
		require.NoError(t, err)
		require.True(t, isExists)
		require.Equal(t, k, key)
	})

	t.Run("get not existing mapping", func(t *testing.T) {
		const k = "k1"
		mapping, err := unpackGetMappingUnion(db.GetOrCreateMapping(context.Background(), "abc1", k))
		require.NoError(t, err)
		require.Greater(t, mapping, int32(0))
		_, isExists, err := db.GetMappingByID(context.Background(), mapping+7)
		require.NoError(t, err)
		require.False(t, isExists)
	})
}

func Test_getPred(t *testing.T) {
	var step uint32 = 5
	tests := []struct {
		name     string
		now      time.Time
		wantPred uint32
	}{
		{name: "", now: time.Unix(999, 0), wantPred: 995},
		{name: "", now: time.Unix(1000, 0), wantPred: 1000},
		{name: "", now: time.Unix(1001, 0), wantPred: 1000},
		{name: "", now: time.Unix(1002, 0), wantPred: 1000},
		{name: "", now: time.Unix(1003, 0), wantPred: 1000},
		{name: "", now: time.Unix(1004, 0), wantPred: 1000},
		{name: "", now: time.Unix(1005, 0), wantPred: 1005},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotPred := roundTime(tt.now, step); gotPred != tt.wantPred {
				t.Errorf("roundTime() = %v, want %v", gotPred, tt.wantPred)
			}
		})
	}
}

func Test_calcBudget(t *testing.T) {
	type args struct {
		oldBudget      int64
		expense        int64
		lastTimeUpdate uint32
		now            uint32
		max            int64
		stepSec        uint32
		bonusToStep    int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{name: "", args: args{oldBudget: 1, expense: 1, lastTimeUpdate: 100, now: 105, max: 500, stepSec: 5, bonusToStep: 5}, want: 5},
		{name: "", args: args{oldBudget: 0, expense: 1, lastTimeUpdate: 100, now: 100, max: 500, stepSec: 5, bonusToStep: 5}, want: -1},
		{name: "", args: args{oldBudget: 0, expense: 1, lastTimeUpdate: 100, now: 105, max: 500, stepSec: 5, bonusToStep: 5}, want: 4},
		{name: "", args: args{oldBudget: 0, expense: 1, lastTimeUpdate: 100, now: 10000000, max: 500, stepSec: 5, bonusToStep: 5}, want: 499},

		{name: "", args: args{oldBudget: 4, expense: 1, lastTimeUpdate: 100, now: 100, max: 500, stepSec: 5, bonusToStep: 5}, want: 3},
		{name: "", args: args{oldBudget: 3, expense: 1, lastTimeUpdate: 100, now: 101, max: 500, stepSec: 5, bonusToStep: 5}, want: 2},
		{name: "", args: args{oldBudget: 2, expense: 1, lastTimeUpdate: 100, now: 102, max: 500, stepSec: 5, bonusToStep: 5}, want: 1},
		{name: "", args: args{oldBudget: 1, expense: 1, lastTimeUpdate: 100, now: 103, max: 500, stepSec: 5, bonusToStep: 5}, want: 0},
		{name: "", args: args{oldBudget: 0, expense: 1, lastTimeUpdate: 100, now: 105, max: 500, stepSec: 5, bonusToStep: 5}, want: 4},
		{name: "", args: args{oldBudget: 1000, expense: 1, lastTimeUpdate: 100, now: 110, max: 500, stepSec: 5, bonusToStep: 5}, want: 999},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := calcBudget(tt.args.oldBudget, tt.args.expense, tt.args.lastTimeUpdate, tt.args.now, tt.args.max, tt.args.bonusToStep, tt.args.stepSec); got != tt.want {
				t.Errorf("calcBudget() = %v, want %v", got, tt.want)
			}
		})
	}
}

func testRereadBinlog(t *testing.T,
	opt1, opt2 *Options,
	path string, dbFile1, dbFile2 string, parallelism int,
	dbHandler func(t *testing.T, dbv2 *DBV2, goroutineNum int),
	validator func(t *testing.T, dbv2 *DBV2),
	clean func(t *testing.T),
) {
	db, _ := initD1b(t, path, dbFile1, true, opt1)
	wg := &sync.WaitGroup{}
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dbHandler(t, db, i)
		}(i)
	}
	wg.Wait()
	require.NoError(t, db.Close())
	db, _ = initD1b(t, path, dbFile2, false, opt2)
	validator(t, db)
	require.NoError(t, db.Close())
	if clean != nil {
		clean(t)
	}
}

func Test_Reread_Binlog_CreateMapping(t *testing.T) {
	test := func(t *testing.T, newDb bool) {
		path := t.TempDir()
		mx := sync.Mutex{}
		mappings := map[string]int32{}
		metricsNames := []string{"a", "b", "c", "d", "e", "f", "j"}
		opt := defaultOptions()
		opt.MaxBudget = 1000000
		opt.GlobalBudget = 500
		newFileName := "db"
		if newDb {
			newFileName = "db1"
		}
		testRereadBinlog(t,
			opt, opt,
			path, "db", newFileName, 50,
			func(t *testing.T, db *DBV2, goroutineNum int) {
				k := strconv.FormatInt(rand.Int63()%10000, 10)
				name := metricsNames[rand.Int()%len(metricsNames)]
				respUnion, err := db.GetOrCreateMapping(context.Background(), name, k)
				require.NoError(t, err)
				require.True(t, respUnion.IsGetMappingResponse() || respUnion.IsCreated())
				mx.Lock()
				defer mx.Unlock()
				resp, ok := respUnion.AsGetMappingResponse()
				if ok {
					mappings[k] = resp.Id
				} else {
					resp, _ := respUnion.AsCreated()
					mappings[k] = resp.Id
				}
			}, func(t *testing.T, db *DBV2) {
				mx.Lock()
				defer mx.Unlock()
				for k, id := range mappings {
					resp, err := db.GetOrCreateMapping(context.Background(), "a", k)
					require.NoError(t, err)
					require.True(t, resp.IsGetMappingResponse() || resp.IsCreated())
					resp1, ok := resp.AsGetMappingResponse()
					if ok {
						require.Equal(t, id, resp1.Id)
					} else {
						resp1, _ := resp.AsCreated()
						require.Equal(t, id, resp1.Id)
					}
				}
			}, nil)
	}
	t.Run("reread from empty db", func(t *testing.T) {
		test(t, true)
	})
	t.Run("reread from old snapshot", func(t *testing.T) {
		test(t, false)
	})
}

func TestDB_deleteMappingsByIdBatched(t *testing.T) {
	ctx := context.Background()
	newDB := func(t *testing.T) *DBV2 {
		path := t.TempDir()
		db, _ := initD1b(t, path, "db", true, nil)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}

	populateMappings := func(t *testing.T, db *DBV2, ids ...int32) {
		keys := make([]string, len(ids))
		for i, id := range ids {
			keys[i] = "k" + strconv.Itoa(int(id))
		}
		require.NoError(t, db.PutMapping(ctx, keys, ids))
	}

	mappingExists := func(t *testing.T, db *DBV2, id int32) bool {
		_, ok, err := db.GetMappingByID(ctx, id)
		require.NoError(t, err)
		return ok
	}

	t.Run("correct empty input", func(t *testing.T) {
		db := newDB(t)
		populateMappings(t, db, 1, 2, 3)
		cnt, err := db.deleteMappingsByIdBatched(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, int32(0), cnt)
		require.True(t, mappingExists(t, db, 1))
	})

	t.Run("deletes exactly the requested present ids", func(t *testing.T) {
		db := newDB(t)
		populateMappings(t, db, 1, 2, 3, 4, 5)
		cnt, err := db.deleteMappingsByIdBatched(ctx, []int32{2, 4})
		require.NoError(t, err)
		require.Equal(t, int32(2), cnt)
		require.True(t, mappingExists(t, db, 1))
		require.False(t, mappingExists(t, db, 2))
		require.True(t, mappingExists(t, db, 3))
		require.False(t, mappingExists(t, db, 4))
		require.True(t, mappingExists(t, db, 5))
	})

	t.Run("countBeforeDeletion counts only present ids", func(t *testing.T) {
		db := newDB(t)
		populateMappings(t, db, 1, 2, 3)
		// 2 and 3 exist, 99 and 100 do not
		cnt, err := db.deleteMappingsByIdBatched(ctx, []int32{2, 3, 99, 100})
		require.NoError(t, err)
		require.Equal(t, int32(2), cnt)
		require.True(t, mappingExists(t, db, 1))
		require.False(t, mappingExists(t, db, 2))
		require.False(t, mappingExists(t, db, 3))
	})

	t.Run("rejects oversized batch", func(t *testing.T) {
		db := newDB(t)
		ids := make([]int32, maxDeletionSizeLimit+1)
		for i := range ids {
			ids[i] = int32(i + 1)
		}
		_, err := db.deleteMappingsByIdBatched(ctx, ids)
		require.Error(t, err)
	})
}

func Test_Reread_Binlog_DeleteMappings(t *testing.T) {
	ctx := context.Background()
	test := func(t *testing.T, newDb bool) {
		path := t.TempDir()
		newFileName := "db"
		if newDb {
			newFileName = "db1"
		}

		db, _ := initD1b(t, path, "db", true, nil)

		presentMappings := []int32{1, 2, 3, 4, 5, 6, 7}
		deletionMappings := []int32{2, 4, 6}
		expectedRemainingMappings := []int32{1, 3, 5, 7}

		keys := make([]string, len(presentMappings))
		for i, id := range presentMappings {
			keys[i] = "key" + strconv.Itoa(int(id))
		}
		require.NoError(t, db.PutMapping(ctx, keys, presentMappings))

		cnt, err := db.deleteMappingsByIdBatched(ctx, deletionMappings)
		require.NoError(t, err)
		require.Equal(t, int32(len(deletionMappings)), cnt)

		require.NoError(t, db.Close())

		db, _ = initD1b(t, path, newFileName, false, nil)
		defer func() { require.NoError(t, db.Close()) }()

		for _, id := range deletionMappings {
			_, ok, err := db.GetMappingByID(ctx, id)
			require.NoError(t, err)
			require.Falsef(t, ok, "mapping id %d shouldn't be present after binlog reread", id)
		}
		for _, id := range expectedRemainingMappings {
			name, ok, err := db.GetMappingByID(ctx, id)
			require.NoError(t, err)
			require.Truef(t, ok, "mapping id %d should be present after binlog reread", id)
			require.Equal(t, "key"+strconv.Itoa(int(id)), name)
		}
	}

	t.Run("reread from empty db", func(t *testing.T) { test(t, true) })
	t.Run("reread from old snapshot", func(t *testing.T) { test(t, false) })
}

func Test_Reread_Binlog_SaveMetric(t *testing.T) {
	test := func(t *testing.T, newDb bool) {
		path := t.TempDir()
		const task = 3
		mx := sync.Mutex{}
		metrics := map[string][]tlmetadata.Event{}
		metricsNames := []string{"a", "b", "c", "d"}
		opt := defaultOptions()
		dbFile2 := "db"
		if newDb {
			dbFile2 = "db1"
		}
		testRereadBinlog(t, opt, opt, path, "db", dbFile2, 100,
			func(t *testing.T, db *DBV2, i int) {
				suffix := strconv.FormatInt(int64(i), 10)
				name := metricsNames[rand.Int()%len(metricsNames)] + suffix
				for i := 0; i < task; i++ {
					mx.Lock()
					metricL, notCreate := metrics[name]
					var id int64
					var version int64
					if notCreate {
						id = metricL[0].Id
						version = metricL[0].Version
					}
					mx.Unlock()
					metric, err := db.SaveEntity(context.Background(), name, id, version, "{}", !notCreate, 0, format.MetricEvent, "")
					require.NoError(t, err)
					mx.Lock()
					metrics[name] = append([]tlmetadata.Event{metric}, metricL...)
					mx.Unlock()
				}
			}, func(t *testing.T, db *DBV2) {
				metric1, err := db.JournalEvents(context.Background(), 0, 100000)
				require.NoError(t, err)
				require.Equal(t, len(metrics), len(metric1))
				for _, metric := range metric1 {
					metric.SetMetadata("")
					m := metrics[metric.Name][0]
					require.Equal(t, m, metric)
				}
				for _, ms := range metrics {
					e := ms[0]
					history, err := db.GetHistoryShort(context.Background(), e.Id)
					require.NoError(t, err)
					require.Len(t, history.Events, len(ms))
					for i := range ms {
						require.Equal(t, tlmetadata.HistoryShortResponseEvent{
							Version:  ms[i].Version,
							Metadata: ms[i].Metadata,
						}, history.Events[i])
					}
				}
			}, nil)
	}
	t.Run("reread with new db", func(t *testing.T) {
		test(t, true)
	})
	t.Run("reread with old db", func(t *testing.T) {
		test(t, false)
	})
}

func Test_Migration(t *testing.T) {
	t.SkipNow()
	path := t.TempDir()
	const task = 3
	mx := sync.Mutex{}
	metrics := map[string]tlmetadata.Event{}
	metricsNames := []string{"a", "b", "c", "d"}
	opt1 := defaultOptions()
	opt2 := defaultOptions()
	opt2.Migration = true
	testRereadBinlog(t, opt1, opt2, path, "db", "db", 100,
		func(t *testing.T, db *DBV2, i int) {
			suffix := strconv.FormatInt(int64(i), 10)
			name := metricsNames[rand.Int()%len(metricsNames)] + suffix
			for i := 0; i < task; i++ {
				mx.Lock()
				delete := false
				metric, notCreate := metrics[name]
				mx.Unlock()
				metric, err := db.SaveEntityold(context.Background(), name, metric.Id, metric.Version, "{}", !notCreate, delete, format.MetricEvent)
				require.NoError(t, err)
				mx.Lock()
				metrics[name] = metric
				mx.Unlock()
			}
		}, func(t *testing.T, db *DBV2) {
			require.Greater(t, len(metrics), 0)
			metric1, err := db.JournalEvents(context.Background(), 0, 100000)
			require.NoError(t, err)
			require.Equal(t, len(metrics), len(metric1))
			for _, metric := range metric1 {
				m, ok := metrics[metric.Name]
				require.True(t, ok)
				require.Equal(t, m, metric)
			}

		}, nil)
}

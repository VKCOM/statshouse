// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package mapping

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/pcache"
)

const (
	metricMapQueueSize = 1000
)

func nameToMapping(name string) uint32 {
	b, err := base64.StdEncoding.DecodeString(name)
	if err != nil {
		panic(err)
	}
	mapping := binary.LittleEndian.Uint32(b)
	return mapping
}

func mappingToName(mapping uint32) string {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, mapping)
	return base64.StdEncoding.EncodeToString(b)
}

func TestMappingReviersible(t *testing.T) {
	var mapping uint32 = 1000
	name := mappingToName(mapping)
	restoredMapping := nameToMapping(name)
	if mapping != restoredMapping {
		t.Errorf("%d != %d", mapping, restoredMapping)
	}
}

func hasBit(n uint32, pos uint) bool {
	val := n & (1 << pos)
	return (val > 0)
}

func loadMapping(ctx context.Context, key string, extra interface{}) (pcache.Value, time.Duration, error) {
	return pcache.Int32ToValue(int32(nameToMapping(key))), time.Second, nil
}

func setUp(dc *pcache.DiskCache, ac *AutoCreate, mapCallback data_model.MapCallbackFunc) *Mapper {
	mapper := NewMapper("test-cluster", loadMapping, dc, ac, metricMapQueueSize, mapCallback)
	return mapper
}

func setUpSimple() *Mapper {
	return setUp(nil, nil, func(tlstatshouse.MetricBytes, data_model.MappedMetricHeader) {
		panic("slow pipeline called")
	})
}

func tearDown(mapper *Mapper) {
	mapper.Stop()
}

type MetricGenerator struct {
	rng     rand.Rand
	metricN int32
	tagN    int32
}

func (mg MetricGenerator) generateMetric(now time.Time) (m tlstatshouse.MetricBytes) {
	metricId := mg.rng.Int31n(mg.metricN)
	flags := mg.rng.Uint32()
	hasCounter := hasBit(flags, 0)
	hasTs := hasBit(flags, 1)
	hasValue := hasBit(flags, 2)
	hasUnique := hasBit(flags, 3)
	tsInPast := hasBit(flags, 4)
	tagCnt := mg.rng.Int31n(16)

	m.Name = []byte(fmt.Sprint("metric-", metricId))
	m.Tags = make([]tl.DictionaryFieldStringBytes, tagCnt)
	for i := 0; i < int(tagCnt); i++ {
		mapping := mg.rng.Int31n(mg.tagN)
		name := mappingToName(uint32(mapping))
		m.Tags[i].Key = []byte(fmt.Sprint("key", i))
		m.Tags[i].Value = []byte(name)
	}
	if hasTs {
		ts := now
		if tsInPast {
			ts = ts.Add(-time.Minute)
		}
		m.SetTs(uint32(ts.Unix()))
	}
	if hasCounter {
		m.SetCounter(1)
	}
	if hasValue {
		m.SetValue([]float64{10})
	}
	if hasUnique {
		m.SetUnique([]int64{1, 2, 3})
	}
	return m
}

func TestSimpleMapper_Map(t *testing.T) {
	mapper := setUpSimple()
	defer tearDown(mapper)

	var args data_model.HandlerArgs
	args.MetricBytes = &tlstatshouse.MetricBytes{
		Name: []byte("metric1"),
	}
	args.Description = "test description"
	h, done := mapper.Map(args, nil)
	if !done {
		t.Fatal("Map failed")
	}
	_ = h
}

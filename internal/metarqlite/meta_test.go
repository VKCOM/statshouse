// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metarqlite

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
	"pgregory.net/rand"
)

// these tests need running rqlite node
const testAddresses = "localhost:4001"

func newTestRQLiteLoader() *RQLiteLoader {
	return NewRQliteLoader(testAddresses, 60*time.Second, nil)
}

func TestCreateSchema(t *testing.T) {
	t.Skip()
	loader := newTestRQLiteLoader()

	err := loader.CreateSchema(context.Background())
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetMappings(t *testing.T) {
	t.Skip()
	loader := newTestRQLiteLoader()

	ma, cur, last, err := loader.GetNewMappings(context.Background(), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%v %d %d\n", ma, cur, last)
}

func TestCreateMappings(t *testing.T) {
	t.Skip()
	loader := newTestRQLiteLoader()

	tag := fmt.Sprintf("str%d", rand.Uint64())

	tagValue, status, err := loader.GetTagMapping(context.Background(), tag, "metric", true)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%d %d\n", tagValue, status)

	tagValue, status, err = loader.GetTagMapping(context.Background(), tag, "metric", true)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%d %d\n", tagValue, status)
}

func TestResetFlood(t *testing.T) {
	t.Skip()
	loader := newTestRQLiteLoader()

	before, after, err := loader.ResetFlood(context.Background(), "metric", 2)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%d %d\n", before, after)
}

func TestLoadJournal(t *testing.T) {
	t.Skip()
	loader := newTestRQLiteLoader()

	ma, last, err := loader.LoadJournal(context.Background(), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%v %d\n", ma, last)
	if len(ma) == 0 {
		return
	}
	ma, last, err = loader.LoadJournal(context.Background(), ma[len(ma)-1].Version, true)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%v %d\n", ma, last)
}

func TestEditMetricNormal(t *testing.T) {
	t.Skip()
	loader := newTestRQLiteLoader()

	metric := "metric12350" // fmt.Sprintf("metric%d", rand.Uint64())

	event := tlmetadata.Event{
		Name:       metric,
		EventType:  format.MetricEvent,
		Unused:     0,
		UpdateTime: 123, // will be ignored
		Data:       "{}",
		Metadata:   "{}",
	}

	ret, err := loader.SaveEntity(context.Background(), event, false, false)
	fmt.Printf("%v\n%v\n", ret, err)

	v1 := ret

	event.Id = ret.Id
	event.Version = ret.Version - 1

	ret, err = loader.SaveEntity(context.Background(), event, false, false)
	fmt.Printf("%v\n%v\n", ret, err)

	event.Id = ret.Id
	event.Version = ret.Version

	ret, err = loader.SaveEntity(context.Background(), event, false, false)
	fmt.Printf("%v\n%v\n", ret, err)

	v2 := ret

	oldMetric, err := loader.GetMetric(context.Background(), v1.Id, v1.Version)
	fmt.Printf("%v\n%v\n", oldMetric, err)

	oldMetric, err = loader.GetMetric(context.Background(), v2.Id, v2.Version)
	fmt.Printf("%v\n%v\n", oldMetric, err)
}

func TestEditMetricBuiltin(t *testing.T) {
	t.Skip()
	loader := newTestRQLiteLoader()

	metric := "__kitty2"

	event := tlmetadata.Event{
		Id:         -114,
		Name:       metric,
		EventType:  format.MetricsGroupEvent,
		Unused:     0,
		UpdateTime: 123, // will be ignored
		Data:       "{}",
		Metadata:   "{}",
	}

	ret, err := loader.SaveEntity(context.Background(), event, false, false)
	fmt.Printf("%v\n%v\n", ret, err)

	v1 := ret

	event.Id = ret.Id
	event.Version = ret.Version - 1

	ret, err = loader.SaveEntity(context.Background(), event, false, false)
	fmt.Printf("%v\n%v\n", ret, err)

	event.Id = ret.Id
	event.Version = ret.Version

	ret, err = loader.SaveEntity(context.Background(), event, false, false)
	fmt.Printf("%v\n%v\n", ret, err)

	v2 := ret

	oldMetric, err := loader.GetMetric(context.Background(), v1.Id, v1.Version)
	fmt.Printf("%v\n%v\n", oldMetric, err)

	oldMetric, err = loader.GetMetric(context.Background(), v2.Id, v2.Version)
	fmt.Printf("%v\n%v\n", oldMetric, err)
}

func TestCreateManyMappings(t *testing.T) {
	t.Skip()
}

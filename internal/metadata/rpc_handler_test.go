// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

func initServer(t *testing.T, now func() time.Time) (net.Listener, *rpc.Server, *tlmetadata.Client, *Handler) {
	path := t.TempDir()
	db, _ := initD1b(t, path, "db", true, nil)
	db.now = now
	handler := NewHandler(db, "abc", log.Printf)
	proxy := ProxyHandler{}
	h := tlmetadata.Handler{
		RawGetMapping:          proxy.HandleProxy("", handler.RawGetMappingByValue),
		RawGetInvertMapping:    proxy.HandleProxy("", handler.RawGetMappingByID),
		RawEditEntitynew:       proxy.HandleProxy("", handler.RawEditEntity),
		RawGetJournalnew:       proxy.HandleProxy("", handler.RawGetJournal),
		PutTagMappingBootstrap: handler.PutTagMappingBootstrap,
		GetTagMappingBootstrap: handler.GetTagMappingBootstrap,
	}
	server := rpc.NewServer(rpc.ServerWithHandler(h.Handle), rpc.ServerWithLogf(log.Printf))
	t.Cleanup(func() {
		server.Close()
	})

	ln, err := net.Listen("tcp4", fmt.Sprintf(":%d", 0))
	require.NoError(t, err)
	go func() {
		err = server.Serve(ln)
		if err != rpc.ErrServerClosed {
			require.NoError(t, err)
		}
	}()
	rpcClient := rpc.NewClient()
	cl := &tlmetadata.Client{
		Client:  rpcClient,
		Network: "tcp4",
		Address: ln.Addr().String(),
	}
	return ln, server, cl, handler
}

func statJson(name string, id int64, any string) (string, map[string]interface{}) {
	m := map[string]interface{}{}
	jsonResp := fmt.Sprintf(`{"name":"%s","stat_id": %d, "any": "%s"}`, name, id, any)
	err := json.Unmarshal([]byte(jsonResp), &m)
	if err != nil {
		panic(err)
	}
	return jsonResp, m
}

func getMetricsAsync(t *testing.T, rpcClient *tlmetadata.Client, from int64) (chan tlmetadata.GetJournalResponsenew, chan error) {
	cErr := make(chan error)
	cResp := make(chan tlmetadata.GetJournalResponsenew)
	go func() {
		r := tlmetadata.GetJournalResponsenew{}
		err := rpcClient.GetJournalnew(context.Background(), tlmetadata.GetJournalnew{
			From:  from,
			Limit: 1000,
		}, nil, &r)
		if err != nil {
			cErr <- err
		} else {
			cResp <- r
		}
	}()
	return cResp, cErr
}

func getJournalAsync(t *testing.T, rpcClient *tlmetadata.Client, from int64) (chan tlmetadata.GetJournalResponsenew, chan error) {
	cErr := make(chan error)
	cResp := make(chan tlmetadata.GetJournalResponsenew)
	go func() {
		r := tlmetadata.GetJournalResponsenew{}
		err := rpcClient.GetJournalnew(context.Background(), tlmetadata.GetJournalnew{
			From:  from,
			Limit: 1000,
		}, nil, &r)
		if err != nil {
			cErr <- err
		} else {
			cResp <- r
		}
	}()
	return cResp, cErr
}

func getMetrics(t *testing.T, rpcClient *tlmetadata.Client, from int64) (tlmetadata.GetJournalResponsenew, error, bool) {
	cErr := make(chan error, 1)
	cResp := make(chan tlmetadata.GetJournalResponsenew, 1)
	go func() {
		r := tlmetadata.GetJournalResponsenew{}
		err := rpcClient.GetJournalnew(context.Background(), tlmetadata.GetJournalnew{
			From:  from,
			Limit: 1000,
		}, nil, &r)
		if err != nil {
			cErr <- err
		} else {
			cResp <- r
		}
	}()
	select {
	case err := <-cErr:
		return tlmetadata.GetJournalResponsenew{}, err, false
	case resp := <-cResp:
		return resp, nil, false
	case <-time.After(3 * time.Second):
		return tlmetadata.GetJournalResponsenew{}, nil, true
	}
}

func getJournal(t *testing.T, rpcClient *tlmetadata.Client, from int64) (tlmetadata.GetJournalResponsenew, error, bool) {
	cErr := make(chan error, 1)
	cResp := make(chan tlmetadata.GetJournalResponsenew, 1)
	go func() {
		r := tlmetadata.GetJournalResponsenew{}
		err := rpcClient.GetJournalnew(context.Background(), tlmetadata.GetJournalnew{
			From:  from,
			Limit: 1000,
		}, nil, &r)
		if err != nil {
			cErr <- err
		} else {
			cResp <- r
		}
	}()
	select {
	case err := <-cErr:
		return tlmetadata.GetJournalResponsenew{}, err, false
	case resp := <-cResp:
		return resp, nil, false
	case <-time.After(3 * time.Second):
		return tlmetadata.GetJournalResponsenew{}, nil, true
	}
}

func getMapping(rpcClient *tlmetadata.Client, metricName, key string, create bool) (tlmetadata.GetMappingResponseUnion, error) {
	resp := tlmetadata.GetMappingResponseUnion{}
	req := tlmetadata.GetMapping{
		FieldMask: 0,
		Metric:    metricName,
		Key:       key,
	}
	if create {
		req.SetCreateIfAbsent(true)
	}
	err := rpcClient.GetMapping(context.Background(), req, nil, &resp)
	return resp, err
}

func getInvertMapping(rpcClient *tlmetadata.Client, id int32) (tlmetadata.GetInvertMappingResponseUnion, error) {
	resp := tlmetadata.GetInvertMappingResponseUnion{}
	req := tlmetadata.GetInvertMapping{
		Id: id,
	}
	err := rpcClient.GetInvertMapping(context.Background(), req, nil, &resp)
	return resp, err
}

func putMetricTest(t *testing.T, rpcClient *tlmetadata.Client, name string, id, version int64, any string) (tlmetadata.Event, error) {
	jsonStr, m := statJson(name, id, any)
	checkJson(t, jsonStr)
	resp, err := putMetric(t, rpcClient, name, jsonStr, id, version)
	require.NoError(t, err)
	checkMetricResponse(t, resp, m)
	require.NoError(t, err)
	return resp, nil
}

func putJournalTest(t *testing.T, rpcClient *tlmetadata.Client, name string, id, version int64, typ int32, any string, delete bool) (tlmetadata.Event, error) {
	jsonStr, m := statJson(name, id, any)
	checkJson(t, jsonStr)
	resp, err := putJournalEvent(t, rpcClient, name, jsonStr, id, version, typ, delete)
	require.NoError(t, err)
	checkJournalResponse(t, resp, m)
	return resp, nil
}

func putMetric(t *testing.T, rpcClient *tlmetadata.Client, name, json string, id, version int64) (tlmetadata.Event, error) {
	resp := tlmetadata.Event{}
	req := tlmetadata.EditEntitynew{
		Event: tlmetadata.Event{
			Id:      id,
			Name:    name,
			Version: version,
			Data:    json,
		},
	}
	req.SetCreate(version == 0)
	err := rpcClient.EditEntitynew(context.Background(), req, nil, &resp)
	return resp, err
}

func putBootstrap(t *testing.T, rpcClient *tlmetadata.Client, mappings []tlstatshouse.Mapping) (int32, error) {
	res := &tlstatshouse.PutTagMappingBootstrapResult{}
	err := rpcClient.PutTagMappingBootstrap(context.Background(), tlmetadata.PutTagMappingBootstrap{
		Mappings: mappings,
	}, nil, res)
	require.NoError(t, err)
	return res.CountInserted, nil
}

func getBootstrap(t *testing.T, rpcClient *tlmetadata.Client) ([]tlstatshouse.Mapping, error) {
	res := &tlstatshouse.GetTagMappingBootstrapResult{}
	err := rpcClient.GetTagMappingBootstrap(context.Background(), tlmetadata.GetTagMappingBootstrap{}, nil, res)
	require.NoError(t, err)
	return res.Mappings, nil
}

func putJournalEvent(t *testing.T, rpcClient *tlmetadata.Client, name, json string, id, version int64, typ int32, delete bool) (tlmetadata.Event, error) {
	resp := tlmetadata.Event{}
	req := tlmetadata.EditEntitynew{
		Event: tlmetadata.Event{
			Id:        id,
			Name:      name,
			Version:   version,
			Data:      json,
			EventType: typ,
		},
	}
	req.SetCreate(version == 0)
	req.SetDelete(delete)
	err := rpcClient.EditEntitynew(context.Background(), req, nil, &resp)
	return resp, err
}

func checkJson(t *testing.T, str string) {
	m := map[string]interface{}{}
	err := json.Unmarshal([]byte(str), &m)
	require.NoError(t, err)
}

func checkMetricResponse(t *testing.T, metric tlmetadata.Event, jsonExpected map[string]interface{}) {
	require.Greater(t, metric.Version, int64(0))
	actualJson := map[string]interface{}{}
	err := json.Unmarshal([]byte(metric.Data), &actualJson)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(jsonExpected, actualJson), fmt.Sprintf("JSON aren't equals. Expected: %s, actual: %s", jsonExpected, metric.Data))
}

func checkJournalResponse(t *testing.T, metric tlmetadata.Event, jsonExpected map[string]interface{}) {
	require.Greater(t, metric.Version, int64(0))
	actualJson := map[string]interface{}{}
	err := json.Unmarshal([]byte(metric.Data), &actualJson)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(jsonExpected, actualJson), fmt.Sprintf("JSON aren't equals. Expected: %s, actual: %s", jsonExpected, metric.Data))
}

func TestRPCServer(t *testing.T) {
	mx := sync.Mutex{}
	var now time.Time
	setNow := func(n time.Time) {
		mx.Lock()
		defer mx.Unlock()
		now = n
	}
	_, _, rpcClient, _ := initServer(t, func() time.Time {
		mx.Lock()
		defer mx.Unlock()
		var def time.Time
		if now == def {
			return time.Now()
		}
		now = now.Add(1 * time.Second)
		return now
	})
	t.Run("insert metric", func(t *testing.T) {
		resp, _ := putMetricTest(t, rpcClient, "abc", 0, 0, "")
		require.NotEqual(t, 0, resp.Version)
	})
	t.Run("insert metric with updated at", func(t *testing.T) {
		now := time.Date(2022, 1, 1, 12, 2, 0, 0, time.Now().Location())
		setNow(now)
		resp, _ := putMetricTest(t, rpcClient, "abc8", 0, 0, "")
		require.NotEqual(t, 0, resp.Version)
		require.Equal(t, now.Add(time.Second).Unix(), int64(resp.UpdateTime))
	})

	t.Run("insert metric with bad version", func(t *testing.T) {
		jsonStr, _ := statJson("abc2", 1235, "a")
		checkJson(t, jsonStr)
		_, err := putMetric(t, rpcClient, "abc2", jsonStr, 0, 4414)
		require.Error(t, err)
	})

	t.Run("insert metric with good version", func(t *testing.T) {
		resp, _ := putMetricTest(t, rpcClient, "abc3", 0, 0, "")
		version := resp.Version
		require.NotEqual(t, 0, resp.Version)
		resp, _ = putMetricTest(t, rpcClient, "abc3", resp.Id, resp.Version, "")
		require.NotEqual(t, 0, resp.Version)
		require.NotEqual(t, version, resp.Version)
	})

	t.Run("get all metrics", func(t *testing.T) {
		jsonStr, m := statJson("abc4", 1234, "a")
		checkJson(t, jsonStr)
		resp, err := putMetric(t, rpcClient, "abc4", jsonStr, 0, 0)
		checkMetricResponse(t, resp, m)
		require.NoError(t, err)
		jsonStr, m = statJson("abc5", 1234, "a")
		checkJson(t, jsonStr)
		resp1, err1 := putMetric(t, rpcClient, "abc5", jsonStr, 0, 0)
		checkMetricResponse(t, resp1, m)
		require.NoError(t, err1)
		metrics, err, b := getMetrics(t, rpcClient, resp.Version-1)
		require.NoError(t, err)
		require.False(t, b, "should return response")
		require.Equal(t, resp1.Version, metrics.CurrentVersion)
		require.Len(t, metrics.Events, 2)
	})
	t.Run("get all metrics with long poll", func(t *testing.T) {
		jsonStr, m := statJson("abc7", 1234, "a")
		checkJson(t, jsonStr)
		resp, err := putMetric(t, rpcClient, "abc7", jsonStr, 0, 0)
		checkMetricResponse(t, resp, m)
		require.NoError(t, err)
		metrics, err, b := getMetrics(t, rpcClient, math.MinInt64)
		require.NoError(t, err)
		require.False(t, b, "should return response")
		respC, errC := getMetricsAsync(t, rpcClient, metrics.CurrentVersion)
		jsonStr, m = statJson("abc6", 0, "a")
		checkJson(t, jsonStr)
		time.Sleep(time.Second)
		resp, err = putMetric(t, rpcClient, "abc6", jsonStr, 0, 0)
		checkMetricResponse(t, resp, m)
		time.Sleep(time.Second)
		require.NoError(t, err)
		select {
		case r := <-respC:
			require.Len(t, r.Events, 1)
			m := r.Events[0]
			require.Equal(t, resp.Version, m.Version)
		case err := <-errC:
			t.Error(err.Error())
		case <-time.After(5 * time.Second):
			t.Error("test timeout")
		}
	})

	t.Run("insert journal", func(t *testing.T) {
		resp, _ := putJournalTest(t, rpcClient, "abc", 0, 0, format.DashboardEvent, "", false)
		require.NotEqual(t, 0, resp.Version)
	})
	t.Run("insert journal with updated at", func(t *testing.T) {
		now := time.Date(2022, 1, 1, 12, 2, 0, 0, time.Now().Location())
		setNow(now)
		resp, _ := putJournalTest(t, rpcClient, "abc8", 0, 0, format.DashboardEvent, "", false)
		require.NotEqual(t, 0, resp.Version)
		require.Equal(t, now.Add(time.Second).Unix(), int64(resp.UpdateTime))
	})

	t.Run("insert journal with bad version", func(t *testing.T) {
		jsonStr, _ := statJson("abc2", 1235, "a")
		checkJson(t, jsonStr)
		_, err := putJournalEvent(t, rpcClient, "abc2", jsonStr, 0, 4414, format.DashboardEvent, false)
		require.Error(t, err)
	})

	t.Run("insert journal with good version", func(t *testing.T) {
		resp, _ := putJournalTest(t, rpcClient, "abc3", 0, 0, format.DashboardEvent, "", false)
		version := resp.Version
		require.NotEqual(t, 0, resp.Version)
		resp, _ = putJournalTest(t, rpcClient, "abc3", resp.Id, resp.Version, format.DashboardEvent, "", false)
		require.NotEqual(t, 0, resp.Version)
		require.NotEqual(t, version, resp.Version)
	})

	t.Run("delete journal", func(t *testing.T) {
		resp, _ := putJournalTest(t, rpcClient, "abc3eqweqw", 0, 0, format.DashboardEvent, "", false)
		version := resp.Version
		require.NotEqual(t, 0, resp.Version)
		resp, _ = putJournalTest(t, rpcClient, "abc3eqweqw", resp.Id, resp.Version, format.DashboardEvent, "", true)
		require.NotEqual(t, 0, resp.Version)
		require.NotEqual(t, version, resp.Version)
		require.NotEqual(t, uint32(0), resp.Unused)
	})

	t.Run("rename journal with good version", func(t *testing.T) {
		resp, _ := putJournalTest(t, rpcClient, "abc313", 0, 0, format.DashboardEvent, "", false)
		version := resp.Version
		require.NotEqual(t, 0, resp.Version)
		resp, _ = putJournalTest(t, rpcClient, "abc424", resp.Id, resp.Version, format.DashboardEvent, "", false)
		require.NotEqual(t, 0, resp.Version)
		require.NotEqual(t, version, resp.Version)
	})

	t.Run("get all journal", func(t *testing.T) {
		jsonStr, m := statJson("abc4", 1234, "a")
		checkJson(t, jsonStr)
		resp, err := putJournalEvent(t, rpcClient, "abc4", jsonStr, 0, 0, format.DashboardEvent, false)
		checkJournalResponse(t, resp, m)
		require.NoError(t, err)
		jsonStr, m = statJson("abc5", 1234, "a")
		checkJson(t, jsonStr)
		resp1, err1 := putJournalEvent(t, rpcClient, "abc5", jsonStr, 0, 0, format.DashboardEvent, false)
		checkJournalResponse(t, resp1, m)
		require.NoError(t, err1)
		metrics, err, b := getJournal(t, rpcClient, resp.Version-1)
		require.NoError(t, err)
		require.False(t, b, "should return response")
		require.Equal(t, resp1.Version, metrics.CurrentVersion)
		require.Len(t, metrics.Events, 2)
	})
	t.Run("get all journal with long poll", func(t *testing.T) {
		jsonStr, m := statJson("abc7", 1234, "a")
		checkJson(t, jsonStr)
		resp, err := putJournalEvent(t, rpcClient, "abc7", jsonStr, 0, 0, format.DashboardEvent, false)
		checkJournalResponse(t, resp, m)
		require.NoError(t, err)
		metrics, err, b := getJournal(t, rpcClient, math.MinInt64)
		require.NoError(t, err)
		require.False(t, b, "should return response")
		respC, errC := getJournalAsync(t, rpcClient, metrics.CurrentVersion)
		jsonStr, m = statJson("abc6", 0, "a")
		checkJson(t, jsonStr)
		time.Sleep(time.Second)
		resp, err = putJournalEvent(t, rpcClient, "abc6", jsonStr, 0, 0, format.DashboardEvent, false)
		checkJournalResponse(t, resp, m)
		time.Sleep(time.Second)
		require.NoError(t, err)
		select {
		case r := <-respC:
			require.Len(t, r.Events, 1)
			m := r.Events[0]
			require.Equal(t, resp.Version, m.Version)
		case err := <-errC:
			t.Error(err.Error())
		case <-time.After(5 * time.Second):
			t.Error("test timeout")
		}
	})

	t.Run("create mapping", func(t *testing.T) {
		mapping, err := unpackGetMappingUnion(getMapping(rpcClient, "abc", "k", true))
		require.NoError(t, err)
		require.Greater(t, mapping, int32(0))
	})
	t.Run("get mapping", func(t *testing.T) {
		mapping, err := unpackGetMappingUnion(getMapping(rpcClient, "abc1", "k1", true))
		require.NoError(t, err)
		require.Greater(t, mapping, int32(0))
		mapping1, err := unpackGetMappingUnion(getMapping(rpcClient, "abc1", "k1", true))
		require.NoError(t, err)
		require.Equal(t, mapping, mapping1)
	})

	t.Run("get mapping 1", func(t *testing.T) {
		mapping, err := unpackGetMappingUnion(getMapping(rpcClient, "abc4", "k9", true))
		require.NoError(t, err)
		require.Greater(t, mapping, int32(0))
		mapping1, err := unpackGetMappingUnion(getMapping(rpcClient, "abc4", "k9", false))
		require.NoError(t, err)
		require.Equal(t, mapping, mapping1)
	})

	t.Run("get invert mapping", func(t *testing.T) {
		mapping, err := unpackGetMappingUnion(getMapping(rpcClient, "abc6", "k11", true))
		require.NoError(t, err)
		require.Greater(t, mapping, int32(0))
		key, err := unpackInvertMappingUnion(getInvertMapping(rpcClient, mapping))
		require.NoError(t, err)
		require.Equal(t, "k11", key)
	})

	t.Run("get not exist invert mapping from pmс", func(t *testing.T) {
		resp, err := getInvertMapping(rpcClient, 9)
		require.NoError(t, err)
		require.True(t, resp.IsKeyNotExists())
	})
}

func TestRPCServerBroadcast(t *testing.T) {
	_, server, rpcClient, h := initServer(t, time.Now)
	defer server.Close()
	_, err := putMetricTest(t, rpcClient, "1", 0, 0, "")
	require.NoError(t, err)
	_, err = putMetricTest(t, rpcClient, "2", 0, 0, "")
	require.NoError(t, err)
	resp, err := putMetricTest(t, rpcClient, "3", 0, 0, "")
	require.NoError(t, err)
	metricChannel, errCh := getMetricsAsync(t, rpcClient, resp.Version)
	_, _ = getMetricsAsync(t, rpcClient, math.MaxInt64-1)

	time.Sleep(time.Second)
	h.getJournalMx.Lock()
	require.Len(t, h.getJournalClients, 2)
	require.Equal(t, resp.Version, h.minVersion)
	h.getJournalMx.Unlock()
	resp, _ = putMetricTest(t, rpcClient, "4", 0, 0, "")
	select {
	case m := <-metricChannel:
		require.Equal(t, resp.Version, m.CurrentVersion)
		require.Len(t, m.Events, 1)
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("expect to get new metrics")
	}
	h.getJournalMx.Lock()
	require.Len(t, h.getJournalClients, 1)
	require.Equal(t, int64(math.MaxInt64-1), h.minVersion)
	h.getJournalMx.Unlock()
}

func TestBootstrap(t *testing.T) {
	_, server, rpcClient, h := initServer(t, time.Now)
	defer server.Close()
	a := tlstatshouse.Mapping{
		Str:   "a",
		Value: 1,
	}
	b := tlstatshouse.Mapping{
		Str:   "b",
		Value: 2,
	}
	c := tlstatshouse.Mapping{
		Str:   "c",
		Value: 3,
	}
	t.Run("insert to empty db", func(t *testing.T) {
		c, err := putBootstrap(t, rpcClient, []tlstatshouse.Mapping{a, b})
		require.NoError(t, err)
		require.Equal(t, int32(0), c)
		m, err := getBootstrap(t, rpcClient)
		require.NoError(t, err)
		require.Len(t, m, 0)
	})

	t.Run("insert to non empty db", func(t *testing.T) {
		require.NoError(t, h.db.PutMapping(context.Background(), []string{a.Str, c.Str}, []int32{a.Value, c.Value}))
		count, err := putBootstrap(t, rpcClient, []tlstatshouse.Mapping{a, b, c})
		require.NoError(t, err)
		require.Equal(t, int32(2), count)

		m, err := getBootstrap(t, rpcClient)
		require.NoError(t, err)
		require.Len(t, m, 2)
		require.Contains(t, m, a)
		require.Contains(t, m, c)
	})
}

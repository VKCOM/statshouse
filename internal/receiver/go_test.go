// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package receiver_test

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/receiver"
)

var _ *goMachine // for staticcheck: type goMachine is unused (U1000)

const (
	goStatsHouseAddr     = "127.0.0.1:"
	goStatsHouseAddrUnix = "@statshouse-test"
	envName              = "abc"
)

type tag struct {
	K string
	V string
}

func identOrEmpty() *rapid.Generator[string] {
	return rapid.OneOf(rapid.Just(""), ident())
}

func keys() *rapid.Generator[statshouse.Tags] {
	return rapid.Custom(func(t *rapid.T) statshouse.Tags {
		var tags statshouse.Tags
		for i := range tags {
			tags[i] = identOrEmpty().Draw(t, fmt.Sprintf("key%d", i))
		}
		return tags
	})
}

func tagArr() *rapid.Generator[[2]string] {
	k := ident()
	v := ident()

	return rapid.Custom(func(t *rapid.T) [2]string {
		return [2]string{
			k.Draw(t, "k"),
			v.Draw(t, "v"),
		}
	})
}

func tagsArrSlice() *rapid.Generator[[][2]string] {
	return rapid.SliceOfN(tagArr(), 0, 16)
}

func tagsSlice() *rapid.Generator[[]tag] {
	return rapid.Map(tagsArrSlice(), func(sl [][2]string) []tag {
		result := make([]tag, 0)
		already := map[string]bool{}
		for _, arr := range sl {
			if already[arr[0]] {
				continue
			}
			already[arr[0]] = true
			result = append(result, tag{
				K: arr[0],
				V: arr[1],
			})
		}
		return result
	})
}

func toTagsStruct(tags [][2]string, skey string, withEnv bool) []tag {
	res := make([]tag, 0)
	envExists := false
	for _, t := range tags {
		name := t[0]
		value := t[1]
		if name == "" || value == "" {
			continue
		}
		if name == "0" || name == "env" || name == "key0" {
			envExists = true
		}
		res = append(res, tag{K: name, V: value})
	}
	if skey != "" {
		res = append(res, tag{K: "_s", V: skey})
	}
	if withEnv && !envExists {
		res = append(res, tag{K: "0", V: envName})
	}
	return res
}

func toTags(ks statshouse.Tags, skey string, withEnv bool) []tag {
	m := map[string]string{
		"_s": skey,
	}
	for i, t := range ks {
		m[strconv.Itoa(i)] = t
	}
	if withEnv && ks[0] == "" {
		m["0"] = envName
	}
	for k, v := range m {
		if v == "" {
			delete(m, k)
		}
	}
	result := make([]tag, 0, len(m))
	for k, v := range m {
		result = append(result, tag{
			K: k,
			V: v,
		})
	}
	return result
}

type goMachine struct {
	counterMetrics floatsMap
	valueMetrics   floatsMap
	uniqueMetrics  intsMap
	recv           *receiver.UDP
	addr           string
	send           *statshouse.Client
	envIsSet       bool
}

func (g *goMachine) init(t *rapid.T) {
	g.counterMetrics = floatsMap{}
	g.valueMetrics = floatsMap{}
	g.uniqueMetrics = intsMap{}
	recv, err := receiver.ListenUDP("udp", goStatsHouseAddr, receiver.DefaultConnBufSize, false, nil, nil, nil)
	//recv, err := receiver.ListenUDP("unixgram", goStatsHouseAddrUnix, receiver.DefaultConnBufSize, true, nil, nil)
	require.NoError(t, err)
	g.recv = recv
	g.addr = recv.Addr()
	env := ""
	if g.envIsSet {
		env = envName
	}
	g.send = statshouse.NewClient(t.Logf, "udp", g.addr, env)
}

func (g *goMachine) Cleanup() {
	_ = g.send.Close()
	_ = g.recv.Close()
}

func (g *goMachine) Counter(t *rapid.T) {
	var (
		name  = ident().Draw(t, "name")
		ks    = keys().Draw(t, "keys")
		value = rapid.Float64Range(0.1, math.MaxInt32).Draw(t, "value")
	)

	k := ts(name, toTags(ks, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}

	g.send.Count(name, ks, value)
	g.counterMetrics.count(k, value)
}

func (g *goMachine) CounterNamed(t *rapid.T) {
	var (
		name  = ident().Draw(t, "name")
		value = rapid.Float64Range(0.1, math.MaxInt32).Draw(t, "value")
		tags  = tagsArrSlice().Draw(t, "tags")
	)

	k := ts(name, toTagsStruct(tags, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}
	g.send.NamedCount(name, tags, value)
	g.counterMetrics.count(k, value)
}

func (g *goMachine) Values(t *rapid.T) {
	var (
		name   = ident().Draw(t, "name")
		ks     = keys().Draw(t, "keys")
		values = rapid.SliceOfN(rapid.Float64(), 1, -1).Draw(t, "values")
	)

	k := ts(name, toTags(ks, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}

	g.send.Values(name, ks, values)
	g.valueMetrics.merge(k, values)
}

func (g *goMachine) ValuesNamed(t *rapid.T) {
	var (
		name   = ident().Draw(t, "name")
		values = rapid.SliceOfN(rapid.Float64(), 1, -1).Draw(t, "values")
		tags   = tagsArrSlice().Draw(t, "tags")
	)

	k := ts(name, toTagsStruct(tags, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}

	g.send.NamedValues(name, tags, values)
	g.valueMetrics.merge(k, values)
}

func (g *goMachine) Uniques(t *rapid.T) {
	var (
		name   = ident().Draw(t, "name")
		ks     = keys().Draw(t, "keys")
		values = rapid.SliceOfN(rapid.Int64(), 1, -1).Draw(t, "values")
	)

	k := ts(name, toTags(ks, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}

	g.send.Uniques(name, ks, values)
	g.uniqueMetrics.merge(k, values)
}

func (g *goMachine) UniquesNamed(t *rapid.T) {
	var (
		name   = ident().Draw(t, "name")
		values = rapid.SliceOfN(rapid.Int64(), 1, -1).Draw(t, "values")
		tags   = tagsArrSlice().Draw(t, "tags")
	)

	k := ts(name, toTagsStruct(tags, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}

	g.send.NamedUniques(name, tags, values)
	g.uniqueMetrics.merge(k, values)
}

func (g *goMachine) STops(t *rapid.T) {
	var (
		name   = ident().Draw(t, "name")
		ks     = keys().Draw(t, "keys")
		values = rapid.SliceOfN(identLike(format.MaxStringLen), 1, -1).Draw(t, "values") // not arbitrary strings for easier text encoding
	)
	k := ts(name, toTags(ks, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}
	g.send.StringsTop(name, ks, values)
	for _, skey := range values {
		g.counterMetrics.count(ts(name, toTags(ks, skey, g.envIsSet)), 1)
	}
}

func (g *goMachine) STopsNamed(t *rapid.T) {
	var (
		name   = ident().Draw(t, "name")
		values = rapid.SliceOfN(identLike(format.MaxStringLen), 1, -1).Draw(t, "values") // not arbitrary strings for easier text encoding
		tags   = tagsArrSlice().Draw(t, "tags")
	)

	k := ts(name, toTagsStruct(tags, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}

	g.send.NamedStringsTop(name, tags, values)
	for _, skey := range values {
		g.counterMetrics.count(ts(name, toTagsStruct(tags, skey, g.envIsSet)), 1)
	}
}

func (g *goMachine) Run(t *rapid.T) {
	totalC := g.counterMetrics.sum()
	totalV := g.valueMetrics.size() + g.uniqueMetrics.size()
	if totalC == 0 && totalV == 0 {
		t.Skip("no metrics to send/receive")
	}

	// send everything
	err := g.send.Close()
	require.NoError(t, err)

	var (
		recvErr        error
		counterMetrics = floatsMap{}
		valueMetrics   = floatsMap{}
		uniqueMetrics  = intsMap{}
		stopMetrics    = stringsMap{}
	)

	// we rely on the fact that UDP buffer is big enough to hold everything sent before this point
	timer := time.AfterFunc(recvTimeout, func() { _ = g.recv.Close() })
	defer timer.Stop()
	serveErr := g.recv.Serve(receiver.CallbackHandler{
		Metrics: func(m *tlstatshouse.MetricBytes, cb data_model.MapCallbackFunc) (h data_model.MappedMetricHeader, done bool) {
			switch {
			case len(m.Value) > 0:
				valueMetrics.merge(ts(string(m.Name), receivedSlice(m.Tags, nil)), m.Value)
			case len(m.Unique) > 0:
				uniqueMetrics.merge(ts(string(m.Name), receivedSlice(m.Tags, nil)), m.Unique)
			case len(m.Value) == 0 && len(m.Unique) == 0 && m.Counter > 0:
				counterMetrics.count(ts(string(m.Name), receivedSlice(m.Tags, nil)), m.Counter)
				// stopMetrics.merge(, toStrings(m.Stop))
			}
			if recvErr != nil || (counterMetrics.sum() >= totalC && valueMetrics.size()+uniqueMetrics.size()+stopMetrics.size() >= totalV) {
				_ = g.recv.Close()
			}
			return h, true
		},
		ParseError: func(pkt []byte, err error) {
			if recvErr == nil {
				recvErr = err
			}
			_ = g.recv.Close()
		},
	})

	require.NoError(t, serveErr)
	require.NoError(t, recvErr)
	require.Equal(t, g.counterMetrics, counterMetrics)
	require.Equal(t, g.valueMetrics, valueMetrics)
	require.Equal(t, g.uniqueMetrics, uniqueMetrics)

	g.init(t)
}

func (g *goMachine) Check(*rapid.T) {}

func TestGoRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := goMachine{}
		m.init(t)
		defer m.Cleanup()
		t.Repeat(rapid.StateMachineActions(&m))
	})
}

/* It seems, test above also sets key0 sometimes. So for speed we commented this test out
func TestGoRoundtripWithEnv(t *testing.T) {
	rapid.Check(t, rapid.Run(&goMachine{envIsSet: true}))
}
*/

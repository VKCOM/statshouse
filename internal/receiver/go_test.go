// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package receiver_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/mapping"
	"github.com/vkcom/statshouse/internal/receiver"
	"github.com/vkcom/statshouse/internal/vkgo/statlogs"
)

const (
	goStatsHouseAddr = "127.0.0.1:"
	envName          = "abc"
)

type tag struct {
	K string
	V string
}

func identOrEmpty() *rapid.Generator {
	return rapid.OneOf(rapid.Just(""), ident())
}

func keys() *rapid.Generator {
	return rapid.Custom(func(t *rapid.T) statlogs.RawTags {
		return statlogs.RawTags{
			Env:   identOrEmpty().Draw(t, "key0").(string),
			Tag1:  identOrEmpty().Draw(t, "key1").(string),
			Tag2:  identOrEmpty().Draw(t, "key2").(string),
			Tag3:  identOrEmpty().Draw(t, "key3").(string),
			Tag4:  identOrEmpty().Draw(t, "key4").(string),
			Tag5:  identOrEmpty().Draw(t, "key5").(string),
			Tag6:  identOrEmpty().Draw(t, "key6").(string),
			Tag7:  identOrEmpty().Draw(t, "key7").(string),
			Tag8:  identOrEmpty().Draw(t, "key8").(string),
			Tag9:  identOrEmpty().Draw(t, "key9").(string),
			Tag10: identOrEmpty().Draw(t, "key10").(string),
			Tag11: identOrEmpty().Draw(t, "key11").(string),
			Tag12: identOrEmpty().Draw(t, "key12").(string),
			Tag13: identOrEmpty().Draw(t, "key13").(string),
			Tag14: identOrEmpty().Draw(t, "key14").(string),
			Tag15: identOrEmpty().Draw(t, "key15").(string),
		}
	})
}

func tagsArrSlice() *rapid.Generator {
	return rapid.SliceOfN(rapid.ArrayOf(2, ident()), 0, 16)
}
func tagsSlice() *rapid.Generator {
	return rapid.SliceOfN(rapid.ArrayOf(2, ident()), 0, 16).Map(func(sl [][2]string) []tag {
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
		res = append(res, tag{K: "skey", V: skey})
	}
	if withEnv && !envExists {
		res = append(res, tag{K: "key0", V: envName})
	}
	return res
}

func toTags(ks statlogs.RawTags, skey string, withEnv bool) []tag {
	m := map[string]string{
		"key0":  ks.Env,
		"key1":  ks.Tag1,
		"key2":  ks.Tag2,
		"key3":  ks.Tag3,
		"key4":  ks.Tag4,
		"key5":  ks.Tag5,
		"key6":  ks.Tag6,
		"key7":  ks.Tag7,
		"key8":  ks.Tag8,
		"key9":  ks.Tag9,
		"key10": ks.Tag10,
		"key11": ks.Tag11,
		"key12": ks.Tag12,
		"key13": ks.Tag13,
		"key14": ks.Tag14,
		"key15": ks.Tag15,
		"skey":  skey,
	}
	if withEnv && ks.Env == "" {
		m["key0"] = envName
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
	send           *statlogs.Registry
	envIsSet       bool
}

func (g *goMachine) Init(t *rapid.T) {
	g.counterMetrics = floatsMap{}
	g.valueMetrics = floatsMap{}
	g.uniqueMetrics = intsMap{}
	recv, err := receiver.ListenUDP(goStatsHouseAddr, receiver.DefaultConnBufSize, false, nil, nil)
	require.NoError(t, err)
	g.recv = recv
	g.addr = recv.Addr()
	env := ""
	if g.envIsSet {
		env = envName
	}
	g.send = statlogs.NewRegistry(t.Logf, g.addr, env)
}

func (g *goMachine) Cleanup() {
	_ = g.send.Close()
	_ = g.recv.Close()
}

func (g *goMachine) Counter(t *rapid.T) {
	var (
		name  = ident().Draw(t, "name").(string)
		ks    = keys().Draw(t, "keys").(statlogs.RawTags)
		value = rapid.Float64Range(0.1, math.MaxInt32).Draw(t, "value").(float64)
	)

	k := ts(name, toTags(ks, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}

	g.send.AccessMetricRaw(name, ks).Count(value)
	g.counterMetrics.count(k, value)
}

func (g *goMachine) CounterNamed(t *rapid.T) {
	var (
		name  = ident().Draw(t, "name").(string)
		value = rapid.Float64Range(0.1, math.MaxInt32).Draw(t, "value").(float64)
		tags  = tagsArrSlice().Draw(t, "tags").([][2]string)
	)

	k := ts(name, toTagsStruct(tags, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}
	g.send.AccessMetric(name, tags).Count(value)
	g.counterMetrics.count(k, value)
}

func (g *goMachine) Values(t *rapid.T) {
	var (
		name   = ident().Draw(t, "name").(string)
		ks     = keys().Draw(t, "keys").(statlogs.RawTags)
		values = rapid.SliceOfN(rapid.Float64(), 1, -1).Draw(t, "values").([]float64)
	)

	k := ts(name, toTags(ks, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}

	g.send.AccessMetricRaw(name, ks).Values(values)
	g.valueMetrics.merge(k, values)
}

func (g *goMachine) ValuesNamed(t *rapid.T) {
	var (
		name   = ident().Draw(t, "name").(string)
		values = rapid.SliceOfN(rapid.Float64(), 1, -1).Draw(t, "values").([]float64)
		tags   = tagsArrSlice().Draw(t, "tags").([][2]string)
	)

	k := ts(name, toTagsStruct(tags, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}

	g.send.AccessMetric(name, tags).Values(values)
	g.valueMetrics.merge(k, values)
}

func (g *goMachine) Uniques(t *rapid.T) {
	var (
		name   = ident().Draw(t, "name").(string)
		ks     = keys().Draw(t, "keys").(statlogs.RawTags)
		values = rapid.SliceOfN(rapid.Int64(), 1, -1).Draw(t, "values").([]int64)
	)

	k := ts(name, toTags(ks, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}

	g.send.AccessMetricRaw(name, ks).Uniques(values)
	g.uniqueMetrics.merge(k, values)
}

func (g *goMachine) UniquesNamed(t *rapid.T) {
	var (
		name   = ident().Draw(t, "name").(string)
		values = rapid.SliceOfN(rapid.Int64(), 1, -1).Draw(t, "values").([]int64)
		tags   = tagsArrSlice().Draw(t, "tags").([][2]string)
	)

	k := ts(name, toTagsStruct(tags, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}

	g.send.AccessMetric(name, tags).Uniques(values)
	g.uniqueMetrics.merge(k, values)
}

func (g *goMachine) STops(t *rapid.T) {
	var (
		name   = ident().Draw(t, "name").(string)
		ks     = keys().Draw(t, "keys").(statlogs.RawTags)
		values = rapid.SliceOfN(identLike(format.MaxStringLen), 1, -1).Draw(t, "values").([]string) // not arbitrary strings for easier text encoding
	)
	k := ts(name, toTags(ks, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}
	g.send.AccessMetricRaw(name, ks).StringsTop(values)
	for _, skey := range values {
		g.counterMetrics.count(ts(name, toTags(ks, skey, g.envIsSet)), 1)
	}
}

func (g *goMachine) STopsNamed(t *rapid.T) {
	var (
		name   = ident().Draw(t, "name").(string)
		values = rapid.SliceOfN(identLike(format.MaxStringLen), 1, -1).Draw(t, "values").([]string) // not arbitrary strings for easier text encoding
		tags   = tagsArrSlice().Draw(t, "tags").([][2]string)
	)

	k := ts(name, toTagsStruct(tags, "", g.envIsSet))
	if len(k) > maxTSSize {
		return
	}

	g.send.AccessMetric(name, tags).StringsTop(values)
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
		Metrics: func(m *tlstatshouse.MetricBytes, cb mapping.MapCallbackFunc) (h data_model.MappedMetricHeader, done bool) {
			switch {
			case len(m.Value) > 0:
				valueMetrics.merge(ts(string(m.Name), receivedSlice(m.Tags, nil)), m.Value)
			case len(m.Unique) > 0:
				uniqueMetrics.merge(ts(string(m.Name), receivedSlice(m.Tags, nil)), m.Unique)
			case len(m.Stop) > 0:
				for _, skey := range m.Stop {
					counterMetrics.count(ts(string(m.Name), receivedSlice(m.Tags, skey)), 1)
				}
			case len(m.Value) == 0 && len(m.Unique) == 0 && len(m.Stop) == 0 && m.Counter > 0:
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

	g.Init(t)
}

func (g *goMachine) Check(*rapid.T) {}

func TestGoRoundtrip(t *testing.T) {
	rapid.Check(t, rapid.Run(&goMachine{}))
}

/* It seems, test above also sets key0 sometimes. So for speed we commented this test out
func TestGoRoundtripWithEnv(t *testing.T) {
	rapid.Check(t, rapid.Run(&goMachine{envIsSet: true}))
}
*/

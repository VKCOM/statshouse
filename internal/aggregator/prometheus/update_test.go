// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"reflect"
	"testing"
	"time"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"

	"github.com/stretchr/testify/require"
)

func target(job, addrValue string) tlstatshouse.PromTargetBytes {
	return tlstatshouse.PromTargetBytes{
		JobName: []byte(job),
		Url:     []byte(addrValue),
	}
}

func Test_sortedTargets(t *testing.T) {
	tests := []struct {
		name string
		arg  []tlstatshouse.PromTargetBytes
		want []tlstatshouse.PromTargetBytes
	}{
		{name: "empty", arg: []tlstatshouse.PromTargetBytes{}, want: []tlstatshouse.PromTargetBytes{}},
		{name: "single", arg: []tlstatshouse.PromTargetBytes{target("a", "localhost:2020")}, want: []tlstatshouse.PromTargetBytes{target("a", "localhost:2020")}},
		{name: "sort by url", arg: []tlstatshouse.PromTargetBytes{target("a", "localhost:12"), target("a", "localhost:11"), target("a", "localhost:10")},
			want: []tlstatshouse.PromTargetBytes{target("a", "localhost:10"), target("a", "localhost:11"), target("a", "localhost:12")}},
		{name: "sort by job", arg: []tlstatshouse.PromTargetBytes{target("c", "localhost:10"), target("b", "localhost:11"), target("a", "localhost:12")},
			want: []tlstatshouse.PromTargetBytes{target("a", "localhost:12"), target("b", "localhost:11"), target("c", "localhost:10")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sortedTargets(tt.arg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sortedTargets() = %v, want %v", got, tt.want)
			}
		})
	}
}

func buildTarget(job string, targets []string, labels [][2]string, scrapeInterval, scrapeTimeout time.Duration, honorTimestamps, honorLabels bool) tlstatshouse.PromTargetBytes {
	target := tlstatshouse.PromTargetBytes{
		JobName:        []byte("abc"),
		Url:            []byte("http://localhost:8080"),
		Labels:         []tl.DictionaryFieldStringBytes{{Key: []byte("k"), Value: []byte("v")}},
		ScrapeInterval: scrapeInterval.Nanoseconds(),
		ScrapeTimeout:  scrapeTimeout.Nanoseconds(),
	}
	target.SetHonorTimestamps(honorTimestamps)
	target.SetHonorLabels(honorLabels)
	return target
}

func Test_hash(t *testing.T) {
	var buf []byte
	groups := []tlstatshouse.PromTargetBytes{buildTarget("abc", []string{"http://localhost:8080"}, [][2]string{{"k", "v"}}, time.Minute, time.Second, false, false)}
	hashStr, hashBytes := hash(groups, &buf)
	require.Equal(t, hashStr, string(hashBytes))
	hashStr1, _ := hash(groups, &buf)
	require.Equal(t, hashStr, hashStr1, "hash must be same for same values")
}

/*

const testJob = "test_job"
const testHost = "test_host"
const addressLabel = "__address__"

func targetLabel(addrValue string) model.LabelSet {
	return model.LabelSet{addressLabel: model.LabelValue(addrValue)}
}

func configStr(addrs []string, scrapeInterval time.Duration) string {
	for i, addr := range addrs {
		addrs[i] = "'" + addr + "'"
	}
	addrsStr := strings.Join(addrs, ",")
	return fmt.Sprintf(`global:
  scrape_interval:     %s
scrape_configs:
- job_name: 'test_job'
  static_configs:
    - targets: [%s]
`, scrapeInterval.String(), addrsStr)
}

func initUpdater(t *testing.T) (*Updater, hostInfo) {
	addrs := []string{testHost + ":2023", testHost + ":2024"}
	cfgStr := configStr(addrs, time.Second)
	cfg, err := config.Load(cfgStr, false, promlog.NewNopLogger())
	require.NoError(t, err)
	scfg := cfg.ScrapeConfigs[0]

	info := hostInfo{
		hash: "",
		result: tlstatshouse.GetTargetsResultBytes{
			Targets: []tlstatshouse.PromTargetBytes{buildTarget(testJob, []string{"http://" + testHost + ":2023/metrics", "http://" + testHost + ":2024/metrics"}, nil, time.Duration(scfg.ScrapeInterval), time.Duration(scfg.ScrapeTimeout), scfg.HonorTimestamps, scfg.HonorLabels)},
			Hash:    nil,
		},
	}

	u := &Updater{
		hosts:             map[string]hostInfo{testHost: info},
		mx:                sync.RWMutex{},
		cfg:               cfg,
		groupsClientsMu:   sync.Mutex{},
		promGroupsClients: nil,
		ctx:               nil,
		cancel:            nil,
		manager:           newDiscoveryManager(context.Background(), cfg),
		logTrace: func(format string, a ...interface{}) {
		},
		groups: map[string][]*targetgroup.Group{
			testJob: {&targetgroup.Group{
				Targets: []model.LabelSet{targetLabel(testHost + ":2023"), targetLabel(testHost + ":2024")},
				Labels:  nil,
				Source:  "",
			}},
		},
	}
	return u, info
}

func Test_hostInfoMapFromConfig(t *testing.T) {
	u, _ := initUpdater(t)
	cfg := u.cfg
	cfg.ScrapeConfigs[0].ScrapeInterval = model.Duration(time.Second * 2)
	u.ApplyConfig(cfg)
	require.Equal(t, cfg, u.cfg)
	require.Contains(t, u.hosts, testHost)
	host := u.hosts[testHost]
	require.Len(t, host.result.Targets, 2)
	group := host.result.Targets[0]
	require.Len(t, group.Labels, 0)
	require.Equal(t, group.ScrapeInterval, int64(time.Second*2))
	require.Equal(t, group.ScrapeTimeout, int64(time.Second))
}
*/

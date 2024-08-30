// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/netip"
	"net/url"
	"slices"
	"sort"
	"sync"

	"github.com/prometheus/common/model"
	_ "github.com/prometheus/prometheus/discovery/consul"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"go.uber.org/atomic"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type scrapeServer struct {
	discovery scrapeDiscovery
	configS   string       // configuration string
	configMu  sync.RWMutex // serialize access to "configS"
	configH   atomic.Int32 // configuration string SHA1 hash

	// set on "run"
	sh2     *agent.Agent // used to report statistics
	running bool         // guard against double "run"

	// current targets, updated on discovery events
	targetsByName map[string]tlstatshouse.GetTargetsResultBytes
	targetsByAddr map[netip.Addr]tlstatshouse.GetTargetsResultBytes
	targetsMu     sync.RWMutex

	// long poll requests
	requests   map[*rpc.HandlerContext]scrapeRequest
	requestsMu sync.Mutex
}

type scrapeRequest struct {
	hctx *rpc.HandlerContext
	name []byte
	addr netip.Addr
	args tlstatshouse.GetTargets2Bytes
}

func newScrapeServer() *scrapeServer {
	res := &scrapeServer{
		targetsByName: make(map[string]tlstatshouse.GetTargetsResultBytes),
		targetsByAddr: make(map[netip.Addr]tlstatshouse.GetTargetsResultBytes),
		requests:      make(map[*rpc.HandlerContext]scrapeRequest),
	}
	res.discovery = newScrapeDiscovery(res.applyScrapeConfig)
	return res
}

func (s *scrapeServer) run(meta *metajournal.MetricsStorage, journal *metajournal.MetricMetaLoader, sh2 *agent.Agent) {
	if s.running {
		return
	}
	s.discovery.run(meta, journal, sh2)
	s.sh2 = sh2
	s.running = true
}

func (s *scrapeServer) reportStats(m map[string]string) {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	m["scrape_config"] = s.configS
}

func (s *scrapeServer) applyConfig(configID int32, configS string) {
	if configID != format.PrometheusConfigID {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.Println("scrape server panic!", r)
		}
	}()
	s.discovery.applyConfig(configS)
}

func (s *scrapeServer) applyScrapeConfig(cs []ScrapeConfig) {
	type nameAddr struct {
		name string
		addr netip.Addr
	}
	targets := make(map[nameAddr]*tlstatshouse.GetTargetsResultBytes)
	for _, c := range cs {
		var gaugeMetrics [][]byte
		if len(c.Options.GaugeMetrics) != 0 {
			gaugeMetrics = make([][]byte, len(c.Options.GaugeMetrics))
			for i, v := range c.Options.GaugeMetrics {
				gaugeMetrics[i] = []byte(v)
			}
		}
		ns := tl.DictionaryFieldStringBytes{
			Key:   []byte(format.ScrapeNamespaceTagName),
			Value: []byte(c.Options.Namespace),
		}
		for _, j := range c.ScrapeConfigs {
			var rcs []*relabel.Config
			if len(j.RelabelConfigs) != 0 {
				rcs = make([]*relabel.Config, 0, len(j.RelabelConfigs))
				for _, src := range j.RelabelConfigs {
					if rc, err := src.toPrometheusFormat(); err == nil {
						rcs = append(rcs, &rc)
					}
				}
			}
			for _, g := range j.StaticConfigs {
				if len(rcs) != 0 && len(g.Labels) != 0 {
					ls := make(labels.Labels, 0, len(g.Labels))
					for k, v := range g.Labels {
						ls = append(ls, labels.Label{
							Name:  string(k),
							Value: string(v),
						})
					}
					ls = relabel.Process(ls, rcs...)
					if ls == nil {
						continue
					}
					g.Labels = make(model.LabelSet, len(ls))
					for _, v := range ls {
						g.Labels[model.LabelName(v.Name)] = model.LabelValue(v.Value)
					}
				}
				jj := j
				ls := make([]tl.DictionaryFieldStringBytes, 0, len(g.Labels)+1)
				ls = append(ls, ns)
				for k, v := range g.Labels {
					switch k {
					case model.SchemeLabel:
						jj.Scheme = string(v)
					case model.MetricsPathLabel:
						jj.MetricsPath = string(v)
					case model.ScrapeIntervalLabel:
						if d, err := model.ParseDuration(string(v)); err == nil {
							jj.ScrapeInterval = d
						}
					case model.ScrapeTimeoutLabel:
						if d, err := model.ParseDuration(string(v)); err == nil {
							jj.ScrapeTimeout = d
						}
					default:
						ls = append(ls, tl.DictionaryFieldStringBytes{
							Key:   []byte(k),
							Value: []byte(v),
						})
					}
				}
				ts := g.Targets
				if len(ts) == 0 {
					if t := g.Labels[model.AddressLabel]; t != "" {
						ts = []string{string(t)}
					}
				}
				for _, t := range ts {
					var k nameAddr
					if ipp, err := netip.ParseAddrPort(t); err == nil {
						k.addr = ipp.Addr()
					} else if host, _, err := net.SplitHostPort(t); err == nil {
						k.name = host
					} else {
						log.Printf("scrape target not recognized: %v\n", err)
						continue
					}
					v := targets[k]
					if v == nil {
						v = &tlstatshouse.GetTargetsResultBytes{GaugeMetrics: gaugeMetrics}
						targets[k] = v
					}
					v.Targets = append(v.Targets, jj.toPromTargetBytes(t, ls))
				}
			}
		}
	}
	var buf []byte
	for _, v := range targets {
		sort.Slice(v.Targets, func(i, j int) bool {
			return bytes.Compare(v.Targets[i].Url, v.Targets[j].Url) < 0
		})
		v.Targets = slices.CompactFunc(v.Targets, func(a, b tlstatshouse.PromTargetBytes) bool {
			return bytes.Compare(a.Url, b.Url) < 0
		})
		v.Hash = nil
		buf = v.WriteBoxed(buf[:0], 0xffffffff)
		sum := sha256.Sum256(buf)
		v.Hash = sum[:]
	}
	// publish targets
	func() {
		s.targetsMu.Lock()
		defer s.targetsMu.Unlock()
		for k := range s.targetsByName {
			s.targetsByName[k] = tlstatshouse.GetTargetsResultBytes{}
		}
		for k := range s.targetsByAddr {
			s.targetsByAddr[k] = tlstatshouse.GetTargetsResultBytes{}
		}
		for k, v := range targets {
			var host string
			if k.name != "" {
				s.targetsByName[k.name] = *v
				host = k.name
			} else {
				s.targetsByAddr[k.addr] = *v
				host = k.addr.String()
			}
			if s.sh2 != nil {
				// success, targets_ready
				s.sh2.AddCounterHostStringBytes(
					s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeTargetDispatch, [format.MaxTags]int32{0, 0, 1}),
					[]byte(host), 1, 0, format.BuiltinMetricMetaAggScrapeTargetDispatch)
			}
		}
	}()
	// serve long poll requests
	pendingRequests := func() []scrapeRequest {
		s.requestsMu.Lock()
		defer s.requestsMu.Unlock()
		res := make([]scrapeRequest, 0, len(s.requests))
		for _, v := range s.requests {
			res = append(res, v)
		}
		return res
	}()
	for _, v := range pendingRequests {
		done, err := s.tryGetNewTargetsAndWriteResult(v)
		if done {
			v.hctx.SendHijackedResponse(err)
			s.CancelHijack(v.hctx)
		}
	}
	// config applied successfully
	if b, err := json.Marshal(cs); err == nil {
		configS := string(b)
		s.configMu.Lock()
		s.configS = configS
		s.configMu.Unlock()
		h := sha1.Sum(b)
		s.configH.Store(int32(binary.BigEndian.Uint32(h[:])))
	}
}

func (s *scrapeServer) reportConfigHash(nowUnix uint32) {
	v := s.configH.Load()
	s.sh2.AddCounterHostStringBytes(
		s.sh2.AggKey(nowUnix, format.BuiltinMetricIDAggScrapeConfigHash, [format.MaxTags]int32{0, v}),
		nil, 1, 0, format.BuiltinMetricMetaAggScrapeConfigHash)
}

func (s *scrapeServer) handleGetTargets(_ context.Context, hctx *rpc.HandlerContext) error {
	var args tlstatshouse.GetTargets2Bytes
	_, err := args.Read(hctx.Request)
	if err != nil {
		return fmt.Errorf("failed to deserialize statshouse.getTargets2 request: %w", err)
	}
	ipp, err := netip.ParseAddrPort(hctx.RemoteAddr().String())
	if err != nil {
		return &rpc.Error{
			Code:        data_model.RPCErrorScrapeAgentIP,
			Description: "scrape agent must have an IP address",
		}
	}
	// fast path, try respond without taking "requestsMu"
	req := scrapeRequest{
		hctx: hctx,
		name: args.PromHostName,
		addr: ipp.Addr(),
		args: args,
	}
	done, err := s.tryGetNewTargetsAndWriteResult(req)
	if done || err != nil {
		return err
	}
	// slow path, take "requestsMu" and try again
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	done, err = s.tryGetNewTargetsAndWriteResult(req)
	if done || err != nil {
		return err
	}
	// long poll, scrape targets will be sent once ready
	err = hctx.HijackResponse(s)
	if rpc.IsHijackedResponse(err) {
		s.requests[hctx] = req
	}
	return err
}

func (s *scrapeServer) tryGetNewTargetsAndWriteResult(req scrapeRequest) (done bool, err error) {
	var ok bool
	var res tlstatshouse.GetTargetsResultBytes
	s.targetsMu.RLock()
	if len(req.name) != 0 && len(s.targetsByName) != 0 {
		res, ok = s.targetsByName[string(req.name)]
	}
	if !ok && len(s.targetsByAddr) != 0 {
		res, ok = s.targetsByAddr[req.addr]
	}
	s.targetsMu.RUnlock()
	if !ok || bytes.Equal(req.args.OldHash, res.Hash) {
		return false, nil
	}
	req.hctx.Response, err = req.args.WriteResult(req.hctx.Response, res)
	if s.sh2 != nil {
		if err != nil {
			// failure, targets_sent
			s.sh2.AddCounterHostStringBytes(
				s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeTargetDispatch, [format.MaxTags]int32{0, 1, 2}),
				[]byte(req.addr.String()), 1, 0, format.BuiltinMetricMetaAggScrapeTargetDispatch)
		} else {
			// success, targets_sent
			s.sh2.AddCounterHostStringBytes(
				s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeTargetDispatch, [format.MaxTags]int32{0, 0, 2}),
				[]byte(req.addr.String()), 1, 0, format.BuiltinMetricMetaAggScrapeTargetDispatch)
		}
	}
	return true, err
}

func (s *scrapeServer) CancelHijack(hctx *rpc.HandlerContext) {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	delete(s.requests, hctx)
}

func (job *scrapeJobConfig) toPromTargetBytes(addr string, labels []tl.DictionaryFieldStringBytes) tlstatshouse.PromTargetBytes {
	var err error
	u := url.URL{Scheme: job.Scheme, Host: addr}
	if u.Path, err = url.QueryUnescape(job.MetricsPath); err != nil {
		u.Path = job.MetricsPath
	}
	mrc, _ := json.Marshal(job.MetricRelabelConfigs)
	return tlstatshouse.PromTargetBytes{
		JobName:              []byte(job.JobName),
		Url:                  []byte(u.String()),
		Labels:               labels,
		ScrapeInterval:       int64(job.ScrapeInterval),
		ScrapeTimeout:        int64(job.ScrapeTimeout),
		MetricRelabelConfigs: mrc,
	}
}

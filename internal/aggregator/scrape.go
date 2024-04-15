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
	"log"
	"net/netip"
	"net/url"
	"sort"
	"sync"

	"github.com/prometheus/prometheus/config"
	_ "github.com/prometheus/prometheus/discovery/consul"
	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"go.uber.org/atomic"
	"gopkg.in/yaml.v2"
)

type scrapeServer struct {
	config  *scrapeConfigService
	configH atomic.Int32 // configuration string SHA1 hash

	// set on "run"
	sh2     *agent.Agent // used to report statistics
	running bool         // guard against double "run"

	// current targets, updated on discovery events
	targets   map[netip.Addr]tlstatshouse.GetTargetsResultBytes
	targetsMu sync.RWMutex

	// long poll requests
	requests   map[*rpc.HandlerContext]scrapeRequest
	requestsMu sync.Mutex
}

type scrapeRequest struct {
	hctx *rpc.HandlerContext
	addr netip.Addr
	args tlstatshouse.GetTargets2Bytes
}

func newScrapeServer(shard, replica int32) scrapeServer {
	var config *scrapeConfigService
	if shard == 1 && replica == 1 {
		config = newScrapeConfigService()
	}
	return scrapeServer{
		config:   config,
		targets:  make(map[netip.Addr]tlstatshouse.GetTargetsResultBytes),
		requests: make(map[*rpc.HandlerContext]scrapeRequest),
	}
}

func (s *scrapeServer) run(meta *metajournal.MetricsStorage, journal *metajournal.MetricMetaLoader, sh2 *agent.Agent) {
	if s.running {
		return
	}
	if s.config != nil {
		s.config.run(meta, journal, sh2)
	}
	s.sh2 = sh2
	s.running = true
}

func (s *scrapeServer) applyConfig(configID int32, configS string) {
	switch configID {
	case format.PrometheusConfigID:
		if s.config != nil {
			s.config.applyConfig(configS)
		}
		return
	case format.PrometheusGeneratedConfigID:
		break
	default:
		return
	}
	// build targets
	cs, err := DeserializeScrapeStaticConfig([]byte(configS))
	if err != nil {
		return
	}
	targets := make(map[netip.Addr]*tlstatshouse.GetTargetsResultBytes)
	for _, c := range cs {
		tags := []tl.DictionaryFieldStringBytes{{
			Key:   []byte(format.ScrapeNamespaceTagName),
			Value: []byte(c.Options.Namespace),
		}}
		for _, j := range c.Jobs {
			c2 := config.ScrapeConfig{
				JobName:        j.JobName,
				Scheme:         j.Scheme,
				MetricsPath:    j.MetricsPath,
				ScrapeInterval: j.ScrapeInterval,
				ScrapeTimeout:  j.ScrapeTimeout,
			}
			for _, g := range j.Groups {
				for _, t := range g.Targets {
					ipp, err := netip.ParseAddrPort(t)
					if err != nil {
						if s.sh2 != nil {
							// failure, targets_ready
							s.sh2.AddCounterHostStringBytes(
								s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeTargetDispatch, [format.MaxTags]int32{0, 1, 1}),
								[]byte(t), 1, 0, nil)
						}
						log.Printf("scrape target must have an IP address: %v\n", err)
						continue
					}
					v := targets[ipp.Addr()]
					if v == nil {
						v = &tlstatshouse.GetTargetsResultBytes{}
						targets[ipp.Addr()] = v
					}
					v.Targets = append(v.Targets, newPromTargetBytes(&c2, t, tags))
				}
			}
		}
	}
	var buf []byte
	for _, v := range targets {
		sort.Slice(v.Targets, func(i, j int) bool {
			lhs, rhs := v.Targets[i], v.Targets[j]
			compareResult := bytes.Compare(lhs.JobName, rhs.JobName)
			if compareResult != 0 {
				return compareResult < 0
			}
			return bytes.Compare(lhs.Url, rhs.Url) < 0
		})
		var err error
		t := tlstatshouse.GetTargetsResultBytes{Targets: v.Targets}
		buf, err = t.WriteBoxed(buf[:0], 0)
		if err != nil {
			continue
		}
		sum := sha256.Sum256(buf)
		v.Hash = sum[:]
	}
	// publish targets
	s.targetsMu.Lock()
	for k := range s.targets {
		s.targets[k] = tlstatshouse.GetTargetsResultBytes{}
	}
	for k, v := range targets {
		s.targets[k] = *v
		if s.sh2 != nil {
			// success, targets_ready
			s.sh2.AddCounterHostStringBytes(
				s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeTargetDispatch, [format.MaxTags]int32{0, 0, 1}),
				[]byte(k.String()), 1, 0, nil)
		}
	}
	s.targetsMu.Unlock()
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
	// config applied successfully, update hash
	h := sha1.Sum([]byte(configS))
	s.configH.Store(int32(binary.BigEndian.Uint32(h[:])))
}

func (s *scrapeServer) reportConfigHash() {
	v := s.configH.Load()
	s.sh2.AddCounterHostStringBytes(
		s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeConfigHash, [format.MaxTags]int32{0, v}),
		nil, 1, 0, nil)
}

func (s *scrapeServer) handleGetTargets(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.GetTargets2Bytes) error {
	ipp, err := netip.ParseAddrPort(hctx.RemoteAddr().String())
	if err != nil {
		return rpc.Error{
			Code:        data_model.RPCErrorScrapeAgentIP,
			Description: "scrape agent must have an IP address",
		}
	}
	// fast path, try respond without taking "requestsMu"
	req := scrapeRequest{
		hctx: hctx,
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
	if s.targets != nil {
		res, ok = s.targets[req.addr]
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
				[]byte(req.addr.String()), 1, 0, nil)
		} else {
			// success, targets_sent
			s.sh2.AddCounterHostStringBytes(
				s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeTargetDispatch, [format.MaxTags]int32{0, 0, 2}),
				[]byte(req.addr.String()), 1, 0, nil)
		}
	}
	return true, err
}

func (s *scrapeServer) CancelHijack(hctx *rpc.HandlerContext) {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	delete(s.requests, hctx)
}

func newPromTargetBytes(cfg *config.ScrapeConfig, addr string, labels []tl.DictionaryFieldStringBytes) tlstatshouse.PromTargetBytes {
	scrapeInterval := cfg.ScrapeInterval
	honorTimestamps := cfg.HonorTimestamps
	honorLabels := cfg.HonorLabels
	scrapeTimeout := cfg.ScrapeTimeout
	bodySizeLimit := int64(cfg.BodySizeLimit)
	labelLimit := int64(cfg.LabelLimit)
	httpConfigStr, _ := yaml.Marshal(cfg.HTTPClientConfig)
	if labelLimit < 0 {
		labelLimit = 0
	}
	labelNameLengthLimit := int64(cfg.LabelNameLengthLimit)
	if labelNameLengthLimit < 0 {
		labelNameLengthLimit = 0
	}
	labelValueLengthLimit := int64(cfg.LabelValueLengthLimit)
	if labelValueLengthLimit < 0 {
		labelValueLengthLimit = 0
	}
	l := url.URL{
		Scheme: cfg.Scheme,
		Host:   addr,
		Path:   cfg.MetricsPath,
	}
	res := tlstatshouse.PromTargetBytes{
		JobName:               []byte(cfg.JobName),
		Url:                   []byte(l.String()),
		Labels:                labels,
		ScrapeInterval:        int64(scrapeInterval),
		ScrapeTimeout:         int64(scrapeTimeout),
		BodySizeLimit:         bodySizeLimit,
		LabelLimit:            labelLimit,
		LabelNameLengthLimit:  labelNameLengthLimit,
		LabelValueLengthLimit: labelValueLengthLimit,
		HttpClientConfig:      httpConfigStr,
	}
	res.SetHonorTimestamps(honorTimestamps)
	res.SetHonorLabels(honorLabels)
	return res
}

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"

	"github.com/prometheus/common/config"
)

type Syncer interface {
	SyncTargets(ctx context.Context) ([]promTarget, error)
}

type syncerImpl struct {
	mx     sync.Mutex
	logOk  *log.Logger
	logErr *log.Logger

	groupLoader GroupLoader
	groupsHash  string
}

type GroupLoader func(ctxParent context.Context, version string) (res *tlstatshouse.GetTargetsResult, versionHash string, err error)

func NewSyncer(logOk, logErr *log.Logger, groupLoader GroupLoader) Syncer {
	return &syncerImpl{
		logOk:       logOk,
		logErr:      logErr,
		groupLoader: groupLoader,
	}
}

func (s *syncerImpl) SyncTargets(ctx context.Context) ([]promTarget, error) {
	s.logOk.Println("start to sync prom targets")
	s.mx.Lock()
	hash := s.groupsHash
	s.mx.Unlock()
	targets, newHash, err := s.groupLoader(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to load prom groups: %w", err)
	}
	for _, target := range targets.Targets {
		s.logOk.Printf("got new group %s: %s", target.JobName, target.Url)
	}
	s.mx.Lock()
	s.groupsHash = newHash
	s.mx.Unlock()
	var result []promTarget
	var buf []byte
	for _, target := range targets.Targets {
		httpConfig := config.DefaultHTTPClientConfig
		if len(target.HttpClientConfig) > 0 {
			err := yaml.Unmarshal([]byte(target.HttpClientConfig), &httpConfig)
			if err != nil {
				s.logErr.Printf("can't parse http client config: %s, group %s skipped", err.Error(), target.JobName)
				continue
			}
		}
		uri, err := url.Parse(target.Url)
		if err != nil {
			s.logErr.Printf("can't parse uri %s: %s", target.Url, err.Error())
		}
		buf, _ = target.Write(buf[:0])
		host, port := parseInstance(uri)
		result = append(result, promTarget{
			jobName:               target.JobName,
			url:                   target.Url,
			labels:                target.Labels,
			scrapeInterval:        time.Duration(target.ScrapeInterval),
			honorTimestamps:       target.IsSetHonorTimestamps(),
			honorLabels:           target.IsSetHonorLabels(),
			scrapeTimeout:         time.Duration(target.ScrapeTimeout),
			bodySizeLimit:         target.BodySizeLimit,
			LabelLimit:            target.LabelLimit,
			LabelNameLengthLimit:  target.LabelNameLengthLimit,
			LabelValueLengthLimit: target.LabelValueLengthLimit,
			httpConfigStr:         target.HttpClientConfig,
			targetStr:             string(buf),
			instance:              uri.Host,
			host:                  host,
			port:                  port,
		})
	}
	return result, nil
}

func parseInstance(uri *url.URL) (host string, port string) {
	host = uri.Hostname()
	port = uri.Port()
	if port == "" {
		switch uri.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		default:
			port = "-1"
		}
	}
	return host, port
}

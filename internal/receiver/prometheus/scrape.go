// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"

	"github.com/pkg/errors"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
)

type promTarget struct {
	jobName               string
	url                   string
	labels                map[string]string
	scrapeInterval        time.Duration
	honorTimestamps       bool
	honorLabels           bool
	scrapeTimeout         time.Duration
	bodySizeLimit         int64
	LabelLimit            int64
	LabelNameLengthLimit  int64
	LabelValueLengthLimit int64
	httpConfig            config.HTTPClientConfig
	httpConfigStr         string
	targetStr             string

	instance string
	host     string
	port     string
}

type scrapeLoop struct {
	mx          sync.Mutex
	target      promTarget
	relabelFunc func(labels.Labels) (lset labels.Labels, mustDropSeries bool)
	cache       *scrapeCache
	client      *http.Client
	req         *http.Request

	gzipr *gzip.Reader
	buf   *bufio.Reader

	logOk  *log.Logger
	logErr *log.Logger

	pusher     MetricPusher
	ctx        context.Context
	cancelLoop func()
}

type labelLimits struct {
	labelLimit            int64
	labelNameLengthLimit  int64
	labelValueLengthLimit int64
}

type loopKey struct {
	job string
	url string
}

type Scraper struct {
	loops        map[loopKey]*scrapeLoop
	parentCtx    context.Context
	cancelParent func()
	logOk        *log.Logger
	logErr       *log.Logger
	pusher       MetricPusher

	targetSyncer Syncer
}

const acceptHeader = `application/openmetrics-text;version=1.0.0,application/openmetrics-text;version=0.0.1;q=0.75,text/plain;version=0.0.4;q=0.5,*/*;q=0.1`
const UserAgent = "statshouse"

var errBodySizeLimit = errors.New("body size limit exceeded")
var nowFunc = time.Now

func NewScraper(pusher MetricPusher, syncer Syncer, logOk, logErr *log.Logger) *Scraper {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scraper{
		loops:        map[loopKey]*scrapeLoop{},
		pusher:       pusher,
		logOk:        logOk,
		logErr:       logErr,
		parentCtx:    ctx,
		cancelParent: cancel,
		targetSyncer: syncer,
	}
}

func (s *Scraper) Run() {
	s.logOk.Println("running prom scraper")
	backoffTimeout := time.Duration(0)
	for {
		targets, err := s.targetSyncer.SyncTargets(s.parentCtx)
		if err != nil {
			if !data_model.SilentRPCError(err) {
				s.logErr.Printf("failed to sync targets: %v", err)
				backoffTimeout = data_model.NextBackoffDuration(backoffTimeout)
				time.Sleep(backoffTimeout)
				continue
			}
			// fall through to enter long poll immediately
		} else {
			s.updateLoopsAsync(targets)
		}
		backoffTimeout = 0
		time.Sleep(data_model.JournalDDOSProtectionTimeout)
	}
}

func (s *Scraper) Stop() {
	s.cancelParent()
}

func (s *scrapeLoop) Stop() {
	s.cancelLoop()
}

func (s *scrapeLoop) applyChanges(target promTarget) {
	s.mx.Lock()
	defer s.mx.Unlock()
	if s.target.targetStr == target.targetStr {
		return
	}
	labelsToAdd, err := getLabelsToAdd(target)
	if err != nil {
		s.logErr.Printf("failed to get labels to add: %s", err.Error())
		return
	}
	if s.target.httpConfigStr != target.httpConfigStr {
		client, err := config.NewClientFromConfig(target.httpConfig, target.jobName)
		if err != nil {
			s.logErr.Printf("can't create loop for url: %s, group: %s: %s", target.url, target.jobName, err.Error())
			return
		}
		s.client = client
	}
	// can change s after this line
	s.target = target
	s.cache = newScrapeCache()

	s.relabelFunc = func(l labels.Labels) (lset labels.Labels, mustDropSeries bool) {
		return mutateSampleLabels(l, labelsToAdd, target.honorLabels)
	}
	if s.target.scrapeTimeout != target.scrapeTimeout {
		s.req = nil
	}
}

func (s *Scraper) updateLoopsAsync(newTargets []promTarget) {
	urlsMap := make(map[loopKey]struct{})
	for _, target := range newTargets {
		urlsMap[loopKey{
			job: target.jobName,
			url: target.url,
		}] = struct{}{}
	}
	for key, loop := range s.loops {
		if _, ok := urlsMap[key]; !ok {
			loop.Stop()
		}
		delete(s.loops, key)
	}
	for _, target := range newTargets {
		key := loopKey{
			job: target.jobName,
			url: target.url,
		}
		oldLoop := s.loops[key]
		if oldLoop != nil {
			oldLoop.applyChanges(target)
			continue
		}
		labelsToAdd, err := getLabelsToAdd(target)
		if err != nil {
			s.logErr.Printf("failed to get labels to add: %s", err.Error())
			continue
		}
		client, err := config.NewClientFromConfig(target.httpConfig, target.jobName)
		if err != nil {
			s.logErr.Printf("can't create loop for url: %s, group: %s: %s", target.url, target.jobName, err.Error())
			continue
		}

		loopCtx, cancelLoop := context.WithCancel(context.Background())
		newLoop := &scrapeLoop{
			target: target,
			logOk:  s.logOk,
			logErr: s.logErr,
			pusher: s.pusher,
			cache:  newScrapeCache(),
			relabelFunc: func(l labels.Labels) (lset labels.Labels, mustDropSeries bool) {
				return mutateSampleLabels(l, labelsToAdd, target.honorLabels)
			},
			client:     client,
			ctx:        loopCtx,
			cancelLoop: cancelLoop,
		}
		s.loops[key] = newLoop
		go func(loopCtx, parentCtx context.Context, newLoop *scrapeLoop) {
			runLoop(loopCtx, parentCtx, newLoop, s.logErr)
		}(loopCtx, s.parentCtx, newLoop)
	}
}

func getLabelsToAdd(t promTarget) (result labels.Labels, err error) {
	const jobLabel = "job"
	const instanceLabel = "instance"
	result = append(result, labels.Label{
		Name:  jobLabel,
		Value: t.jobName,
	})
	result = append(result, labels.Label{
		Name:  instanceLabel,
		Value: t.instance,
	})
	for k, v := range t.labels {
		if k != instanceLabel && k != jobLabel {
			result = append(result, labels.Label{
				Name:  k,
				Value: v,
			})
		}
	}
	return result, nil
}

func runLoop(loopCtx, prtCtx context.Context, loop *scrapeLoop,
	logErr *log.Logger) {
	timer := time.NewTimer(scrapeInterval(nowFunc(), loop.target.scrapeInterval))
loop:
	for {
		select {
		case <-loopCtx.Done():
			timer.Stop()
			break loop
		case <-prtCtx.Done():
			timer.Stop()
			break loop
		case <-timer.C:
			now := nowFunc()
			err := loop.scrapeAndReport(prtCtx, now)
			if err != nil {
				logErr.Println("failed to scrape target: ", err)
			}
			timer = time.NewTimer(scrapeInterval(nowFunc(), loop.target.scrapeInterval))
		}
	}
}

func (s *scrapeLoop) scrapeAndReport(ctx context.Context, when time.Time) error {
	s.logOk.Println("starting scrape ", s.target.url)
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	ctx, cancelFunc := context.WithTimeout(ctx, s.target.scrapeTimeout)
	defer cancelFunc()
	startTime := nowFunc()
	contentType, err := s.scrape(ctx, buf)
	if err != nil {
		s.pushScrapeTimeMetric(when, time.Since(startTime), format.TagValueIDScrapeError)
		return fmt.Errorf("failed to scrape %s, %w", s.target.url, err)
	}
	s.parseAndReport(buf.Bytes(), contentType)
	finishTime := nowFunc()
	s.pushScrapeTimeMetric(when, finishTime.Sub(startTime), 0)
	return nil
}

func newTag(k, v string) tl.DictionaryFieldStringBytes {
	return tl.DictionaryFieldStringBytes{Key: []byte(k), Value: []byte(v)}
}

func (s *scrapeLoop) pushScrapeTimeMetric(when time.Time, d time.Duration, err int) {
	tagsCount := 2
	if err != 0 {
		tagsCount++
	}
	if s.pusher.IsLocal() {
		metric := &tlstatshouse.MetricBytes{
			Tags: make([]tl.DictionaryFieldStringBytes, 0, tagsCount),
		}
		metric.Name = []byte(format.BuiltinMetricNamePromScrapeTime)
		metric.Tags = append(metric.Tags, newTag("key2", s.target.jobName))
		metric.Tags = append(metric.Tags, newTag("key3", s.target.host))
		metric.Tags = append(metric.Tags, newTag("key4", s.target.port))

		if err != 0 {
			metric.Tags = append(metric.Tags, newTag("key5", strconv.Itoa(err)))
		}
		metric.SetValue([]float64{d.Seconds()})
		metric.SetTs(uint32(when.Unix()))
		s.pusher.PushLocal(metric)
	}
}

func (s *scrapeLoop) parseAndReport(content []byte, contentType string) {
	if !s.pusher.IsLocal() {
		s.logErr.Println("can't push prom metric to remote sh")
		return
	}
	sentSuccessCount := 0
	sentFailedCount := 0
	s.mx.Lock()
	cache := s.cache
	target := s.target
	relabelFunc := s.relabelFunc
	s.mx.Unlock()
	parser, err := textparse.New(content, contentType)
	if err != nil {
		s.logErr.Println("failed to create parser: ", err.Error())
	}
	scrapeTime := time.Now().Unix() * 1_000
	seriesToSend := make([]tlstatshouse.MetricBytes, 0)
	var lset labels.Labels

	for {
		entry, err := parser.Next()
		if err == io.EOF {
			break
		}
		switch entry {
		case textparse.EntryType:
			cache.setType(parser.Type())
			continue
		case textparse.EntryHelp:
			continue
		case textparse.EntryUnit:
			continue
		case textparse.EntryComment:
			continue
		default:
		}

		series, tsPointer, value := parser.Series()
		var ts int64
		if target.honorTimestamps && tsPointer != nil {
			ts = *tsPointer
		}
		if ts == 0 {
			ts = scrapeTime
		}
		if cache.IsDropped(series) {
			sentFailedCount++
			continue
		}
		var mustDropSeries bool
		lset = lset[:0]
		parser.Metric(&lset)
		lset, mustDropSeries = relabelFunc(lset)
		if mustDropSeries {
			seriesStr := string(series)
			cache.AddDropped(seriesStr)
			sentFailedCount++
			continue
		}

		// в отличие от prom пропускаем только текущий sample, а не весь scrape
		if err := verifyLabelLimits(lset, labelLimits{
			labelLimit:            target.LabelLimit,
			labelNameLengthLimit:  target.LabelNameLengthLimit,
			labelValueLengthLimit: target.LabelValueLengthLimit,
		}); err != nil {
			sentFailedCount++
			// todo lgo err
			continue
		}

		meta, ok := cache.getMeta(lset)
		if !ok {
			sentFailedCount++
			// todo handle untyped
			continue
		}

		seriesToSend = meta.processSample(value, ts, seriesToSend[:0])
		for _, metric := range seriesToSend {
			s.pusher.PushLocal(&metric)
			sentSuccessCount++
		}
	}
	seriesToSend = cache.metadata.calculateHistograms(seriesToSend[:0])
	for _, metric := range seriesToSend {
		s.pusher.PushLocal(&metric)
		sentSuccessCount++
	}
	s.logOk.Println("pushed ", sentSuccessCount, " series")
}

func (s *scrapeLoop) scrape(ctx context.Context, w io.Writer) (contentType string, err error) {
	s.mx.Lock()
	if s.req == nil {
		req, err := http.NewRequest("GET", s.target.url, nil)
		if err != nil {
			return "", err
		}
		req.Header.Add("Accept", acceptHeader)
		req.Header.Add("Accept-Encoding", "gzip")
		req.Header.Set("User-Agent", UserAgent)
		req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", strconv.FormatFloat(s.target.scrapeTimeout.Seconds(), 'f', -1, 64))
		s.req = req
	}
	client := s.client
	req := s.req
	target := s.target
	s.mx.Unlock()
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return "", errors.Errorf("server returned HTTP status %s", resp.Status)
	}

	if target.bodySizeLimit <= 0 {
		target.bodySizeLimit = math.MaxInt64
	}
	if resp.Header.Get("Content-Encoding") != "gzip" {
		n, err := io.Copy(w, io.LimitReader(resp.Body, target.bodySizeLimit))
		if err != nil {
			return "", err
		}
		if n >= target.bodySizeLimit {
			return "", errBodySizeLimit
		}
		return resp.Header.Get("Content-Type"), nil
	}

	if s.gzipr == nil {
		s.buf = bufio.NewReader(resp.Body)
		s.gzipr, err = gzip.NewReader(s.buf)
		if err != nil {
			return "", err
		}
	} else {
		s.buf.Reset(resp.Body)
		if err = s.gzipr.Reset(s.buf); err != nil {
			return "", err
		}
	}

	n, err := io.Copy(w, io.LimitReader(s.gzipr, target.bodySizeLimit))
	_ = s.gzipr.Close()
	if err != nil {
		return "", err
	}
	if n >= target.bodySizeLimit {
		return "", errBodySizeLimit
	}
	return resp.Header.Get("Content-Type"), nil
}

func scrapeInterval(now time.Time, interval time.Duration) time.Duration {
	truncated := now.Truncate(interval)
	// if now.Sub(truncated) < interval/10 {
	//	return truncated.Add(interval / 10).Sub(now)
	// }
	return truncated.Add(interval + time.Second/10).Sub(now)
}

func filterLabels(lset labels.Labels) labels.Labels {
	i := 0
	for _, label := range lset {
		if !strings.HasPrefix(label.Name, "__") {
			lset[i] = label
			i++
		}
	}
	lset = lset[:i]
	return lset
}

// todo write to relabelFunc
func mutateSampleLabels(lset labels.Labels, labelsToAdd labels.Labels, honor bool) (result labels.Labels, mustDropSeries bool) {
	lb := labels.NewBuilder(lset)
	targetLabels := labelsToAdd

	if honor {
		for _, l := range targetLabels {
			if !lset.Has(l.Name) {
				lb.Set(l.Name, l.Value)
			}
		}
	} else {
		var conflictingExposedLabels labels.Labels
		for _, l := range targetLabels {
			existingValue := lset.Get(l.Name)
			if existingValue != "" {
				conflictingExposedLabels = append(conflictingExposedLabels, labels.Label{Name: l.Name, Value: existingValue})
			}
			// It is now safe to set the target label.
			lb.Set(l.Name, l.Value)
		}

		if len(conflictingExposedLabels) > 0 {
			resolveConflictingExposedLabels(lb, lset, targetLabels, conflictingExposedLabels)
		}
	}

	res := lb.Labels()

	// we ignore relabel config
	// if len(rc) > 0 {
	//	res = relabel.Process(res, rc...)
	// }

	return res, res == nil
}

func resolveConflictingExposedLabels(lb *labels.Builder, exposedLabels, targetLabels, conflictingExposedLabels labels.Labels) {
	sort.SliceStable(conflictingExposedLabels, func(i, j int) bool {
		return len(conflictingExposedLabels[i].Name) < len(conflictingExposedLabels[j].Name)
	})

	for i, l := range conflictingExposedLabels {
		newName := l.Name
		for {
			newName = model.ExportedLabelPrefix + newName
			if !exposedLabels.Has(newName) &&
				!targetLabels.Has(newName) &&
				!conflictingExposedLabels[:i].Has(newName) {
				conflictingExposedLabels[i].Name = newName
				break
			}
		}
	}

	for _, l := range conflictingExposedLabels {
		lb.Set(l.Name, l.Value)
	}
}

func verifyLabelLimits(lset labels.Labels, limits labelLimits) error {
	met := lset.Get(labels.MetricName)
	if limits.labelLimit > 0 {
		nbLabels := len(lset)
		if int64(nbLabels) > limits.labelLimit {
			return fmt.Errorf("label_limit exceeded (metric: %.50s, number of label: %d, limit: %d)", met, nbLabels, limits.labelLimit)
		}
	}

	if limits.labelNameLengthLimit == 0 && limits.labelValueLengthLimit == 0 {
		return nil
	}

	for _, l := range lset {
		if limits.labelNameLengthLimit > 0 {
			nameLength := len(l.Name)
			if int64(nameLength) > limits.labelNameLengthLimit {
				return fmt.Errorf("label_name_length_limit exceeded (metric: %.50s, label: %.50v, name length: %d, limit: %d)", met, l, nameLength, limits.labelNameLengthLimit)
			}
		}

		if limits.labelValueLengthLimit > 0 {
			valueLength := len(l.Value)
			if int64(valueLength) > limits.labelValueLengthLimit {
				return fmt.Errorf("label_value_length_limit exceeded (metric: %.50s, label: %.50v, value length: %d, limit: %d)", met, l, valueLength, limits.labelValueLengthLimit)
			}
		}
	}
	return nil
}

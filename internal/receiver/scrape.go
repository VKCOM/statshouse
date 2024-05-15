package receiver

import (
	"bytes"
	"context"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

type scrape struct {
	agent   *agent.Agent
	handler Handler
}

type scraper struct {
	mu         sync.Mutex
	instance   string
	options    scrapeOptions
	handler    Handler
	counters   map[uint64]*scrapeCounter
	histograms map[string]*scrapeHistogram
	hash       hash.Hash64

	// lifetime management
	ctx    context.Context
	cancel func()
	dead   bool

	// HTTP client
	client  http.Client
	request *http.Request

	buffer bytes.Buffer
	metric tlstatshouse.MetricBytes
}

type scrapeTarget struct {
	url string
	opt scrapeOptions
}

type scrapeOptions struct {
	interval     time.Duration
	timeout      time.Duration
	namespace    string
	job          string
	gaugeMetrics map[string]bool
}

type scrapeCounter struct {
	name        string
	description string
	tags        []labels.Label
	value       float64
}

type scrapeHistogram struct {
	nameB        string // "_bucket" metric full name
	nameS        string // "_sum" metric full name
	tags         []labels.Label
	descriptionB string // "_bucket" metric description
	descriptionS string // "_sum" metric description
	buckets      []string
	series       map[uint64]*scrapeHistogramSeries
}

type scrapeHistogramSeries struct {
	sum    float64
	count  float64
	bucket []float64
}

func RunScrape(sh *agent.Agent, h Handler) {
	s := scrape{agent: sh, handler: h}
	go s.run()
}

func (s *scrape) run() {
	log.Println("scrape running")
	var lastHash string
	var backoffTimeout time.Duration
	m := map[string]*scraper{} // URL key
	for {
		targets, hash, err := s.getTargets(lastHash)
		if err != nil && !data_model.SilentRPCError(err) {
			// backoff then try again
			backoffTimeout = data_model.NextBackoffDuration(backoffTimeout)
			time.Sleep(backoffTimeout)
			continue
		}
		if err == nil {
			lastHash = hash
			log.Println("scrape target count", len(targets))
			s.update(m, targets)
		}
		backoffTimeout = 0
		time.Sleep(data_model.JournalDDOSProtectionTimeout)
	}
}

func (s *scrape) getTargets(hash string) ([]scrapeTarget, string, error) {
	targets, newHash, err := s.agent.LoadPromTargets(context.Background(), hash)
	if err != nil {
		return nil, "", err
	}
	var gaugeMetrics map[string]bool
	if len(targets.GaugeMetrics) != 0 {
		gaugeMetrics = make(map[string]bool, len(targets.GaugeMetrics))
		for _, v := range targets.GaugeMetrics {
			gaugeMetrics[v] = true
		}
	}
	var res []scrapeTarget
	for _, v := range targets.Targets {
		var namespace string
		if v.Labels != nil {
			namespace = v.Labels[format.ScrapeNamespaceTagName]
		}
		res = append(res, scrapeTarget{
			url: string(v.Url),
			opt: scrapeOptions{
				interval:     time.Duration(v.ScrapeInterval),
				timeout:      time.Duration(v.ScrapeTimeout),
				namespace:    namespace,
				job:          v.JobName,
				gaugeMetrics: gaugeMetrics,
			},
		})
	}
	return res, newHash, nil
}

func (s *scrape) update(m map[string]*scraper, targets []scrapeTarget) {
	for _, s := range m {
		s.dead = true
	}
	for _, t := range targets {
		if v := m[t.url]; v != nil {
			v.mu.Lock()
			v.options = t.opt
			v.mu.Unlock()
			v.dead = false
			log.Println("scrape update", t)
		} else {
			log.Println("scrape create", t)
			m[t.url] = s.newScraper(t)
		}
	}
	for k, v := range m {
		if v.dead {
			v.cancel()
			delete(m, k)
		}
	}
}

func (s *scrape) newScraper(t scrapeTarget) *scraper {
	// configure HTTP client
	req, err := http.NewRequest(http.MethodGet, t.url, nil)
	if err != nil {
		log.Printf("scrape URL parse error %q: %v\n", t.url, err)
		return nil
	}
	req.Header.Add("Accept", "application/openmetrics-text;version=1.0.0,application/openmetrics-text;version=0.0.1;q=0.75,text/plain;version=0.0.4;q=0.5,*/*;q=0.1")
	req.Header.Set("User-Agent", "statshouse")
	// build instance tag value
	instance := srvfunc.HostnameForStatshouse()
	if port := req.URL.Port(); port != "" {
		instance += ":" + port
	} else {
		switch req.URL.Scheme {
		case "http":
			instance += ":80"
		case "https":
			instance += ":443"
		}
	}
	// create and run
	ctx, cancel := context.WithCancel(context.Background())
	res := &scraper{
		instance: instance,
		options:  t.opt,
		handler:  s.handler,
		ctx:      ctx,
		cancel:   cancel,
		request:  req,
	}
	go res.run()
	return res
}

func (s *scraper) run() {
	// get a resettable timer
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	log.Println("scrape start", s)
scrape_loop:
	for s.ctx.Err() == nil {
		// read options
		var opt scrapeOptions
		s.mu.Lock()
		opt = s.options
		s.mu.Unlock()
		// wait next interval start
		now := time.Now()
		timer.Reset(now.Truncate(opt.interval).Add(opt.interval).Sub(now))
		select {
		case <-s.ctx.Done():
			break scrape_loop
		case start := <-timer.C:
			// work
			err := s.scrape(opt)
			dur := time.Since(start)
			s.reportScrapeTime(opt.job, err, dur)
		}
	}
	log.Println("scrape stop", s)
}

func (s *scraper) scrape(opt scrapeOptions) error {
	buf, contentType, err := s.readBytes(opt.timeout)
	if err != nil {
		return err
	}
	p, err := textparse.New(buf, contentType)
	if err != nil {
		return err
	}
	var name string
	var description string
	var metricType textparse.MetricType
	type histogram struct {
		buckets []string
		series  map[uint64]*scrapeHistogramSeries // tags hash -> bucket values
	}
	var histograms map[string]*histogram
	var counters map[uint64]float64
	b := s.metric
	for {
		var l labels.Labels
		for i := 0; ; i++ {
			var entry textparse.Entry
			entry, err = p.Next()
			if err == io.EOF {
				break
			}
			if entry == textparse.EntrySeries {
				p.Metric(&l)
				break
			}
			if i == 0 {
				name = ""
				description = ""
				metricType = ""
			}
			switch entry {
			case textparse.EntryHelp:
				_, v := p.Help()
				description = string(v)
			case textparse.EntryType:
				var metricNameB []byte
				metricNameB, metricType = p.Type()
				name = string(metricNameB)
				if metricType == textparse.MetricTypeCounter && opt.gaugeMetrics[name] {
					metricType = textparse.MetricTypeGauge
				}
			}
		}
		if err == io.EOF {
			break
		}
		if name == "" || metricType == "" {
			continue
		}
		_, _, v := p.Series()
		switch metricType {
		case textparse.MetricTypeGauge:
			s.resetMetric(&b, opt.job, len(l))
			setMetricName(&b, opt.namespace, name)
			for _, v := range l {
				if v.Name != labels.MetricName {
					b.Tags = appendTag(b.Tags, v.Name, v.Value)
				}
			}
			setMetricValue(&b, v)
			s.handler.HandleMetrics(data_model.HandlerArgs{
				MetricBytes:    &b,
				Description:    description,
				ScrapeInterval: int(opt.interval.Seconds()),
			})
		case textparse.MetricTypeCounter:
			if s.hash == nil {
				s.hash = fnv.New64()
			}
			for _, v := range l {
				s.hash.Write([]byte(v.Name))
				s.hash.Write([]byte(v.Value))
			}
			hashSum := s.hash.Sum64()
			s.hash.Reset()
			if s.counters == nil {
				s.counters = make(map[uint64]*scrapeCounter)
			}
			if metric := s.counters[hashSum]; metric == nil {
				// initialize counter
				metric = &scrapeCounter{
					description: description,
					tags:        make([]labels.Label, 0, len(l)),
					value:       v,
				}
				if opt.namespace != "" {
					metric.name = opt.namespace + format.NamespaceSeparator + name
				} else {
					metric.name = name
				}
				for _, v := range l {
					if v.Name != labels.MetricName {
						metric.tags = append(metric.tags, v)
					}
				}
				s.counters[hashSum] = metric
			}
			if counters == nil {
				counters = map[uint64]float64{hashSum: v}
			} else {
				counters[hashSum] = v
			}
		case textparse.MetricTypeHistogram:
			if s.hash == nil {
				s.hash = fnv.New64()
			}
			var fullName string
			for _, v := range l {
				switch v.Name {
				case labels.MetricName:
					fullName = v.Value
				case labels.BucketLabel:
					// skip
				default:
					s.hash.Write([]byte(v.Name))
					s.hash.Write([]byte(v.Value))
				}
			}
			hashSum := s.hash.Sum64()
			s.hash.Reset()
			if fullName == "" || len(fullName) == len(name) {
				continue // should not happen
			}
			if s.histograms == nil {
				s.histograms = make(map[string]*scrapeHistogram)
			}
			metric := s.histograms[name]
			if metric == nil {
				// initialize histogram
				metric = &scrapeHistogram{
					tags:         make([]labels.Label, 0, len(l)),
					series:       make(map[uint64]*scrapeHistogramSeries),
					descriptionS: description,
				}
				if opt.namespace != "" {
					metric.nameB = opt.namespace + format.NamespaceSeparator + name + "_bucket"
					metric.nameS = opt.namespace + format.NamespaceSeparator + name + "_sum"
				} else {
					metric.nameB = name + "_bucket"
					metric.nameS = name + "_sum"
				}
				for _, v := range l {
					switch v.Name {
					case labels.MetricName, labels.BucketLabel:
						// skip
					default:
						metric.tags = append(metric.tags, v)
					}
				}
				s.histograms[name] = metric
			}
			// append series value
			if histograms == nil {
				histograms = make(map[string]*histogram)
			}
			curr := histograms[name]
			if curr == nil {
				curr = &histogram{
					series: make(map[uint64]*scrapeHistogramSeries),
				}
				histograms[name] = curr
			}
			series := curr.series[hashSum]
			if series == nil {
				series = &scrapeHistogramSeries{}
				curr.series[hashSum] = series
			}
			switch {
			case strings.HasSuffix(fullName, "_bucket"):
				series.bucket = append(series.bucket, v)
				// remember buckets if histogram does not yet exist
				if len(metric.buckets) == 0 && len(curr.series) == 1 {
					for _, v := range l {
						if v.Name == labels.BucketLabel {
							curr.buckets = append(curr.buckets, v.Value)
							break
						}
					}
				}
			case strings.HasSuffix(fullName, "_sum"):
				series.sum = v
			case strings.HasSuffix(fullName, "_count"):
				series.count = v
			}
		}
	}
	for hashSum, currValue := range counters {
		if prev := s.counters[hashSum]; prev != nil {
			v := currValue - prev.value
			if v > 0 {
				s.resetMetric(&b, opt.job, len(prev.tags))
				b.Name = appendString(b.Name, prev.name)
				for _, tag := range prev.tags {
					b.Tags = appendTag(b.Tags, tag.Name, tag.Value)
				}
				b.SetCounter(v)
				s.handler.HandleMetrics(data_model.HandlerArgs{
					MetricBytes:    &b,
					Description:    prev.description,
					ScrapeInterval: int(opt.interval.Seconds()),
				})
			}
			prev.value = currValue
		}
	}
	for metricName, curr := range histograms {
		// calculate buckets diff
		for _, s := range curr.series {
			for i := len(s.bucket); i > 1; i-- {
				s.bucket[i-1] -= s.bucket[i-2]
			}
		}
		metric := s.histograms[metricName]
		if len(metric.buckets) == 0 {
			// build description
			var sb strings.Builder
			if len(metric.descriptionS) != 0 {
				sb.WriteString(metric.descriptionS)
				sb.WriteByte('\n')
				sb.WriteByte('\n')
			}
			sb.WriteString(format.HistogramBucketsStartMark)
			sb.WriteString(curr.buckets[0])
			for i := 1; i < len(curr.buckets); i++ {
				sb.WriteByte(format.HistogramBucketsDelimC)
				sb.WriteString(curr.buckets[i])
			}
			sb.WriteByte(format.HistogramBucketsEndMarkC)
			metric.descriptionB = sb.String()
			// encode bucket tag values
			metric.buckets = make([]string, len(curr.buckets))
			for i := 0; i < len(curr.buckets); i++ {
				var bucket float64
				if bucket, err = strconv.ParseFloat(curr.buckets[i], 32); err == nil {
					metric.buckets[i] = strconv.FormatInt(int64(statshouse.LexEncode(float32(bucket))), 10)
				}
			}
		}
		for hashSum, curr := range curr.series {
			if prev, ok := metric.series[hashSum]; ok {
				// "_bucket" metric
				for i := 0; i < len(prev.bucket) && i < len(curr.bucket) && i < len(metric.buckets); i++ {
					v := curr.bucket[i] - prev.bucket[i]
					if v > 0 {
						s.resetMetric(&b, opt.job, len(metric.tags)+1)
						b.Name = appendString(b.Name, metric.nameB)
						for _, v := range metric.tags {
							b.Tags = appendTag(b.Tags, v.Name, v.Value)
						}
						b.Tags = appendTag(b.Tags, format.LETagName, metric.buckets[i])
						b.SetCounter(v)
						s.handler.HandleMetrics(data_model.HandlerArgs{
							MetricBytes:    &b,
							Description:    metric.descriptionB,
							ScrapeInterval: int(opt.interval.Seconds()),
						})
					}
				}
			}
			// "_sum" metric
			if curr.count > 0 {
				s.resetMetric(&b, opt.job, len(metric.tags))
				b.Name = appendString(b.Name, metric.nameS)
				for _, v := range metric.tags {
					b.Tags = appendTag(b.Tags, v.Name, v.Value)
				}
				b.SetCounter(curr.count)
				setMetricValue(&b, curr.sum)
				s.handler.HandleMetrics(data_model.HandlerArgs{
					MetricBytes:    &b,
					Description:    metric.descriptionS,
					ScrapeInterval: int(opt.interval.Seconds()),
				})
			}
			metric.series[hashSum] = curr
		}
	}
	s.metric = b
	return nil
}

func (s *scraper) readBytes(timeout time.Duration) ([]byte, string, error) {
	// set timeout
	s.request.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", strconv.FormatFloat(timeout.Seconds(), 'f', -1, 64))
	s.client.Timeout = timeout
	// send request
	resp, err := s.client.Do(s.request)
	if err != nil {
		return nil, "", err
	}
	defer func() {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf(resp.Status)
	}
	// read response
	s.buffer.Reset()
	_, err = io.Copy(&s.buffer, resp.Body)
	if err != nil {
		return nil, "", err
	}
	return s.buffer.Bytes(), resp.Header.Get("Content-Type"), nil
}

func (s *scraper) resetMetric(b *tlstatshouse.MetricBytes, job string, tagsCap int) {
	b.Reset()
	tagsCap += 2 // job, instance
	if cap(b.Tags) < tagsCap {
		b.Tags = make([]tl.DictionaryFieldStringBytes, 0, tagsCap)
	}
	b.Tags = b.Tags[:2]
	setTagAt(b.Tags, 0, "job", job)
	setTagAt(b.Tags, 1, "instance", s.instance)
}

func (s *scraper) reportScrapeTime(job string, err error, v time.Duration) {
	b := s.metric
	b.Reset()
	b.Name = appendString(s.metric.Name, format.BuiltinMetricNamePromScrapeTime)
	b.Tags = appendTag(b.Tags, "2", job)
	if err != nil {
		b.Tags = appendTag(b.Tags, "5", "1") // error
	} else {
		b.Tags = appendTag(b.Tags, "5", "2") // ok
	}
	setMetricValue(&b, v.Seconds())
	s.handler.HandleMetrics(data_model.HandlerArgs{
		MetricBytes: &s.metric,
	})
	s.metric = b
}

func (s *scraper) String() string {
	return fmt.Sprintf("URL %s, instance %s, %s", s.request.URL, s.instance, s.options)
}

func (t scrapeTarget) String() string {
	return fmt.Sprintf("URL %s, %s", t.url, t.opt)
}

func (opt scrapeOptions) String() string {
	return fmt.Sprintf("namespace %s, job %s, interval %v, timeout %v", opt.namespace, opt.job, opt.interval, opt.timeout)
}

func setMetricName(b *tlstatshouse.MetricBytes, namespace string, name string) {
	n := len(name)
	if namespace != "" {
		n += len(namespace)
		n += len(format.NamespaceSeparator)
	}
	s := b.Name
	if cap(s) < n {
		s = make([]byte, n)
	} else {
		s = s[:n]
	}
	if namespace != "" {
		n = copy(s, namespace)
		n += copy(s[n:], format.NamespaceSeparator)
		copy(s[n:], name)
	} else {
		copy(s, name)
	}
	b.Name = s
}

func setMetricValue(b *tlstatshouse.MetricBytes, v float64) {
	if cap(b.Value) < 1 {
		b.Value = make([]float64, 1)
	} else {
		b.Value = b.Value[:1]
	}
	b.Value[0] = v
	b.FieldsMask |= 1 << 1
}

func appendTag(s []tl.DictionaryFieldStringBytes, name, value string) []tl.DictionaryFieldStringBytes {
	n := len(s)
	if cap(s) < n+1 {
		t := make([]tl.DictionaryFieldStringBytes, n+1)
		copy(t, s)
		s = t
	} else {
		s = s[:n+1]
	}
	setTagAt(s, n, name, value)
	return s
}

func setTagAt(s []tl.DictionaryFieldStringBytes, i int, k, v string) {
	s[i].Key = appendString(s[i].Key, k)
	s[i].Value = appendString(s[i].Value, v)
}

func appendString(s []byte, str string) []byte {
	if cap(s) < len(str) {
		s = make([]byte, len(str))
	} else {
		s = s[:len(str)]
	}
	copy(s, str)
	return s
}

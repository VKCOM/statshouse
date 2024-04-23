package receiver

import (
	"bytes"
	"context"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
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
	instance   tl.DictionaryFieldStringBytes
	options    scrapeOptions
	handler    Handler
	histograms map[string]*scrapeHistogram
	hash       hash.Hash64

	// lifetime management
	ctx    context.Context
	cancel func()
	dead   bool

	// HTTP client
	client  http.Client
	request *http.Request
}

type scrapeTarget struct {
	url string
	opt scrapeOptions
}

type scrapeOptions struct {
	interval  time.Duration
	timeout   time.Duration
	namespace string
	job       tl.DictionaryFieldStringBytes
}

type scrapeHistogram struct {
	nameB        []byte // "_bucket" metric full name
	nameS        []byte // "_sum" metric full name
	tags         []tl.DictionaryFieldStringBytes
	descriptionB string // "_bucket" metric description
	descriptionS string // "_sum" metric description
	buckets      [][]byte
	series       map[uint64]*scrapeHistogramSeries
}

type scrapeHistogramSeries struct {
	sum    float64
	count  float64
	bucket []float64
}

var jobTagName = []byte("job")
var instanceTagName = []byte("instance")
var bucketTagName = []byte(format.LETagName)

func RunScrape(sh *agent.Agent, h Handler) {
	s := scrape{agent: sh, handler: h}
	go s.run()
}

func (s *scrape) run() {
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
	var res []scrapeTarget
	for _, v := range targets.Targets {
		var namespace string
		if v.Labels != nil {
			namespace = v.Labels[format.ScrapeNamespaceTagName]
		}
		res = append(res, scrapeTarget{
			url: string(v.Url),
			opt: scrapeOptions{
				interval:  time.Duration(v.ScrapeInterval),
				timeout:   time.Duration(v.ScrapeTimeout),
				namespace: namespace,
				job:       tl.DictionaryFieldStringBytes{Key: jobTagName, Value: []byte(v.JobName)},
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
		} else {
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
		return nil
	}
	req.Header.Add("Accept", "application/openmetrics-text;version=1.0.0,application/openmetrics-text;version=0.0.1;q=0.75,text/plain;version=0.0.4;q=0.5,*/*;q=0.1")
	req.Header.Set("User-Agent", "statshouse")
	// build instance tag
	instance := tl.DictionaryFieldStringBytes{Key: instanceTagName}
	if v, err := url.Parse(t.url); err == nil {
		port := v.Port()
		if port == "" {
			switch v.Scheme {
			case "http":
				port = "80"
			case "https":
				port = "443"
			}
		}
		instance.Value = []byte(v.Hostname() + ":" + port)
	} else {
		instance.Value = []byte(srvfunc.HostnameForStatshouse())
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
	// scrape loop
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
			return
		case <-timer.C:
			// work
			_ = s.scrape(opt)
		}
	}
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
	var metricType textparse.MetricType
	var description string
	type histogram struct {
		buckets []string
		series  map[uint64]*scrapeHistogramSeries // tags hash -> bucket values
	}
	var histograms map[string]*histogram
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
				metricType = ""
				description = ""
			}
			switch entry {
			case textparse.EntryHelp:
				_, v := p.Help()
				description = string(v)
			case textparse.EntryType:
				_, metricType = p.Type()
			}
		}
		if err == io.EOF {
			break
		}
		_, _, v := p.Series()
		switch metricType {
		case textparse.MetricTypeCounter, textparse.MetricTypeGauge:
			b := tlstatshouse.MetricBytes{
				Tags: make([]tl.DictionaryFieldStringBytes, 0, len(l)+1),
			}
			b.Tags = append(b.Tags, s.options.job)
			b.Tags = append(b.Tags, s.instance)
			for _, v := range l {
				if v.Name == "" {
					continue
				}
				if v.Name == labels.MetricName {
					if opt.namespace != "" {
						b.Name = []byte(opt.namespace + format.NamespaceSeparator + v.Value)
					} else {
						b.Name = []byte(v.Value)
					}
				} else {
					b.Tags = append(b.Tags, tl.DictionaryFieldStringBytes{Key: []byte(v.Name), Value: []byte(v.Value)})
				}
			}
			if len(b.Name) != 0 {
				b.SetValue([]float64{v})
				s.handler.HandleMetrics(data_model.HandlerArgs{
					MetricBytes:    &b,
					Description:    description,
					ScrapeInterval: int(opt.interval.Seconds()),
				})
			}
		case textparse.MetricTypeHistogram:
			if s.hash == nil {
				s.hash = fnv.New64()
			}
			var name, baseName string
			for _, v := range l {
				switch v.Name {
				case labels.MetricName:
					name = v.Value
					baseName = strings.TrimSuffix(v.Value, "_bucket")
					if len(baseName) == len(v.Value) {
						baseName = strings.TrimSuffix(v.Value, "_sum")
						if len(baseName) == len(v.Value) {
							baseName = strings.TrimSuffix(v.Value, "_count")
						}
					}
				case labels.BucketLabel:
					// skip
				default:
					s.hash.Write([]byte(v.Name))
					s.hash.Write([]byte(v.Value))
				}
			}
			hashSum := s.hash.Sum64()
			s.hash.Reset()
			if name == "" || len(name) == len(baseName) {
				continue // should not happen
			}
			if s.histograms == nil {
				s.histograms = make(map[string]*scrapeHistogram)
			}
			prevH := s.histograms[baseName]
			if prevH == nil {
				// initialize histogram
				prevH = &scrapeHistogram{
					tags:         make([]tl.DictionaryFieldStringBytes, 0, len(l)+1),
					series:       make(map[uint64]*scrapeHistogramSeries),
					descriptionS: description,
				}
				if opt.namespace != "" {
					prevH.nameB = []byte(opt.namespace + format.NamespaceSeparator + baseName + "_bucket")
					prevH.nameS = []byte(opt.namespace + format.NamespaceSeparator + baseName + "_sum")
				} else {
					prevH.nameB = []byte(baseName + "_bucket")
					prevH.nameS = []byte(baseName + "_sum")
				}
				prevH.tags = append(prevH.tags, s.options.job)
				prevH.tags = append(prevH.tags, s.instance)
				for _, v := range l {
					switch v.Name {
					case labels.MetricName, labels.BucketLabel:
						// skip
					default:
						prevH.tags = append(prevH.tags, tl.DictionaryFieldStringBytes{Key: []byte(v.Name), Value: []byte(v.Value)})
					}
				}
				s.histograms[baseName] = prevH
			}
			// append series value
			if histograms == nil {
				histograms = make(map[string]*histogram)
			}
			currH := histograms[baseName]
			if currH == nil {
				currH = &histogram{
					series: make(map[uint64]*scrapeHistogramSeries),
				}
				histograms[baseName] = currH
			}
			series := currH.series[hashSum]
			if series == nil {
				series = &scrapeHistogramSeries{}
				currH.series[hashSum] = series
			}
			switch {
			case strings.HasSuffix(name, "_bucket"):
				series.bucket = append(series.bucket, v)
				// remember buckets if histogram does not yet exist
				if len(prevH.buckets) == 0 && len(currH.series) == 1 {
					for _, v := range l {
						if v.Name == labels.BucketLabel {
							currH.buckets = append(currH.buckets, v.Value)
							break
						}
					}
				}
			case strings.HasSuffix(name, "_sum"):
				series.sum = v
			case strings.HasSuffix(name, "_count"):
				series.count = v
			}
		}
	}
	for metricName, currH := range histograms {
		// calculate buckets diff
		for _, s := range currH.series {
			for i := len(s.bucket); i > 1; i-- {
				s.bucket[i-1] -= s.bucket[i-2]
			}
		}
		prevH := s.histograms[metricName]
		if len(prevH.buckets) == 0 {
			// build description
			var sb strings.Builder
			if len(prevH.descriptionS) != 0 {
				sb.WriteString(prevH.descriptionS)
				sb.WriteByte('\n')
				sb.WriteByte('\n')
			}
			sb.WriteString(format.HistogramBucketsStartMark)
			sb.WriteString(currH.buckets[0])
			for i := 1; i < len(currH.buckets); i++ {
				sb.WriteByte(format.HistogramBucketsDelimC)
				sb.WriteString(currH.buckets[i])
			}
			sb.WriteByte(format.HistogramBucketsEndMarkC)
			prevH.descriptionB = sb.String()
			// encode bucket tag values
			prevH.buckets = make([][]byte, len(currH.buckets))
			for i := 0; i < len(currH.buckets); i++ {
				var bucket float64
				if bucket, err = strconv.ParseFloat(currH.buckets[i], 32); err == nil {
					prevH.buckets[i] = []byte(strconv.FormatInt(int64(statshouse.LexEncode(float32(bucket))), 10))
				}
			}
		}
		for hashSum, curr := range currH.series {
			if prev, ok := prevH.series[hashSum]; ok {
				// "_bucket" metric
				for i := 0; i < len(prev.bucket) && i < len(curr.bucket) && i < len(prevH.buckets); i++ {
					count := curr.bucket[i] - prev.bucket[i]
					if count > 0 {
						bytes := tlstatshouse.MetricBytes{
							Name: prevH.nameB,
							Tags: make([]tl.DictionaryFieldStringBytes, 0, len(prevH.tags)+1),
						}
						bytes.Tags = append(bytes.Tags, prevH.tags...)
						bytes.Tags = append(bytes.Tags, tl.DictionaryFieldStringBytes{Key: bucketTagName, Value: prevH.buckets[i]})
						bytes.SetCounter(count)
						s.handler.HandleMetrics(data_model.HandlerArgs{
							MetricBytes:    &bytes,
							Description:    prevH.descriptionB,
							ScrapeInterval: int(opt.interval.Seconds()),
						})
					}
				}
				// "_sum" metric
				if count := curr.count - prev.count; count > 0 {
					bytes := tlstatshouse.MetricBytes{
						Name: prevH.nameS,
						Tags: make([]tl.DictionaryFieldStringBytes, 0, len(prevH.tags)),
					}
					bytes.Tags = append(bytes.Tags, prevH.tags...)
					bytes.SetCounter(count)
					bytes.SetValue([]float64{curr.sum - prev.sum})
					s.handler.HandleMetrics(data_model.HandlerArgs{
						MetricBytes:    &bytes,
						Description:    prevH.descriptionS,
						ScrapeInterval: int(opt.interval.Seconds()),
					})
				}
			}
			prevH.series[hashSum] = curr
		}
	}
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
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	_, err = io.Copy(buf, resp.Body)
	if err != nil {
		return nil, "", err
	}
	return buf.Bytes(), resp.Header.Get("Content-Type"), nil
}

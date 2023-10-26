package stats

import (
	"context"
	"fmt"
	"log"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/receiver"
)

type Collector interface {
	Name() string
	WriteMetrics(nowUnix int64) error
	PushDuration(now int64, d time.Duration)
	Skip() bool
}

type CollectorManagerOptions struct {
	ScrapeInterval time.Duration
	HostName       string
}

type CollectorManager struct {
	opt        CollectorManagerOptions
	ctx        context.Context
	cancel     func()
	collectors []Collector
	logErr     *log.Logger
}

type scrapeResult struct {
	isSuccess bool
}

const procPath = "/proc"
const sysPath = "/sys"

func NewCollectorManager(opt CollectorManagerOptions, h receiver.Handler, logErr *log.Logger) (*CollectorManager, error) {
	newWriter := func() MetricWriter {
		if h == nil {
			return &MetricWriterRemoteImpl{HostName: opt.HostName}
		}
		return &MetricWriterSHImpl{
			HostName: []byte(opt.HostName),
			handler:  h,
			metric:   &tlstatshouse.MetricBytes{},
		}
	}
	cpuStats, err := NewCpuStats(newWriter())
	if err != nil {
		return nil, err
	}
	diskStats, err := NewDiskStats(newWriter(), logErr)
	if err != nil {
		return nil, err
	}
	memStats, err := NewMemoryStats(newWriter())
	if err != nil {
		return nil, err
	}
	netStats, err := NewNetStats(newWriter())
	if err != nil {
		return nil, err
	}
	psiStats, err := NewPSI(newWriter())
	if err != nil {
		return nil, err
	}
	sockStats, err := NewSocksStats(newWriter())
	if err != nil {
		return nil, err
	}
	protocolsStats, err := NewProtocolsStats(newWriter())
	if err != nil {
		return nil, err
	}
	allCollectors := []Collector{cpuStats, diskStats, memStats, netStats, psiStats, sockStats, protocolsStats} // TODO add modules
	var collectors []Collector
	for _, collector := range allCollectors {
		if !collector.Skip() {
			collectors = append(collectors, collector)
		} else {
			logErr.Printf("skip: %s collector", collector.Name())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &CollectorManager{
		opt:        opt,
		ctx:        ctx,
		cancel:     cancel,
		collectors: collectors,
		logErr:     logErr,
	}, nil
}

func (m *CollectorManager) RunCollector() error {
	errGroup := errgroup.Group{}
	for _, c := range m.collectors {
		collector := c
		errGroup.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					m.logErr.Println(r)
					err = fmt.Errorf("panic during to write system metrics: %s", r)
				}
			}()
			now := time.Now()
			for {
				startTime := time.Now()
				err := collector.WriteMetrics(now.Unix())
				if err != nil {
					m.logErr.Printf("failed to write metrics: %v (collector: %s)", err, c.Name())
				} else {
					d := time.Since(startTime)
					collector.PushDuration(now.Unix(), d)
				}
				select {
				case now = <-time.After(tillNextHalfPeriod(now)):
				case <-m.ctx.Done():
					return nil
				}
			}
		})
	}
	return errGroup.Wait()
}

func tillNextHalfPeriod(now time.Time) time.Duration {
	return now.Truncate(time.Second).Add(time.Second * 3 / 2).Sub(now)
}

func (m *CollectorManager) StopCollector() {
	m.cancel()
}

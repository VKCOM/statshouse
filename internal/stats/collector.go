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
	collectors := []Collector{cpuStats, diskStats, memStats, netStats, psiStats}
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
					err = fmt.Errorf("panic during to write system metrics: %s", r)
				}
			}()
			for {
				now := time.Now()
				err := collector.WriteMetrics(now.Unix())
				if err != nil {
					m.logErr.Printf("failed to write metrics: %v (collector: %s)", err, c.Name())
				}
				d := time.Since(now)
				collector.PushDuration(now.Unix(), d)
				select {
				case <-time.After(tillNextHalfPeriod(time.Now())):
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

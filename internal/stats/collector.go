package stats

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"github.com/VKCOM/statshouse/internal/env"
	"golang.org/x/sync/errgroup"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/receiver"
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

var errStopCollector = fmt.Errorf("stop collector")

const procPath = "/proc"
const sysPath = "/sys"

func NewCollectorManager(opt CollectorManagerOptions, h receiver.Handler, envLoader *env.Loader, logErr *log.Logger) (*CollectorManager, error) {
	newWriter := func() MetricWriter {
		if h == nil {
			return &MetricWriterRemoteImpl{HostName: opt.HostName, envLoader: envLoader}
		}
		return &MetricWriterSHImpl{
			HostName:  []byte(opt.HostName),
			handler:   h,
			metric:    &tlstatshouse.MetricBytes{},
			envLoader: envLoader,
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
	vmStatsCollector, err := NewVMStats(newWriter())
	if err != nil {
		return nil, err
	}
	klogStats, err := NewDMesgStats(newWriter())
	if err != nil {
		return nil, err
	}
	gcStats, err := NewGoStats(newWriter())
	if err != nil {
		return nil, err
	}
	netClassStats, err := NewNetClassStats(newWriter())
	if err != nil {
		return nil, err
	}
	allCollectors := []Collector{cpuStats, diskStats, memStats, netStats, psiStats, sockStats, protocolsStats, vmStatsCollector, klogStats, gcStats, netClassStats} // TODO add modules
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
		name := c.Name()
		errGroup.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					m.logErr.Println("collector", name, "panic:", r)
					_, _ = m.logErr.Writer().Write(debug.Stack())
				}
			}()
			m.logErr.Printf("start %s collector", collector.Name())
			for {
				now := time.Now()
				err := collector.WriteMetrics(now.Unix())

				if errors.Is(err, errStopCollector) {
					return nil
				}
				if err != nil {
					m.logErr.Printf("failed to write metrics: %v (collector: %s)", err, c.Name())
				} else {
					d := time.Since(now)
					collector.PushDuration(now.Unix(), d)
				}
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

package stats

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/receiver"
	"go.uber.org/multierr"
)

type Collector interface {
	Name() string
	PushMetrics() error
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
}

type scrapeResult struct {
	isSuccess bool
}

const procPath = "/proc"
const sysPath = "/sys"

func NewCollectorManager(opt CollectorManagerOptions, h receiver.Handler) (*CollectorManager, error) {
	newPusher := func() Pusher {
		if h == nil {
			return &PusherRemoteImpl{HostName: opt.HostName}
		}
		return &PusherSHImpl{
			HostName: []byte(opt.HostName),
			handler:  h,
			metric:   &tlstatshouse.MetricBytes{},
		}
	}
	cpuStats, err := NewCpuStats(newPusher())
	if err != nil {
		return nil, err
	}
	diskStats, err := NewDiskStats(newPusher())
	if err != nil {
		return nil, err
	}
	memStats, err := NewMemoryStats(newPusher())
	if err != nil {
		return nil, err
	}
	netStats, err := NewNetStats(newPusher())
	if err != nil {
		return nil, err
	}
	psiStats, err := NewPSI(newPusher())
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
	}, nil
}

func (m *CollectorManager) RunCollector() error {
	wg := sync.WaitGroup{}
	mx := sync.Mutex{}
	var err error
	for _, c := range m.collectors {
		wg.Add(1)
		go func(c Collector) {
			defer func() {
				if r := recover(); r != nil {
					mx.Lock()
					err = multierr.Append(err, fmt.Errorf("panic during to push system metrics: %s", r))
					mx.Unlock()
				}
				wg.Done()
			}()
			for {
				err := c.PushMetrics()
				if err != nil {
					log.Printf("failed to push metrics: %v (collector: %s)", err, c.Name())
				}
				// todo round interval to begin of second
				select {
				case <-time.After(m.opt.ScrapeInterval):
				case <-m.ctx.Done():
					return
				}
			}
		}(c)
	}
	wg.Wait()
	return err
}

func (m *CollectorManager) StopCollector() {
	m.cancel()
}

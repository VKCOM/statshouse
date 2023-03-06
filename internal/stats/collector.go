package stats

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"go.uber.org/multierr"
)

type Collector interface {
	Name() string
	PushMetrics() error
}

type CollectorManagerOptions struct {
	ScrapeInterval time.Duration
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

func NewCollectorManager(opt CollectorManagerOptions) (*CollectorManager, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	pusher := &PusherRemoteImpl{HostName: hostname}
	cpuStats, err := NewCpuStats(pusher)
	if err != nil {
		return nil, err
	}
	diskStats, err := NewDiskStats(pusher)
	if err != nil {
		return nil, err
	}
	memStats, err := NewMemoryStats(pusher)
	if err != nil {
		return nil, err
	}
	netStats, err := NewNetStats(pusher)
	if err != nil {
		return nil, err
	}
	psiStats, err := NewPSI(pusher)
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

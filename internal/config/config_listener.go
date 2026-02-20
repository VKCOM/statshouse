package config

import (
	"flag"
	"log"
	"strings"
	"sync"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
)

type Config interface {
	Bind(f *flag.FlagSet, default_ Config)
	ValidateConfig() error
	Copy() Config
}

type ConfigListener struct {
	mx           sync.RWMutex
	configMetric string
	initial      Config
	changeCB     []func(config Config)
}

func NewConfigListener(configMetric string, config Config) *ConfigListener {
	return &ConfigListener{
		configMetric: configMetric,
		initial:      config.Copy(), // in case user overwrites his config in callback
	}
}

func (l *ConfigListener) ValidateConfig(cfg string) error {
	_, err := l.parseConfig(cfg)
	return err
}

func (l *ConfigListener) parseConfig(cfg string) (Config, error) {
	l.mx.Lock()
	defer l.mx.Unlock()
	var f flag.FlagSet
	f.Usage = func() {} // don't print usage on unknown flags
	f.Init("", flag.ContinueOnError)
	c := l.initial.Copy()
	c.Bind(&f, c)
	s := strings.Split(cfg, "\n")
	for i := 0; i < len(s); i++ {
		t := strings.TrimSpace(s[i])
		if len(t) == 0 || strings.HasPrefix(t, "#") {
			continue
		}
		_ = f.Parse([]string{t})
	}
	err := c.ValidateConfig()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (l *ConfigListener) AddChangeCB(f func(config Config)) {
	l.mx.Lock()
	defer l.mx.Unlock()
	l.changeCB = append(l.changeCB, f)
}

func (l *ConfigListener) ApplyEventCB(newEntries []tlmetadata.Event) {
	for _, e := range newEntries {
		if e.EventType == format.MetricEvent && e.Name == l.configMetric {
			metric := format.MetricMetaValue{}
			err := metric.UnmarshalBinary([]byte(e.Data))
			if err != nil {
				return
			}
			cfg, err := l.parseConfig(metric.Description)
			if err != nil {
				log.Println("failed to parse remote config:", err.Error())
			}
			// do not call callback under mutex
			for _, f := range l.changeCB {
				f(cfg)
			}
			return
		}
	}
}

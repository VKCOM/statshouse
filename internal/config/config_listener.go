package config

import (
	"log"
	"strings"
	"sync"

	"github.com/spf13/pflag"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/format"
)

type Config interface {
	Bind(_ *pflag.FlagSet, default_ Config)
	ValidateConfig() error
	Copy() Config
}

type ConfigListener struct {
	mx           sync.RWMutex
	configMetric string
	config       Config
	changeCB     []func(config Config)
}

func NewConfigListener[a Config](configMetric string, config a) *ConfigListener {
	return &ConfigListener{
		configMetric: configMetric,
		config:       config,
	}
}

func (l *ConfigListener) parseConfig(cfg string) error {
	l.mx.Lock()
	defer l.mx.Unlock()
	var f pflag.FlagSet
	f.Init("", pflag.ContinueOnError)
	c := l.config.Copy()
	c.Bind(&f, l.config)
	s := strings.Split(cfg, "\n")
	for i := 0; i < len(s); {
		t := strings.TrimSpace(s[i])
		if len(t) == 0 || strings.HasPrefix(t, "#") {
			s = append(s[0:i], s[i+1:]...)
		} else {
			s[i] = t
			i++
		}
	}
	err := f.Parse(s)
	if err != nil {
		return err
	}
	err = c.ValidateConfig()
	if err != nil {
		return err
	}
	l.config = c
	for _, f := range l.changeCB {
		f(c)

	}
	return nil
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
			err = l.parseConfig(metric.Description)
			if err != nil {
				log.Println("failed to parse remote config:", err.Error())
			}
			return
		}
	}
}

package config

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/format"
)

type config struct {
	S string
}

func (c *config) Bind(f *flag.FlagSet, default_ Config) {
	def := default_.(*config)
	f.StringVar(&c.S, "s", def.S, "")
}

func (c *config) ValidateConfig() error {
	return nil
}

func (c *config) Copy() Config {
	cp := *c
	return &cp
}

const remoteConfig = "remote_config"

func TestConfigListener_ApplyEventCB(t *testing.T) {
	cfg := ConfigListener{
		configMetric: remoteConfig,
		config:       &config{"default"},
	}
	t.Run("check default value", func(t *testing.T) {
		cfg.ApplyEventCB([]tlmetadata.Event{{
			Name:      remoteConfig,
			Data:      "",
			EventType: format.MetricEvent,
		}})
		require.Equal(t, config{"default"}, *cfg.config.(*config))
	})
	t.Run("check non default value", func(t *testing.T) {
		m := format.MetricMetaValue{
			Name:        remoteConfig,
			Description: "--s=test1",
		}
		data, err := m.MarshalBinary()
		require.NoError(t, err)
		cfg.ApplyEventCB([]tlmetadata.Event{{
			Name:      remoteConfig,
			Data:      string(data),
			EventType: format.MetricEvent,
		}})
		require.Equal(t, config{"test1"}, *cfg.config.(*config))
	})

}

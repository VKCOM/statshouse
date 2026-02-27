package config

import (
	"flag"
	"testing"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/stretchr/testify/require"
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
	lastCfg := &config{"default"}
	cfg := NewConfigListener(remoteConfig, lastCfg)
	cfg.AddChangeCB(func(c Config) {
		lastCfg = c.(*config)
	})
	applyEvent := func(description string) {
		m := format.MetricMetaValue{
			Name:        remoteConfig,
			Description: description,
		}
		data, err := m.MarshalBinary()
		require.NoError(t, err)
		cfg.ApplyEventCB([]tlmetadata.Event{{
			Name:      remoteConfig,
			Data:      string(data),
			EventType: format.MetricEvent,
		}})
	}

	t.Run("check default value", func(t *testing.T) {
		applyEvent("")
		require.Equal(t, config{"default"}, *lastCfg)
	})
	t.Run("check non default value", func(t *testing.T) {
		applyEvent("--s=test1")
		require.Equal(t, config{"test1"}, *lastCfg)
	})
	t.Run("check empty value", func(t *testing.T) {
		applyEvent("--s=")
		require.Equal(t, config{""}, *lastCfg)
	})
	t.Run("check back to defult value", func(t *testing.T) {
		applyEvent("")
		require.Equal(t, config{"default"}, *lastCfg)
	})
}

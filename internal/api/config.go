package api

import (
	"github.com/spf13/pflag"
	"github.com/vkcom/statshouse/internal/config"
)

type Config struct {
	ApproxCacheMaxSize int
}

func (argv *Config) ValidateConfig() error {
	return nil
}

func (argv *Config) Copy() config.Config {
	cp := *argv
	return &cp
}

func (argv *Config) Bind(pflag *pflag.FlagSet, defaultI config.Config) {
	default_ := defaultI.(*Config)
	pflag.IntVar(&argv.ApproxCacheMaxSize, "approx-cache-max-size", default_.ApproxCacheMaxSize, "approximate max amount of rows to cache for each table+resolution")
}

func DefaultConfig() *Config {
	return &Config{
		ApproxCacheMaxSize: 1_000_000,
	}
}

package api

import (
	"fmt"
	"time"

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

type HandlerOptions struct {
	insecureMode            bool
	LocalMode               bool
	querySequential         bool
	readOnly                bool
	verbose                 bool
	timezone                string
	protectedMetricPrefixes []string
	querySelectTimeout      time.Duration
	weekStartAt             int
	location                *time.Location
	utcOffset               int64
}

func (argv *HandlerOptions) Bind(pflag *pflag.FlagSet) {
	pflag.BoolVar(&argv.insecureMode, "insecure-mode", false, "set insecure-mode if you don't need any access verification")
	pflag.BoolVar(&argv.LocalMode, "local-mode", false, "set local-mode if you need to have default access without access token")
	pflag.BoolVar(&argv.querySequential, "query-sequential", false, "disables query parallel execution")
	pflag.BoolVar(&argv.readOnly, "readonly", false, "read only mode")
	pflag.BoolVar(&argv.verbose, "verbose", false, "verbose logging")
	pflag.DurationVar(&argv.querySelectTimeout, "query-select-timeout", QuerySelectTimeoutDefault, "query select timeout")
	pflag.StringSliceVar(&argv.protectedMetricPrefixes, "protected-metric-prefixes", nil, "comma-separated list of metric prefixes that require access bits set")
	pflag.StringVar(&argv.timezone, "timezone", "Europe/Moscow", "location of the desired timezone")
	pflag.IntVar(&argv.weekStartAt, "week-start", int(time.Monday), "week day of beginning of the week (from sunday=0 to saturday=6)")
}

func (argv *HandlerOptions) LoadLocation() error {
	if argv.weekStartAt < int(time.Sunday) || argv.weekStartAt > int(time.Saturday) {
		return fmt.Errorf("invalid --week-start value, only 0-6 allowed %q given", argv.weekStartAt)
	}
	var err error
	argv.location, err = time.LoadLocation(argv.timezone)
	if err != nil {
		return fmt.Errorf("failed to load timezone %q: %w", argv.timezone, err)
	}
	argv.utcOffset = calcUTCOffset(argv.location, time.Weekday(argv.weekStartAt)) // demands restart after summer/winter time switching
	return nil
}

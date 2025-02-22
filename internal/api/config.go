package api

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/chutil"
	"github.com/vkcom/statshouse/internal/config"
)

type Config struct {
	ApproxCacheMaxSize     int
	Version3Start          int64
	Version3Prob           float64
	Version3StrcmpOff      bool
	UserLimitsStr          string
	UserLimits             []chutil.ConnLimits
	CacheVersion           int
	MaxCacheSize           int   // bytes
	MaxCacheAge            int   // seconds
	CacheStaleAcceptPeriod int64 // seconds
	CacheTrimBackoffPeriod int64 // seconds
	BotUserNames           []string
}

func (argv *Config) ValidateConfig() error {
	if argv.UserLimitsStr != "" {
		var userLimits []chutil.ConnLimits
		err := json.Unmarshal([]byte(argv.UserLimitsStr), &userLimits)
		if err != nil {
			return fmt.Errorf("failed to parse user limit config: %w err", err)
		}
		argv.UserLimits = userLimits
	}

	return nil
}

func (argv *Config) Copy() config.Config {
	cp := *argv
	cp.UserLimits = make([]chutil.ConnLimits, len(cp.UserLimits))
	copy(cp.UserLimits, argv.UserLimits)
	return &cp
}

func (argv *Config) Bind(f *flag.FlagSet, defaultI config.Config) {
	default_ := defaultI.(*Config)
	f.IntVar(&argv.ApproxCacheMaxSize, "approx-cache-max-size", default_.ApproxCacheMaxSize, "approximate max amount of rows to cache for each table+resolution")
	f.Int64Var(&argv.Version3Start, "version3-start", 0, "timestamp of schema version 3 start, zero means not set")
	f.Float64Var(&argv.Version3Prob, "version3-prob", 0, "the probability of choosing version 3 when version was set to 2 or empty")
	f.BoolVar(&argv.Version3StrcmpOff, "version3-strcmp-off", false, "disable string comparision for schema version 3")
	f.IntVar(&argv.CacheVersion, "cache-version", 1, "cache version")
	f.IntVar(&argv.MaxCacheSize, "max-cache-size", 4*1024*1024*1024, "maximum cache size in bytes")
	f.IntVar(&argv.MaxCacheAge, "max-cache-age", 120, "maximum cache age in seconds")
	f.Int64Var(&argv.CacheStaleAcceptPeriod, "cache-stale-accept-period", 5, "cache stale accept period in seconds")
	f.Int64Var(&argv.CacheTrimBackoffPeriod, "cache-trim-backoff-period", 1, "cache trim backoff period in seconds")
	config.StringSliceVar(f, &argv.BotUserNames, "bot-user-names", "", "known bot user list")
	f.StringVar(&argv.UserLimitsStr, "user-limits", "", "array of ConnLimits encoded to json")
}

func DefaultConfig() *Config {
	return &Config{
		ApproxCacheMaxSize: 1_000_000,
	}
}

type HandlerOptions struct {
	insecureMode             bool
	LocalMode                bool
	querySequential          bool
	readOnly                 bool
	verbose                  bool
	timezone                 string
	protectedMetricPrefixesS string
	protectedMetricPrefixes  []string
	querySelectTimeout       time.Duration
	weekStartAt              int
	location                 *time.Location
	utcOffset                int64
}

func (argv *HandlerOptions) Bind(f *flag.FlagSet) {
	f.BoolVar(&argv.insecureMode, "insecure-mode", false, "set insecure-mode if you don't need any access verification")
	f.BoolVar(&argv.LocalMode, "local-mode", false, "set local-mode if you need to have default access without access token")
	f.BoolVar(&argv.querySequential, "query-sequential", false, "disables query parallel execution")
	f.BoolVar(&argv.readOnly, "readonly", false, "read only mode")
	f.BoolVar(&argv.verbose, "verbose", false, "verbose logging")
	f.DurationVar(&argv.querySelectTimeout, "query-select-timeout", QuerySelectTimeoutDefault, "query select timeout")
	f.StringVar(&argv.protectedMetricPrefixesS, "protected-metric-prefixes", "", "comma-separated list of metric prefixes that require access bits set")
	f.StringVar(&argv.timezone, "timezone", "Europe/Moscow", "location of the desired timezone")
	f.IntVar(&argv.weekStartAt, "week-start", int(time.Monday), "week day of beginning of the week (from sunday=0 to saturday=6)")
}

func (argv *HandlerOptions) Parse() error {
	argv.protectedMetricPrefixes = strings.Split(argv.protectedMetricPrefixesS, ",")

	// Parse location
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

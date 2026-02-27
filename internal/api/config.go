package api

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/config"
	"github.com/VKCOM/statshouse/internal/format"
)

type Config struct {
	ApproxCacheMaxSize           int
	Version6Start                int64
	UserLimitsStr                string
	UserLimits                   []chutil.ConnLimits
	MaxCacheSize                 int // hard limit, in bytes
	MaxCacheSizeSoft             int // soft limit, in bytes
	MaxCacheAge                  int // seconds
	CacheChunkSize               int
	CacheBlacklist               []string
	CacheWhitelist               []string
	DisableCHAddr                []string
	CHSelectSettingsStr          string
	CHSelectSettings             map[string]string
	BlockedMetricPrefixesS       string
	BlockedMetricPrefixes        []string
	BlockedUsersS                string
	BlockedUsers                 []string
	AvailableShardsStr           string
	AvailableShards              []uint32
	CHMaxShardConnsRatio         int
	ReplicaThrottleCfgStr        string
	ReplicaThrottleCfg           *chutil.ReplicaThrottleConfig
	HardwareMetricResolution     int
	HardwareSlowMetricResolution int
	Announcement                 string // if !empty, show to user in UI
	chutil.RateLimitConfig
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

	if argv.CHSelectSettingsStr != "" {
		settings := make(map[string]string)
		pairs := strings.Split(argv.CHSelectSettingsStr, ",")
		for _, pair := range pairs {
			parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(parts) != 2 {
				log.Printf("invalid CH select setting format '%s', expected 'key=value', skipping", pair)
				continue
			}
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			if key == "" || value == "" {
				log.Printf("invalid CH select setting '%s', key and value cannot be empty, skipping", pair)
				continue
			}
			settings[key] = value
		}
		argv.CHSelectSettings = settings
	}
	argv.BlockedMetricPrefixes = nil
	if argv.BlockedMetricPrefixesS != "" {
		argv.BlockedMetricPrefixes = strings.Split(argv.BlockedMetricPrefixesS, ",")
	}
	argv.BlockedUsers = nil
	if argv.BlockedUsersS != "" {
		argv.BlockedUsers = strings.Split(argv.BlockedUsersS, ",")
	}
	argv.AvailableShards = nil
	if argv.AvailableShardsStr != "" {
		shards, err := parseShardKeys(argv.AvailableShardsStr)
		if err != nil {
			return fmt.Errorf("failed to parse available shards: %w", err)
		}
		argv.AvailableShards = shards
	}
	argv.ReplicaThrottleCfg = nil
	if argv.ReplicaThrottleCfgStr != "" {
		var throttleConfig chutil.ReplicaThrottleConfig
		err := json.Unmarshal([]byte(argv.ReplicaThrottleCfgStr), &throttleConfig)
		if err != nil {
			return fmt.Errorf("failed to parse replica throttle config: %w", err)
		}
		argv.ReplicaThrottleCfg = &throttleConfig
	}
	if format.AllowedResolution(argv.HardwareMetricResolution) != argv.HardwareMetricResolution {
		return fmt.Errorf("--hardware-metric-resolution (%d) but must be 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30 or 60", argv.HardwareMetricResolution)
	}
	if format.AllowedResolution(argv.HardwareSlowMetricResolution) != argv.HardwareSlowMetricResolution {
		return fmt.Errorf("--hardware-slow-metric-resolution (%d) but must be 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30 or 60", argv.HardwareSlowMetricResolution)
	}
	return nil
}

func (argv *Config) Copy() config.Config {
	cp := *argv
	cp.UserLimits = make([]chutil.ConnLimits, len(cp.UserLimits))
	copy(cp.UserLimits, argv.UserLimits)
	cp.CHSelectSettings = maps.Clone(argv.CHSelectSettings)
	return &cp
}

func (argv *Config) Bind(f *flag.FlagSet, defaultI config.Config) {
	default_ := defaultI.(*Config)
	f.IntVar(&argv.ApproxCacheMaxSize, "approx-cache-max-size", default_.ApproxCacheMaxSize, "approximate max amount of rows to cache for each table+resolution")
	f.Int64Var(&argv.Version6Start, "version6-start", default_.Version6Start, "timestamp of schema version 6 start, zero means v6 feature is disabled")
	f.IntVar(&argv.MaxCacheSize, "max-cache-size", default_.MaxCacheSize, "cache hard memory limit (in bytes)")
	f.IntVar(&argv.MaxCacheSizeSoft, "max-cache-size-soft", default_.MaxCacheSizeSoft, "cache soft memory limit (in bytes)")
	f.IntVar(&argv.MaxCacheAge, "max-cache-age", default_.MaxCacheAge, "maximum cache age in seconds")
	f.IntVar(&argv.CacheChunkSize, "cache-chunk-size", default_.CacheChunkSize, "cache chunk size")
	// TODO - check how those StringSliceVar are merged with previous config
	config.StringSliceVar(f, &argv.CacheBlacklist, "cache-blacklist", "", "user(s) with cache disabled")
	config.StringSliceVar(f, &argv.CacheWhitelist, "cache-whitelist", "", "user(s) with cache enabled")
	f.StringVar(&argv.UserLimitsStr, "user-limits", default_.UserLimitsStr, "array of ConnLimits encoded to json")
	config.StringSliceVar(f, &argv.DisableCHAddr, "disable-clickhouse-addrs", "", "disable clickhouse addresses")
	f.StringVar(&argv.CHSelectSettingsStr, "ch-select-settings", default_.CHSelectSettingsStr, "comma-separated ClickHouse SELECT settings (e.g., max_bytes_to_read=1000000000,max_execution_time=30)")
	f.StringVar(&argv.BlockedMetricPrefixesS, "blocked-metric-prefixes", default_.BlockedMetricPrefixesS, "comma-separated list of metric prefixes that are blocked")
	f.StringVar(&argv.BlockedUsersS, "blocked-users", default_.BlockedUsersS, "comma-separated list of users that are blocked")
	f.StringVar(&argv.AvailableShardsStr, "available-shards", default_.AvailableShardsStr, "comma-separated list of default shards for metrics when namespace doesn't specify shards")
	f.IntVar(&argv.CHMaxShardConnsRatio, "clickhouse-max-shard-conns-ratio", default_.CHMaxShardConnsRatio, "maximum number of ClickHouse connections per shard (%)")
	f.StringVar(&argv.ReplicaThrottleCfgStr, "replica-throttle-config", "", "JSON config for replica throttling testing (feature flag)")
	f.IntVar(&argv.HardwareMetricResolution, "hardware-metric-resolution", default_.HardwareMetricResolution, "Statshouse hardware metric resolution")
	f.IntVar(&argv.HardwareSlowMetricResolution, "hardware-slow-metric-resolution", default_.HardwareSlowMetricResolution, "Statshouse slow hardware metric resolution")

	f.BoolVar(&argv.RateLimitDisable, "rate-limit-disable", default_.RateLimitDisable, "disable rate limiting")
	f.DurationVar(&argv.WindowDuration, "rate-limit-window-duration", default_.WindowDuration, "time window for analyzing ClickHouse requests")
	f.Uint64Var(&argv.MaxErrorRate, "rate-limit-max-error-rate", default_.MaxErrorRate, "error rate threshold (%) to trigger sleep stage")
	f.Uint64Var(&argv.MaxInflightWeight, "rate-limit-max-inflight-weight", default_.MaxInflightWeight, "maximum weight per inflight request (%)")
	f.Uint64Var(&argv.RecoverWeightStep, "rate-limit-recover-weight-step", default_.RecoverWeightStep, "recover weight step (%) per recovery cycle")
	f.DurationVar(&argv.RecoverGapDuration, "rate-limit-recover-gap-duration", default_.RecoverGapDuration, "recover time gap per recovery cycle")
	f.Float64Var(&argv.SleepMultiplier, "rate-limit-sleep-multiplier", default_.SleepMultiplier, "sleep duration multiplier after failed check")
	f.DurationVar(&argv.MaxSleepDuration, "rate-limit-max-sleep-duration", default_.MaxSleepDuration, "maximum sleep duration limit")
	f.DurationVar(&argv.MinSleepDuration, "rate-limit-min-sleep-duration", default_.MinSleepDuration, "minimum sleep duration when entering sleep")
	f.Uint64Var(&argv.CheckCount, "rate-limit-check-count", default_.CheckCount, "successful checks needed to exit check stage")
	f.StringVar(&argv.Announcement, "announcement", default_.Announcement, "announcement text to show user in UI")
	f.DurationVar(&argv.RecalcInterval, "recalc-interval-sleep-duration", default_.RecalcInterval, "rate limit state recalculation interval")
}

func DefaultConfig() *Config {
	return &Config{
		ApproxCacheMaxSize:   1_000_000,
		MaxCacheSize:         5 * 1024 * 1024 * 1024,
		MaxCacheSizeSoft:     0,
		MaxCacheAge:          60,
		CacheChunkSize:       5,
		AvailableShardsStr:   "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16",
		CHMaxShardConnsRatio: 20,
		RateLimitConfig: chutil.RateLimitConfig{
			WindowDuration:     2 * time.Minute,
			MaxErrorRate:       20,
			MaxInflightWeight:  10,
			RecoverWeightStep:  1,
			RecoverGapDuration: 6 * time.Second,
			SleepMultiplier:    1.5,
			MaxSleepDuration:   2 * time.Minute,
			MinSleepDuration:   30 * time.Second,
			CheckCount:         9,
			RecalcInterval:     1 * time.Second,
		},
		HardwareMetricResolution:     5,
		HardwareSlowMetricResolution: 15,
	}
}

func (cfg *Config) BuildSelectSettings() string {
	if len(cfg.CHSelectSettings) == 0 {
		return " SETTINGS optimize_aggregation_in_order=1"
	}

	settingPairs := make([]string, 0, len(cfg.CHSelectSettings)+1)
	settingPairs = append(settingPairs, "optimize_aggregation_in_order=1")

	for k, v := range cfg.CHSelectSettings {
		if k == "optimize_aggregation_in_order" {
			settingPairs[0] = fmt.Sprintf("optimize_aggregation_in_order=%s", v)
		} else {
			settingPairs = append(settingPairs, fmt.Sprintf("%s=%s", k, v))
		}
	}

	return fmt.Sprintf(" SETTINGS %s", strings.Join(settingPairs, ","))
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
	QuerySelectTimeout       time.Duration
	weekStartAt              int
	location                 *time.Location
	utcOffset                int64
	proxyWhiteList           []string
}

func (argv *HandlerOptions) Bind(f *flag.FlagSet) {
	f.BoolVar(&argv.insecureMode, "insecure-mode", false, "set insecure-mode if you don't need any access verification")
	f.BoolVar(&argv.LocalMode, "local-mode", false, "set local-mode if you need to have default access without access token")
	f.BoolVar(&argv.querySequential, "query-sequential", false, "disables query parallel execution")
	f.BoolVar(&argv.readOnly, "readonly", false, "read only mode")
	f.BoolVar(&argv.verbose, "verbose", false, "verbose logging")
	f.DurationVar(&argv.QuerySelectTimeout, "query-select-timeout", QuerySelectTimeoutDefault, "query select timeout")
	f.StringVar(&argv.protectedMetricPrefixesS, "protected-metric-prefixes", "", "comma-separated list of metric prefixes that require access bits set")
	f.StringVar(&argv.timezone, "timezone", "Europe/Moscow", "location of the desired timezone")
	f.IntVar(&argv.weekStartAt, "week-start", int(time.Monday), "week day of beginning of the week (from sunday=0 to saturday=6)")
	config.StringSliceVar(f, &argv.proxyWhiteList, "proxy-whitelist", "", "list of hosts to which API is allowed to proxy requests")
}

func (argv *HandlerOptions) Parse() error {
	if argv.protectedMetricPrefixesS == "" {
		argv.protectedMetricPrefixes = nil
	} else {
		argv.protectedMetricPrefixes = strings.Split(argv.protectedMetricPrefixesS, ",")
	}

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

func parseShardKeys(shardsStr string) ([]uint32, error) {
	var shards []uint32
	parts := strings.Split(shardsStr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		shard, err := strconv.ParseUint(part, 10, 32)
		if err != nil || shard <= 0 {
			return nil, fmt.Errorf("invalid shard num %s", part)
		}
		shards = append(shards, uint32(shard))
	}
	return shards, nil
}

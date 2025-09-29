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
)

type Config struct {
	ApproxCacheMaxSize     int
	Version3Start          int64
	Version3Prob           float64
	Version3StrcmpOff      bool
	UserLimitsStr          string
	UserLimits             []chutil.ConnLimits
	MaxCacheSize           int // hard limit, in bytes
	MaxCacheSizeSoft       int // soft limit, in bytes
	MaxCacheAge            int // seconds
	CacheChunkSize         int
	CacheBlacklist         []string
	CacheWhitelist         []string
	DisableCHAddr          []string
	NewShardingStart       int64
	CHSelectSettingsStr    string
	CHSelectSettings       map[string]string
	BlockedMetricPrefixesS string
	BlockedMetricPrefixes  []string
	BlockedUsersS          string
	BlockedUsers           []string
	AvailableShardsStr     string
	AvailableShards        []uint32
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
		shards, err := parseShardNumbers(argv.AvailableShardsStr)
		if err != nil {
			return fmt.Errorf("failed to parse available shards: %w", err)
		}
		argv.AvailableShards = shards
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
	f.Int64Var(&argv.Version3Start, "version3-start", 0, "timestamp of schema version 3 start, zero means not set")
	f.Float64Var(&argv.Version3Prob, "version3-prob", 0, "the probability of choosing version 3 when version was set to 2 or empty")
	f.BoolVar(&argv.Version3StrcmpOff, "version3-strcmp-off", false, "disable string comparision for schema version 3")
	f.IntVar(&argv.MaxCacheSize, "max-cache-size", 5*1024*1024*1024, "cache hard memory limit (in bytes)")
	f.IntVar(&argv.MaxCacheSizeSoft, "max-cache-size-soft", 0, "cache soft memory limit (in bytes)")
	f.IntVar(&argv.MaxCacheAge, "max-cache-age", 60, "maximum cache age in seconds")
	f.IntVar(&argv.CacheChunkSize, "cache-chunk-size", 5, "cache chunk size")
	config.StringSliceVar(f, &argv.CacheBlacklist, "cache-blacklist", "", "user(s) with cache disabled")
	config.StringSliceVar(f, &argv.CacheWhitelist, "cache-whitelist", "", "user(s) with cache enabled")
	f.StringVar(&argv.UserLimitsStr, "user-limits", "", "array of ConnLimits encoded to json")
	config.StringSliceVar(f, &argv.DisableCHAddr, "disable-clickhouse-addrs", "", "disable clickhouse addresses")
	f.Int64Var(&argv.NewShardingStart, "new-sharding-start", 0, "timestamp of new sharding start, zero means not set")
	f.StringVar(&argv.CHSelectSettingsStr, "ch-select-settings", "", "comma-separated ClickHouse SELECT settings (e.g., max_bytes_to_read=1000000000,max_execution_time=30)")
	f.StringVar(&argv.BlockedMetricPrefixesS, "blocked-metric-prefixes", "", "comma-separated list of metric prefixes that are blocked")
	f.StringVar(&argv.BlockedUsersS, "blocked-users", "", "comma-separated list of users that are blocked")
	f.StringVar(&argv.AvailableShardsStr, "available-shards", default_.AvailableShardsStr, "comma-separated list of default shards for metrics when namespace doesn't specify shards")
}

func DefaultConfig() *Config {
	return &Config{
		ApproxCacheMaxSize: 1_000_000,
		AvailableShardsStr: "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16",
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
	querySelectTimeout       time.Duration
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
	f.DurationVar(&argv.querySelectTimeout, "query-select-timeout", QuerySelectTimeoutDefault, "query select timeout")
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

func parseShardNumbers(shardsStr string) ([]uint32, error) {
	var shards []uint32
	parts := strings.Split(shardsStr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		shard, err := strconv.Atoi(part)
		if err != nil || shard <= 0 {
			return nil, fmt.Errorf("invalid shard num %s", part)
		}
		shards = append(shards, uint32(shard))
	}
	return shards, nil
}

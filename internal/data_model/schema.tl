// Mega TODO - when all statshouses updated to commit contating this message, we can:
// 1. remove all functions without "2" suffix from this TL, statshouse.sourceBucket, statshouse.item, statshouse.ingestion_status, statshouse.string_top_element
// 2. remove code referring to them from aggregator (recursively)
// This commit is made on 2022-sep-25

// Protocol between statshouse and statshouse aggregator is private for now

---types---

statshouse.sample_factor metric:int value:float = statshouse.SampleFactor;

statshouse.ingestion_status2 env:int metric:int value:float = statshouse.IngestionStatus2;

statshouse.centroidFloat#73fd01e0 value:float count:float = statshouse.CentroidFloat;

statshouse.mapping str:string value:int = statshouse.Mapping;

statshouse.multi_value#0c803e06 {fields_mask:#}
    counter: fields_mask.0?double // no counter means counter equal to 0
    counter_eq_1: fields_mask.1?true // optimization for most common value

    value_set: fields_mask.2?true // to avoid min/max corruption when some clients treat the same row as counter/value

    value_min: fields_mask.3?double // simple value (all values identical)
    value_max: fields_mask.4?double // complex value (there is different values)
    value_sum: fields_mask.4?double // complex value (there is different values)
    value_sum_square: fields_mask.4?double // complex value (there is different values)

    uniques: fields_mask.5?string // serialized HLL

    centroids: fields_mask.6?(vector statshouse.centroidFloat)
    implicit_centroid: fields_mask.18?true // centroid should be restored from the single simple value

    // we need hosts to pass through agent sending on behalf of aggregator. Or from clients using _h tag
    max_host_tag: fields_mask.7?int
    // optimization for single values: if max_host_tag is set and *_host_tag below is not set, it is equal to max_host_tag
    min_host_tag: fields_mask.8?int
    max_counter_host_tag: fields_mask.9?int
    // field mask bits 10 - 12 are used in statshouse.multi_item
    max_host_stag: fields_mask.14?string
    min_host_stag: fields_mask.15?string
    max_counter_host_stag: fields_mask.16?string
    = statshouse.MultiValue fields_mask;

statshouse.top_element#9ffdea42 stag:string fields_mask:# tag:fields_mask.10?int value:(statshouse.multi_value fields_mask) = statshouse.TopElement;

statshouse.multi_item#0c803e07
    fields_mask: # // conveyor V2 agents send shard num in top 8 bits here
    metric: int
    keys: (vector int)
    skeys: fields_mask.12?(vector string)
    weightMultiplier: fields_mask.17?true // agents increased metric weight by num shards, expect the same from aggregators

    t: fields_mask.10?# // timestamp, should be rare

    tail: (statshouse.multi_value fields_mask) // no fields_mask, because anyway uses 0 bytes if counter with 0.0 or 1.0
    // won't be set in a v3 pipeline
    top: fields_mask.11?(vector (statshouse.top_element)) // most items are without string keys, so we save 4 bytes using fields_mask
    = statshouse.MultiItem;

statshouse.sourceBucket2#3af6e822
      metrics: (vector (statshouse.multi_item)) // must be sorted by metric (but for now order is not used)

      sample_factors: (vector statshouse.sample_factor) // We need as compact representation as possible.
      ingestion_status_ok: (vector statshouse.sample_factor) // legacy, never sent by 1.0 agents. TODO - rename to unused

      missed_seconds: #
      legacy_agent_env: int // used to fill key0 of many built-in stats. TODO - remove, er use bit in common header now

      // fields below ere added before 1.0, not sent by legacy agents
      ingestion_status_ok2: (vector statshouse.ingestion_status2) // We need as compact representation as possible.

  = statshouse.SourceBucket2;

statshouse.sourceBucket3#16c4dd7b
    fields_mask: #
    // TODO: check how it is used and maybe change to a new version
    metrics: (vector (statshouse.multi_item)) // must be sorted by metric (but for now order is not used)
    sample_factors: (vector statshouse.sample_factor) // We need as compact representation as possible.
    ingestion_status_ok2: (vector statshouse.ingestion_status2) // We need as compact representation as possible.
  = statshouse.SourceBucket3;

// TODO - remove ttl_nanosec
statshouse.getTagMappingResult#1a7d91fd value:int ttl_nanosec:long = statshouse.GetTagMappingResult;

statshouse.getTagMappingBootstrapResult#486a40de mappings:(vector statshouse.mapping) = statshouse.GetTagMappingBootstrapResult;
statshouse.putTagMappingBootstrapResult#486affde
    count_inserted: int
 = statshouse.PutTagMappingBootstrapResult;

// metrics are row in JSON format, version is opaque string interpreted by aggregator
statshouse.getMetricsResult#0c803d05 version: string metrics: (vector string) = statshouse.GetMetricsResult;

// Deprecated, not used by new conveyor agents
statshouse.getConfigResult#0c803d07 {fields_mask:#}
    addresses: (vector string)
    max_addresses_count: int // when reducing # of shards, will return full list of addresses, limit by this number. Important for proxy
    previous_addresses: int // currently unused, but can be helpful for proxy in the future
    ts: fields_mask.0?long // aggregator timestamp, for detecting clock discrepancy
= statshouse.GetConfigResult fields_mask;

statshouse.getConfigResult3#f13698cb
    addresses: (vector string) // 3 replicas for each shard
    shard_by_metric_count: # // should be <= len(addresses), so that we can add 17'th shard without breaking all metrics which are spread automatically between first 16.
    unused: 12*[int] // arbitrary space for extensions, so we can avoid using fields masks and complications with them
    unused_s: 4*[string] // arbitrary space for extensions, so we can avoid using fields masks and complications with them
= statshouse.GetConfigResult3;

// proxy uses and fills data in this header for all requests
statshouse.commonProxyHeader#6c803d07 {fields_mask:#}
    // header uses bits backward to reduce conflicts with request fields
    ingress_proxy:fields_mask.31?true     // simply set to 1 by ingress proxy for all passing requests
    agent_env_staging_0:fields_mask.30?true // set by test version of agents to help with debug
    agent_env_staging_1:fields_mask.29?true

    shard_replica:int // Aggregator will only accept correct shard/replica conbinations
    shard_replica_total:int
    agent_ip:4*[int] // filled by proxy. ipv4 is in ipv6-compatible format, source_ip[3] holds ipv4 address bits.
    host_name:string
    component_tag:int // was agent_env before release version. Checks for legacy in aggregator are no more needed after release
    build_arch:int // comparison of x64 and arm64 nodes will be interesting for some time
    owner:fields_mask.28?string
= statshouse.CommonProxyHeader fields_mask;

statshouse.promTarget#ac5296df {fields_mask_arg:#}
    fields_mask: #
    job_name: string
    url: string
    labels: (dictionary string)
    scrape_interval: long
    honor_timestamps: fields_mask.0?true
    honor_labels: fields_mask.1?true
    scrape_timeout: long
    body_size_limit: long
    label_limit: long
    label_name_length_limit: long
    label_value_length_limit: long
    // raw yaml from prom config
    http_client_config: string
    metric_relabel_configs: fields_mask_arg.1?string
= statshouse.PromTarget fields_mask_arg;

statshouse.getTargetsResult#51ac86df {fields_mask:#}
    targets: (vector (statshouse.promTarget fields_mask))
    hash: string
    gauge_metrics:fields_mask.0?(vector string)
= statshouse.GetTargetsResult fields_mask;

// saved between agent invocations. All values nanoseconds.
statshouse.shutdownInfo#4124cf9c
    startShutdownTime:long
    finishShutdownTime:long
    stopRecentSenders:long
    stopReceivers:long
    stopFlusher:long
    stopFlushing:long
    stopPreprocessor:long
    stopInserters:long // for aggregator
    stopRPCServer:long // for aggregator
    saveMappings:long
    saveJournal:long
    // we might want more timeouts later without changing format, so we reserve some space
    c:long d:long e:long f:long g:long h:long i:long j:long k:long l:long m:long n:long
= statshouse.ShutdownInfo;

statshouse.sendSourceBucket3Response#0e177acc
    field_mask: #
    discard: field_mask.0?true // agent should discard bucket, either they were inserted successfully or they can never be inserted
    warning: string // if !empty, aggregator wants this in client log
    mappings: (vector statshouse.mapping)
= statshouse.SendSourceBucket3Response;

---functions---

// Deprecated, not used by new conveyor agents
@readwrite statshouse.getConfig2#4285ff57
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask) // the only request not passed through proxy. But we make it with common header anyway.
    cluster:string // prevent wrong configuration
    ts: fields_mask.0?true
     = statshouse.GetConfigResult fields_mask;

@readwrite statshouse.getConfig3#7d7b4991
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask) // the only request not passed through proxy. But we make it with common header anyway.
    cluster:string // prevent wrong configuration
    previousConfig: fields_mask.0?statshouse.getConfigResult3 // if sent, will long poll on it
     = statshouse.GetConfigResult3;

// For ingress proxy, we pass shard_replica/shard_total immediately after fields mask
// TODO - remove shard_replica, shard_replica_total fields from the wrong place

// sendSourceBucket and sendKeepAlive must share common response, because aggregator hijacks them into the same slice
// we use string as a crude logging capability. Agents write to log what they receive from aggregators.
// type of compressed_data should be checked by first 4 bytes (tag)
@readwrite statshouse.sendSourceBucket2#44575940
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    owner:fields_mask.4?string
    time:#
    historic:fields_mask.0?true
    spare:fields_mask.1?true
    build_commit:string
    build_commit_date:int // deprecated, not sent by agents, not used by aggregators
    build_commit_ts:#
    queue_size_disk:int
    queue_size_memory:int
    queue_size_disk_sum:fields_mask.2?int   // for all shard-replicas. Helps detect situation where only subset of aggregators is accessible.
    queue_size_memory_sum:fields_mask.2?int
    queue_size_disk_unsent:fields_mask.3?int
    queue_size_disk_sum_unsent:fields_mask.3?int
    original_size:#
    compressed_data:string
    sharding:fields_mask.5?int // deprecated because sharding is now configured for each metric separately
    = String;

@readwrite statshouse.sendSourceBucket3#0d04aa3f
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    time:#
    historic:fields_mask.0?true
    spare:fields_mask.1?true
    // build stats
    build_commit:string
    build_commit_ts:#
    // data
    original_size:#
    compressed_data:string
    send_more_bytes:string // send dummy zeroes to test network, according to send-more-data agent config value
    = statshouse.SendSourceBucket3Response;

@readwrite statshouse.sendKeepAlive2#4285ff53
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
     = String;

@readwrite statshouse.sendKeepAlive3#4285ff54
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
     = statshouse.SendSourceBucket3Response;

// This function is long poll. Will receive answer only when version changes
@readwrite statshouse.getMetrics3#42855554
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    field_mask: #
    from: long // строго больше
    limit: long
    return_if_empty: field_mask.3?true
    new_journal: field_mask.15?true    // will return from new journal. We will deprecate this flag quickly, hence 15
    compact_journal: field_mask.4?true // will return from new compact journal (only fields for agents)
     = metadata.GetJournalResponsenew fields_mask;

@readwrite statshouse.getTagMapping2#4285ff56
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    metric:string
    key:string
    create:fields_mask.0?true // required, after initial error caches send request to create with empty name, we assign such requests to empty metric
    tag_id_key:int // to look for tag which is source of garbage
    client_env:int // to look for env which is source of garbage
     = statshouse.GetTagMappingResult;

@readwrite statshouse.autoCreate#28bea524
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    metric:string // metric name
    kind:string   // metric kind
    tags:(vector string) // metric tags
    resolution:fields_mask.0?int
    description:fields_mask.1?string
     = True;

// when local cache is empty, statshouse will ask aggregator for common mappings
@readwrite statshouse.getTagMappingBootstrap#75a7f68e
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
     = statshouse.GetTagMappingBootstrapResult;

@readwrite statshouse.getTargets2#41df72a3
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    prom_host_name:string // in contrast to normalized and validated for statshouse key, like in header
    old_hash: string
    gauge_metrics:fields_mask.0?Bool
    metric_relabel_configs:fields_mask.1?Bool
     = statshouse.GetTargetsResult fields_mask;

@readwrite statshouse.testConnection2#4285ff58
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    payload:string
    response_size:int // will reply with response of this size
    response_timeout_sec:int // if >0, will long poll for requested time
     = String;

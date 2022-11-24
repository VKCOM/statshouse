---types---

// Protocol between statshouse and statshouse aggregator is private for now

statshouse.string_top_element key:string value:float = statshouse.StringTopElement;

statshouse.ingestion_status metric:int count:int = statshouse.IngestionStatus; // Legacy - remove

statshouse.item#0c803d06
    fields_mask: # // contains shard num in top 8 bits
    metric: int
    keys: (vector int)

    counter: double

    t: fields_mask.0?# // timestamp, should be rare

    value_min: fields_mask.1?double // simple value (all values identical)
    value_max: fields_mask.2?double // complex value (there is different values)
    value_sum: fields_mask.2?double // complex value (there is different values)
    value_sum_square: fields_mask.2?double // complex value (there is different values)

    percentiles: fields_mask.3?(vector float) // deprecated, newer statshouses use centroids instead

    uniques: fields_mask.4?(vector long) // TBD - send hashes instead of values?

    string_tops: fields_mask.5?(vector statshouse.string_top_element)

    centroids: fields_mask.6?(vector statshouse.centroid)
    = statshouse.Item;

statshouse.sourceBucket#3af6c822
      metrics: (vector statshouse.item) // must be sorted by shard

      sample_factors: (vector statshouse.sample_factor) // We need as compact representation as possible.
      ingestion_status_ok: (vector statshouse.ingestion_status) // We need as compact representation as possible. count < 0 for uncached and count > 0 for cached

      missed_seconds: #
      agent_env: int // used to fill key0 of many built-in stats

  = statshouse.SourceBucket;

---functions---

@readwrite statshouse.getConfig#3285ff57
    fields_mask:#
    host_name:string
    source_env:int
    build_arch:fields_mask.1?int // comparison of x64 and arm64 nodes will be interesting for some time
    cluster:fields_mask.0?string // prevent wrong configuration
     = statshouse.GetConfigResult fields_mask;

@readwrite statshouse.sendSourceBucket#94575940
    fields_mask:#
    shard_replica:fields_mask.5?int // Aggregator will only accept correct shard/replica conbinations
    shard_replica_total:fields_mask.5?int
    ingress_proxy:fields_mask.6?true
    host_name:string
    source_env:fields_mask.9?int
    build_arch:fields_mask.9?int // comparison of x64 and arm64 nodes will be interesting for some time
    legacy_source_ip:string // will be set by proxy in future. If empty, aggregator uses connection remote ip
    time:#
    historic:fields_mask.0?true
    spare:fields_mask.1?true
    shard_replica_legacy:fields_mask.2?int // Deprecated.
    shard_replica_legacy_total:fields_mask.2?int // Deprecated.
    build_commit:fields_mask.3?string
    build_commit_date:fields_mask.7?int // 20220218 for 2022-02-18T22:58:12+0000, so we can easily use raw key and have human-readability
    build_commit_ts:fields_mask.8?int
    queue_size_disk:fields_mask.4?int
    queue_size_memory:fields_mask.4?int
    original_size:int
    compressed_data:string = String;

@readwrite statshouse.sendKeepAlive#3285ff53
    fields_mask:#
    shard_replica:fields_mask.5?int // Aggregator will only accept correct shard/replica conbinations
    shard_replica_total:fields_mask.5?int
    ingress_proxy:fields_mask.6?true
    host_name:string
    source_env:fields_mask.0?int
    build_arch:fields_mask.2?int // comparison of x64 and arm64 nodes will be interesting for some time
    shard_replica_legacy:fields_mask.1?int // Deprecated.
    shard_replica_legacy_total:fields_mask.1?int
     = String;

@readwrite statshouse.getMetrics#3285ff54
    fields_mask:#
    shard_replica:fields_mask.5?int // Aggregator will only accept correct shard/replica conbinations
    shard_replica_total:fields_mask.5?int
    ingress_proxy:fields_mask.6?true
    version:string
    host_name:fields_mask.0?string
    source_env:fields_mask.0?int
    build_arch:fields_mask.1?int // comparison of x64 and arm64 nodes will be interesting for some time
     = statshouse.GetMetricsResult;
@readwrite statshouse.getMetrics2#4285ff54
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    version:string
     = statshouse.GetMetricsResult;

@readwrite statshouse.getTagMapping#3285ff56
    fields_mask:#
    shard_replica:fields_mask.5?int // Aggregator will only accept correct shard/replica conbinations
    shard_replica_total:fields_mask.5?int
    ingress_proxy:fields_mask.6?true
    metric:string
    key:string
    create:fields_mask.0?true // required, after initial error caches send request to create with empty name, we assign such requests to empty metric
    tag_id_key:fields_mask.2?int // to look for tag which is source of garbage
    client_env:fields_mask.2?int // to look for env which is source of garbage
    host_name:fields_mask.2?string
    source_env:fields_mask.2?int
    build_arch:fields_mask.3?int // comparison of x64 and arm64 nodes will be interesting for some time
    canonical_metric_name:fields_mask.1?true // If set, full metric name is passed, if not set, cluster_name is passed
     = statshouse.GetTagMappingResult;

@readwrite statshouse.getTargets#51df72a3
    fields_mask:#
    shard_replica:fields_mask.5?int // Aggregator will only accept correct shard/replica conbinations
    shard_replica_total:fields_mask.5?int
    ingress_proxy:fields_mask.6?true
    host_name:string
    source_env:fields_mask.0?int
    build_arch:fields_mask.0?int // comparison of x64 and arm64 nodes will be interesting for some time
    old_hash: string
     = statshouse.GetTargetsResult fields_mask;

@readwrite statshouse.testConnection#3285ff58
    fields_mask:#
    shard_replica:fields_mask.5?int // Aggregator will only accept correct shard/replica conbinations
    shard_replica_total:fields_mask.5?int
    ingress_proxy:fields_mask.6?true
    payload:string
    response_size:int // will reply with response of this size
    response_timeout_sec:int // if >0, will long poll for requested time
     = String;


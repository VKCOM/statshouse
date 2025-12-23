---types---

statshouseApi.fnCount#89689775                = statshouseApi.Function;
statshouseApi.fnCountNorm#60e68b5c            = statshouseApi.Function;
statshouseApi.fnCumulCount#871201c4           = statshouseApi.Function;
statshouseApi.fnMin#b4cb2644                  = statshouseApi.Function;
statshouseApi.fnMax#f90de384                  = statshouseApi.Function;
statshouseApi.fnAvg#6323c2f6                  = statshouseApi.Function;
statshouseApi.fnCumulAvg#f4d9ad09             = statshouseApi.Function;
statshouseApi.fnSum#80ce3cf1                  = statshouseApi.Function;
statshouseApi.fnSumNorm#361963d5              = statshouseApi.Function;
statshouseApi.fnCumulSum#42fc39b6             = statshouseApi.Function;
statshouseApi.fnStddev#2043e480               = statshouseApi.Function;
statshouseApi.fnP01#381b1cee                  = statshouseApi.Function;
statshouseApi.fnP1#bbb36a23                   = statshouseApi.Function;
statshouseApi.fnP5#bcdeae3a                   = statshouseApi.Function;
statshouseApi.fnP10#56071d39                  = statshouseApi.Function;
statshouseApi.fnP25#cf9ad7bf                  = statshouseApi.Function;
statshouseApi.fnP50#77c5de5c                  = statshouseApi.Function;
statshouseApi.fnP75#0e674272                  = statshouseApi.Function;
statshouseApi.fnP90#d4c8c793                  = statshouseApi.Function;
statshouseApi.fnP95#9a92b76f                  = statshouseApi.Function;
statshouseApi.fnP99#71992e9a                  = statshouseApi.Function;
statshouseApi.fnP999#a3434c26                 = statshouseApi.Function;
statshouseApi.fnUnique#f20fb854               = statshouseApi.Function;
statshouseApi.fnUniqueNorm#9ceb6f68           = statshouseApi.Function;
statshouseApi.fnMaxHost#b4790064              = statshouseApi.Function;
statshouseApi.fnMaxCountHost#885e665b         = statshouseApi.Function;
statshouseApi.fnDerivativeMin#4817df2b        = statshouseApi.Function;
statshouseApi.fnDerivativeMax#43eeb810        = statshouseApi.Function;
statshouseApi.fnDerivativeAvg#60d2b603        = statshouseApi.Function;
statshouseApi.fnDerivativeCount#e617771c      = statshouseApi.Function;
statshouseApi.fnDerivativeCountNorm#bfb5f7fc  = statshouseApi.Function;
statshouseApi.fnDerivativeSum#a3a43781        = statshouseApi.Function;
statshouseApi.fnDerivativeSumNorm#96683390    = statshouseApi.Function;
statshouseApi.fnDerivativeUnique#5745a0a3     = statshouseApi.Function;
statshouseApi.fnDerivativeUniqueNorm#4bd4f327 = statshouseApi.Function;

statshouseApi.flagMapped#670ab89c = statshouseApi.Flag;
statshouseApi.flagRaw#4ca979c0    = statshouseApi.Flag;
statshouseApi.flagAuto#2a6e4c14   = statshouseApi.Flag;

statshouseApi.tagValue#43eeb763 fields_mask:#
    in: Bool
    value: string
    flag: statshouseApi.Flag
    = statshouseApi.TagValue;

statshouseApi.filter#511276a6 fields_mask:#
    key: string
    values: %(Vector %(statshouseApi.TagValue))
    = statshouseApi.Filter;

statshouseApi.seriesMeta#5c2bf286 {query_fields_mask:#} fields_mask:#
    time_shift: long
    tags: %(Dictionary string)
    what: fields_mask.1?statshouseApi.Function
    name: query_fields_mask.4?string
    color: query_fields_mask.5?string
    total: query_fields_mask.6?int
    max_hosts: query_fields_mask.7?%(Vector string)
    = statshouseApi.SeriesMeta query_fields_mask;

statshouseApi.pointMeta#5c2bf296 fields_mask:#
    time_shift: long
    from: long
    to: long
    tags: %(Dictionary string)
    what: fields_mask.1?statshouseApi.Function
    = statshouseApi.PointMeta;

statshouseApi.series#07a3e919 fields_mask:#
    series_data: %(Vector %(Vector double))
    time: %(Vector long)
    = statshouseApi.Series;

statshouseApi.query#c9951bb9 fields_mask:#
    version: int
    top_n: int
    metric_name: string
    time_from: long
    time_to: long
    interval: string
    function: statshouseApi.Function
    group_by: %(Vector string)
    filter: %(Vector %(statshouseApi.Filter))
    time_shift: %(Vector long)
    promql: fields_mask.0?string
    what: fields_mask.1?%(Vector statshouseApi.Function)
    excess_points_flag: fields_mask.2?true
    widthAgg: fields_mask.3?string
    name_flag: fields_mask.4?true
    color_flag: fields_mask.5?true
    total_flag: fields_mask.6?true
    max_host_flag: fields_mask.7?true
    = statshouseApi.Query;

statshouseApi.queryPoint#c9951bbb fields_mask:#
    version: int
    top_n: int
    metric_name: string
    time_from: long
    time_to: long
    function: statshouseApi.Function
    group_by: %(Vector string)
    filter: %(Vector %(statshouseApi.Filter))
    time_shift: %(Vector long)
    what: fields_mask.1?%(Vector statshouseApi.Function)
    = statshouseApi.QueryPoint;

statshouseApi.queryResponse#4487e49a {query_fields_mask:#}
    fields_mask:#
    series: %(statshouseApi.Series)
    series_meta: %(Vector %(statshouseApi.SeriesMeta query_fields_mask))
    chunk_ids: %(Vector int)
    total_time_points: int
    response_id: long
    excess_point_left: fields_mask.0?true
    excess_point_right: fields_mask.1?true
    = statshouseApi.GetQueryResponse query_fields_mask;


statshouseApi.queryPointResponse#4487e41a fields_mask:#
    data: %(Vector double)
    meta: %(Vector %(statshouseApi.PointMeta))
    = statshouseApi.GetQueryPointResponse;


statshouseApi.chunkResponse#63928b42 fields_mask:#
     series: %(statshouseApi.Series)
     index: int
    = statshouseApi.GetChunkResponse;

statshouseApi.releaseChunksResponse#d12dc2bd fields_mask:#
    releasedChunkCount: int
    = statshouseApi.ReleaseChunksResponse;

statshouseApi.getMappingResponse {fields_mask:#}
    intValue:fields_mask.3?int
    stringValue:fields_mask.4?string
    = statshouseApi.GetMappingResponse fields_mask;

---functions---

@readwrite
statshouseApi.getQuery#0c7349bb fields_mask:#
    access_token: string
    query: %statshouseApi.Query
    = statshouseApi.GetQueryResponse fields_mask;

@read
statshouseApi.getQueryPoint#0c7348bb fields_mask:#
    access_token: string
    query: %statshouseApi.QueryPoint
    = statshouseApi.GetQueryPointResponse;

@read
statshouseApi.getChunk#52721884 fields_mask:#
    access_token: string
    response_id: long
    chunk_id: int
    = statshouseApi.GetChunkResponse;

@write
statshouseApi.releaseChunks#62adc773 fields_mask:#
    access_token: string
    response_id: long
    = statshouseApi.ReleaseChunksResponse;/////

@readwrite statshouseApi.getMapping#4239a8f8
    fields_mask:#
    access_token: string
    intValue:fields_mask.0?int
    stringValue:fields_mask.1?string
     = statshouseApi.GetMappingResponse fields_mask;

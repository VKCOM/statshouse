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

statshouseApi.seriesMeta#5c2bf286 fields_mask:#
    time_shift: long
    tags: %(Dictionary string)
    what: fields_mask.1?statshouseApi.Function
    = statshouseApi.SeriesMeta;

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
    what: fields_mask.1?%(Vector statshouseApi.Function)
    = statshouseApi.Query;

statshouseApi.queryResponse#4487e49a fields_mask:#
    series: %(statshouseApi.Series)
    series_meta: %(Vector %(statshouseApi.SeriesMeta))
    chunk_ids: %(Vector int)
    total_time_points: int
    response_id: long
    = statshouseApi.GetQueryResponse;

statshouseApi.chunkResponse#63928b42 fields_mask:#
     series: %(statshouseApi.Series)
     index: int
    = statshouseApi.GetChunkResponse;

statshouseApi.releaseChunksResponse#d12dc2bd fields_mask:#
    releasedChunkCount: int
    = statshouseApi.ReleaseChunksResponse;

---functions---

@readwrite
statshouseApi.getQuery#0c7349bb fields_mask:#
    access_token: string
    query: %statshouseApi.Query
    = statshouseApi.GetQueryResponse;

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

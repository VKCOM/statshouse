
statshouse_metadata.metricOld#9286abfa {field_mask: #} // метрики
    id: long
    name: string
    event_type: field_mask.2?int
    unused: field_mask.3?#
    version: long
    update_time: #
    data: string = statshouse_metadata.MetricOld field_mask;

statshouse_metadata.event#9286affa
    field_mask: #
    id: long
    name: string
    event_type: int
    unused: # // not this is always 0
    version: long
    update_time: #
    data: string = statshouse_metadata.Event;

statshouse_metadata.getMetricsResponse#9286abfb {field_mask: #}
    current_version: long
    metrics: (vector (statshouse_metadata.metricOld field_mask))
    = statshouse_metadata.GetMetricsResponse field_mask;

statshouse_metadata.getJournalResponsenew#9286aaaa {field_mask: #}
    current_version: long // generally should be ignored by receiver logic, and version of last event in vector used .
    events: (vector statshouse_metadata.event)
    = statshouse_metadata.GetJournalResponsenew field_mask;

statshouse_metadata.getMappingResponse#9286abfc {field_mask: #}
    id: int
    = statshouse_metadata.GetMappingResponse field_mask;

statshouse_metadata.getMappingResponseFloodLimitError#9286abfd {field_mask: #}
    = statshouse_metadata.GetMappingResponse field_mask;

statshouse_metadata.getMappingResponseKeyNotExists#9286abff {field_mask: #}
    = statshouse_metadata.GetMappingResponse field_mask;

statshouse_metadata.getMappingResponseCreated#9286abbb {field_mask: #}
    id: int
    = statshouse_metadata.GetMappingResponse field_mask;

statshouse_metadata.getInvertMappingResponse#9286abac {field_mask: #}
    key: string
    = statshouse_metadata.GetInvertMappingResponse field_mask;

statshouse_metadata.getInvertMappingResponseKeyNotExists#9286abab {field_mask: #}
    = statshouse_metadata.GetInvertMappingResponse field_mask;

statshouse_metadata.putMappingResponse#9286abfe
    = statshouse_metadata.PutMappingResponse;


statshouse_metadata.resetFloodResponse#9286abee
    = statshouse_metadata.ResetFloodResponse;

statshouse_metadata.resetFloodResponse2#9286abef
    budget_before:int
    budget_after:int
    = statshouse_metadata.ResetFloodResponse2;

---functions---

@readwrite statshouse_metadata.editEntitynew#86df475f
    fields_mask: #
    event: statshouse_metadata.event
    create: fields_mask.0?true
    delete: fields_mask.1?true
     = statshouse_metadata.Event;

@read statshouse_metadata.getMetrics#93ba92f5
    field_mask: #
    from: long // строго больше
    limit: long
     = statshouse_metadata.GetMetricsResponse field_mask;

@read statshouse_metadata.getJournalnew#93ba92f8
    field_mask: #
    from: long // строго больше
    limit: long
    return_if_empty: field_mask.3?true
     = statshouse_metadata.GetJournalResponsenew field_mask;

@readwrite statshouse_metadata.getMapping#9dfa7a83
    field_mask: #
    metric: string
    key: string
    createIfAbsent: field_mask.1?%True
     = statshouse_metadata.GetMappingResponse field_mask;

@readwrite statshouse_metadata.putMapping#9faf5281
    field_mask: #
    keys: (vector string) // TODO - 2 arrays are crap, use (vector statshouse.mapping)
    value: (vector int)
    = statshouse_metadata.PutMappingResponse;

@read statshouse_metadata.getInvertMapping#9faf5280
    field_mask: #
    id: int
    = statshouse_metadata.GetInvertMappingResponse field_mask;

// TODO - deprecate this func, has no fields_mask so not extendable
@readwrite statshouse_metadata.resetFlood#9faf5282
    metric: string
    = statshouse_metadata.ResetFloodResponse;

@readwrite statshouse_metadata.resetFlood2#88d0fd5e
    field_mask: #
    metric: string
    = statshouse_metadata.ResetFloodResponse2;

@readwrite statshouse_metadata.getTagMappingBootstrap#5fc81a9b
    fields_mask:#
     = statshouse.GetTagMappingBootstrapResult;

@readwrite statshouse_metadata.putTagMappingBootstrap#5fc8ab9b
    fields_mask:#
    mappings: (vector statshouse.mapping)
     = statshouse.PutTagMappingBootstrapResult;

---types---

statshouse_metadata.createMappingEvent#12345678
    field_mask: #
    id: int
    key: string
    metric: string // todo use id instead
    badget: long
    create: field_mask.0?%True
	updated_at: #
     = statshouse_metadata.CreateMappingEvent;

statshouse_metadata.editMetricEvent#12345677
    fields_mask: #
    metric: (statshouse_metadata.metricOld fields_mask)
    old_version: long = statshouse_metadata.EditMetricEvent;

statshouse_metadata.createMetricEvent#12345674
    fields_mask: #
    metric: (statshouse_metadata.metricOld fields_mask)
     = statshouse_metadata.CreateMetricEvent;

statshouse_metadata.editEntityEvent#1234b677
    fields_mask: #
    metric: (statshouse_metadata.event)
    old_version: long = statshouse_metadata.EditEntityEvent;

statshouse_metadata.createEntityEvent#1a345674
    fields_mask: #
    metric: (statshouse_metadata.event)
     = statshouse_metadata.CreateEntityEvent;

statshouse_metadata.putMappingEvent#12345676
    fields_mask: #
    keys: (vector string)
    value: (vector int) = statshouse_metadata.PutMappingEvent;

statshouse_metadata.putBootstrapEvent#5854dfaf
    fields_mask: #
    mappings: (vector statshouse.mapping) = statshouse_metadata.PutBootstrapEvent;

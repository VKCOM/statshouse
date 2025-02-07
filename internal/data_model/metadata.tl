
metadata.metricOld#9286abfa {field_mask: #} // метрики
    id: long
    name: string
    event_type: field_mask.2?int
    unused: field_mask.3?#
    version: long
    update_time: #
    data: string = metadata.MetricOld field_mask;

metadata.event#9286affa
    field_mask: #
    id: long
    name: string
    namespace_id: field_mask.0?long // empty or 0 namespace is default namepsace
    event_type: int
    unused: # // not this is always 0
    version: long
    update_time: #
    data: string
    metadata: field_mask.1?string = metadata.Event;


metadata.history_short_response_event#1186baaf {field_mask: #}
    version: long
    metadata: string
        = metadata.History_Short_Response_event field_mask;

metadata.history_short_response#7186baaf {field_mask: #}
    events: (Vector (metadata.history_short_response_event field_mask))
    = metadata.History_Short_Response field_mask;

metadata.getMetricsResponse#9286abfb {field_mask: #}
    current_version: long
    metrics: (vector (metadata.metricOld field_mask))
    = metadata.GetMetricsResponse field_mask;

metadata.getJournalResponsenew#9286aaaa {field_mask: #}
    current_version: long // important for receivers that want to load until 'latest' version. Also useful for reporting progress of loading.
    events: (vector metadata.event)
    = metadata.GetJournalResponsenew field_mask;

metadata.getMappingResponse#9286abfc {field_mask: #}
    id: int
    = metadata.GetMappingResponse field_mask;

metadata.getMappingResponseFloodLimitError#9286abfd {field_mask: #}
    = metadata.GetMappingResponse field_mask;

metadata.getMappingResponseKeyNotExists#9286abff {field_mask: #}
    = metadata.GetMappingResponse field_mask;

metadata.getMappingResponseCreated#9286abbb {field_mask: #}
    id: int
    = metadata.GetMappingResponse field_mask;

metadata.getInvertMappingResponse#9286abac {field_mask: #}
    key: string
    = metadata.GetInvertMappingResponse field_mask;

metadata.getInvertMappingResponseKeyNotExists#9286abab {field_mask: #}
    = metadata.GetInvertMappingResponse field_mask;

metadata.putMappingResponse#9286abfe
    = metadata.PutMappingResponse;


metadata.resetFloodResponse#9286abee
    = metadata.ResetFloodResponse;

metadata.resetFloodResponse2#9286abef
    budget_before:int
    budget_after:int
    = metadata.ResetFloodResponse2;

---functions---

@readwrite metadata.editEntitynew#86df475f
    fields_mask: #
    event: metadata.event
    create: fields_mask.0?true
    delete: fields_mask.1?true
     = metadata.Event;

@readwrite metadata.getHistoryShortInfo#22ff6a79
    fields_mask: #
    id: long
     = metadata.History_Short_Response fields_mask;

@read metadata.getMetrics#93ba92f5
    field_mask: #
    from: long // строго больше
    limit: long
     = metadata.GetMetricsResponse field_mask;

@read metadata.getJournalnew#93ba92f8
    field_mask: #
    from: long // строго больше
    limit: long
    return_if_empty: field_mask.3?true
     = metadata.GetJournalResponsenew field_mask;

@read metadata.getEntity#72b132f8
    field_mask: #
    id: long
    version: long
     = metadata.Event;


@readwrite metadata.getMapping#9dfa7a83
    field_mask: #
    metric: string
    key: string
    createIfAbsent: field_mask.1?%True
     = metadata.GetMappingResponse field_mask;

@readwrite metadata.putMapping#9faf5281
    field_mask: #
    keys: (vector string) // TODO - 2 arrays are crap, use (vector statshouse.mapping)
    value: (vector int)
    = metadata.PutMappingResponse;

@read metadata.getInvertMapping#9faf5280
    field_mask: #
    id: int
    = metadata.GetInvertMappingResponse field_mask;

// TODO - deprecate this func, has no fields_mask so not extendable
@readwrite metadata.resetFlood#9faf5282
    metric: string
    = metadata.ResetFloodResponse;

@readwrite metadata.resetFlood2#88d0fd5e
    field_mask: #
    metric: string
    value: field_mask.1?%int
    = metadata.ResetFloodResponse2;

@readwrite metadata.getTagMappingBootstrap#5fc81a9b
    fields_mask:#
     = statshouse.GetTagMappingBootstrapResult;

@readwrite metadata.putTagMappingBootstrap#5fc8ab9b
    fields_mask:#
    mappings: (vector statshouse.mapping)
     = statshouse.PutTagMappingBootstrapResult;

---types---

metadata.createMappingEvent#12345678
    field_mask: #
    id: int
    key: string
    metric: string // todo use id instead
    budget: long
    create: field_mask.0?%True
	updated_at: #
     = metadata.CreateMappingEvent;

// deprecated
metadata.editMetricEvent#12345677
    fields_mask: #
    metric: (metadata.metricOld fields_mask)
    old_version: long = metadata.EditMetricEvent;

// deprecated
metadata.createMetricEvent#12345674
    fields_mask: #
    metric: (metadata.metricOld fields_mask)
     = metadata.CreateMetricEvent;

metadata.editEntityEvent#1234b677
    fields_mask: #
    metric: (metadata.event)
    old_version: long = metadata.EditEntityEvent;

metadata.createEntityEvent#1a345674
    fields_mask: #
    metric: (metadata.event)
     = metadata.CreateEntityEvent;

metadata.putMappingEvent#12345676
    fields_mask: #
    keys: (vector string)
    value: (vector int) = metadata.PutMappingEvent;

metadata.putBootstrapEvent#5854dfaf
    fields_mask: #
    mappings: (vector statshouse.mapping) = metadata.PutBootstrapEvent;

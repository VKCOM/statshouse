---types---

kv_engine.meta_info#53eeb763
    db_offset: long
    committed_offset: long
    = kv_engine.Meta_Info;

kv_engine.get_response#63eeb764
    meta: kv_engine.meta_info
    value: long
    = kv_engine.Get_Response;

kv_engine.change_response#73eaa764
    meta: kv_engine.meta_info
    new_value: long
    = kv_engine.Change_Response;



kv_engine.backup_response#732a1764
    path: string
    offset: long
    = kv_engine.Backup_Response;


//kv_engine.check_response#123a1764
//    ok: Bool
//    = kv_engine.Check_Response;

kv_engine.kv key:long value:long = kv_engine.KV;


---functions---

@read
kv_engine.get#1c7349bb
    key: long
    = kv_engine.Get_Response;

@readwrite
kv_engine.put#2c7349ba
    key: long
    value: long
    = kv_engine.Change_Response;

@readwrite
kv_engine.inc#3c7239bb
    key: long
    incr: long
    = kv_engine.Change_Response;

@readwrite
kv_engine.check#2c3239ba
    kv: (vector kv_engine.KV)
    = Bool;

@readwrite
kv_engine.backup#3c7231b2
    prefix: string
    = kv_engine.Backup_Response;

@read
kv_engine.healthcheck#2c1259aa = Bool;

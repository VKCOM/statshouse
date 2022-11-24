---types---

vectorTotal {t:Type} total_count:int vector:%(Vector t) = VectorTotal t;
// Boolean emulation
boolFalse#bc799737 = Bool;
boolTrue#997275b5 = Bool;

boolStat statTrue:int statFalse:int statUnknown:int = BoolStat;

stat#9d56e6b2 %(Dictionary string) = Stat;

net.pid ip:int port_pid:int utime:int = net.Pid;

engine.query {X:Type} query:!X = engine.Query;
engine.queryShortened query:%(VectorTotal int) = engine.Query;

engine.queryResult size:int = engine.QueryResult;
engine.queryResultError error_code:int error_string:string = engine.QueryResult;
engine.queryResultAio = engine.QueryResult;


engine.httpQuery#58300321 fields_mask:#
    uri:fields_mask.0?string
    args:fields_mask.1?%(Dictionary string)
    headers:fields_mask.2?%(Dictionary string)
    = engine.HttpQuery;

engine.httpQueryResponse#284852fc fields_mask:#
    return_code:fields_mask.0?int
    data:fields_mask.1?string
    content_type:fields_mask.2?string
    additional_headers:fields_mask.3?%(Dictionary string)
    = engine.HttpQueryResponse;

engine.reindexStatusNever = engine.ReindexStatus;
engine.reindexStatusRunningOld#ac530b46 pid:int start_time:int = engine.ReindexStatus;
engine.reindexStatusRunning pids:%(Vector int) start_time:int = engine.ReindexStatus;
engine.reindexStatusFailed exit_code:int finish_time:int = engine.ReindexStatus;
engine.reindexStatusSignaled signal:int finish_time:int = engine.ReindexStatus;
engine.reindexStatusDoneOld#afdbd505 finish_time:int = engine.ReindexStatus;
engine.reindexStatusDone finish_time:int need_restart:Bool = engine.ReindexStatus;

// if engine has memory_megabytes memory for metafiles -> how many times disk will be used to load metafiles?
engine.metafilesOneMemoryStat
  memory_megabytes:int
  total_aio_queries:long
  total_aio_bytes:long
  = engine.MetafilesOneMemoryStat;

engine.metafilesStatData#b673669b
  data:%(Vector %engine.MetafilesOneMemoryStat)
  = engine.MetafilesStat;

engine.switchedToMasterMode = engine.SwitchMasterReplicaModeResult;
engine.switchedToReplicaMode = engine.SwitchMasterReplicaModeResult;
engine.alreadyInMasterMode = engine.SwitchMasterReplicaModeResult;
engine.alreadyInReplicaMode = engine.SwitchMasterReplicaModeResult;
engine.switchedToMasterModeForcefully bytes_truncated:long = engine.SwitchMasterReplicaModeResult;

engine.failedToSwitchMode error:string = engine.SwitchMasterReplicaModeResult;

engine.mode#b9b7b6c9 {fields_mask:#} read_enabled:fields_mask.0?Bool write_enabled:fields_mask.1?Bool = engine.ReadWriteMode fields_mask;

engine.binlogPrefix binlog_prefix:string snapshot_prefix:string = engine.BinlogPrefix;

engine.reloadDynamicLibOptions#0f3d0fb1
  fields_mask:#

  lib_id:string
  lib_file_name:string

  slices_part:fields_mask.0?double // reload libs on first (int)(slices_part * total_slices) slices (default value is 1.0)
  = engine.ReloadDynamicLibOptions;

---functions---

@any @internal engine.switchToMasterMode = engine.SwitchMasterReplicaModeResult;
@any @internal engine.switchToReplicaMode = engine.SwitchMasterReplicaModeResult;

// Same as engine.switchToMasterMode, but with binlog truncation
@any @internal engine.switchToMasterModeForcefully = engine.SwitchMasterReplicaModeResult;

@any engine.getReadWriteMode fields_mask:# = engine.ReadWriteMode fields_mask;

// Only for engines, which use rpc_queries.
@any @internal engine.replaceConfigServer cluster_name:string server_offset:int host:string port:int = True;

//NEVER use this. Only for debugging engines.
@any @internal engine.setVerbosity verbosity:int = True;
@any @internal engine.setVerbosityType type:string verbosity:int = True;
@write @internal engine.setPersistentConfigValue name:string value:int = True;
@any @internal engine.setNoPersistentConfigValue name:string value:int = True; // unreliable: may have different value when indexed with/without snapshot
@write @internal engine.setPersistentConfigArray name:string values:%(Vector int) = True;
@any @internal engine.setNoPersistentConfigArray name:string values:%(Vector int) = True;
@any @internal engine.sendSignal signal:int = True;

// The following queries effects work until engine restarts
@any @internal engine.dumpNextQueries num_queries:int = True;
@any @internal engine.dumpLastQueries = True;
@any @internal engine.dumpForceQueries buffers_pressure_threshold:double = True;
@any @internal engine.recordNextQueries binlogname:string num_queries:int append:Bool = Bool;

@any @internal engine.sendResponseTo {X:Type} pid:%net.Pid query:!X = Bool;

@any engine.nop  = True;
@read engine.readNop = True; // same as nop, but assumed as read in proxy
@write engine.writeNop = True; // same as nop, but assumed as write in proxy
@any engine.stat = Stat;
@any engine.filteredStat stat_names:%(Vector string) = Stat;
@write @internal engine.pushStat#f4b19fa2 fields_mask:# stat:fields_mask.0?%Stat = Bool;
@any engine.count = BoolStat;
@any engine.pid = net.Pid;
@any engine.version = String;

@read @internal engine.setMetafileMemory megabytes:int = BoolStat; // supported only by text && messages now.
@write @internal engine.setFsyncInterval seconds:double = BoolStat; // supported only by messages

@any @internal engine.invokeHttpQuery query:%engine.HttpQuery = engine.HttpQueryResponse;

@readwrite engine.registerDynamicLib lib_id:string = BoolStat;
@readwrite engine.unregisterDynamicLib lib_id:string = BoolStat;
@readwrite engine.reloadDynamicLib options:%engine.ReloadDynamicLibOptions = BoolStat;

@any @internal engine.sleep time_ms:int = Bool;
@any @internal engine.asyncSleep time_ms:int = Bool;
@any @internal engine.getReindexStatus = engine.ReindexStatus;

@any @internal engine.getBinlogPrefixes = Vector %engine.BinlogPrefix;

@any @internal engine.enableMetafilesAnalyzer enable:Bool = Bool;
@any @internal engine.getExpectedMetafilesStats = Dictionary engine.MetafilesStat;

@any engine.isProduction = Bool;

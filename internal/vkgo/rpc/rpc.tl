// Definitions requried for RPC server
// This file is used to generate "extra" code (cd pkg/vktl && make gen_rpc)
// then this code is slightly edited and updated

vector#1cb5c415 {t:Type} # [t] = Vector t;
tuple#9770768a {t:Type} {n:#} [t] = Tuple t n;

dictionaryField {t:Type} key:string value:t = DictionaryField t;
dictionary#1f4c618f {t:Type} %(Vector %(DictionaryField t)) = Dictionary t;

netPID ip:int port_pid:int utime:int = NetPID;

true = True;

rpcInvokeReqExtra#f3ef81a9
  flags:#

  return_binlog_pos:flags.0?%True
  return_binlog_time:flags.1?%True
  return_pid:flags.2?%True
  return_request_sizes:flags.3?%True
  return_failed_subqueries:flags.4?%True
  return_query_stats:flags.6?%True
  no_result:flags.7?%True // Currently for proxy only. Client goes to proxy, it clears this bit and sends query to engine. Client does not wait for answer.
  // Bits 17, 22, 24 was used in before, but their support was dropped
  wait_binlog_pos:flags.16?long // Perform query only after position in binlog is at least this
  string_forward_keys:flags.18?%(Vector string) // For cluster that are split by string (like pmemcached in some modes) - first specified string is used to choose target, then it is deleted from vector
  int_forward_keys:flags.19?%(Vector long) // First long is used to choose target. Then it is deleted from vector
  string_forward:flags.20?string // Same as string_forward_keys, but it is not deleted
  int_forward:flags.21?long // Same as int_forward_keys, but it is not deleted
  custom_timeout_ms:flags.23?int  // Custom timeout for query
  supported_compression_version:flags.25?int // note, that client support compression, to possibly compress answers
  random_delay:flags.26?double // starting query would be delayed by random number, not grater than given
  = RpcInvokeReqExtra;

rpcReqResultExtra#c5011709
    flags:#

    binlog_pos:flags.0?long  binlog_time:flags.1?long
    engine_pid:flags.2?%netPID
    request_size:flags.3?int response_size:flags.3?int
    failed_subqueries:flags.4?int
    compression_version:flags.5?int
    stats:flags.6?%(Dictionary string)
    = RpcReqResultExtra;

reqError#b527877d {X:Type} error_code:int error:string = ReqResult X;
reqResultHeader#8cc84ce1 {X:Type} flags:# extra:%(RpcReqResultExtra flags) result:X = ReqResult X;
//reqResultFalse {X:Type} = ReqResult X;
_ {X:Type} result:X = ReqResult X;

rpcReqResult#63aeda4e {X:Type} query_id:long result:(ReqResult X) = RpcReqResult X;

rpcReqResultError#7ae432f5 query_id:long error_code:int error:string = RpcReqResultError;

rpcPong#8430eaa7 ping_id:long = RpcPong;

rpcDestFlags#e352035e extra:%(RpcInvokeReqExtra) = RpcDestFlags;
rpcDestActor#7568aabd actor_id:long = RpcDestActor;
rpcDestActorFlags#f0a5acf7 actor_id:long extra:%(RpcInvokeReqExtra) = RpcDestActorFlags;

---functions---

@any @internal rpcPing#5730a2df ping_id:long = RpcPong;
@any rpcInvokeReq#2374df3d {X:Type} query_id:long = RpcReqResult X;

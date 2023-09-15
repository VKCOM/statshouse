// Definitions requried for RPC server
// This file is used to generate "extra" code (cd pkg/vktl && make gen_rpc)
// then this code is slightly edited and updated

// TODO - replace definitions in combined.tl with definitions here

vector#1cb5c415 {t:Type} # [t] = Vector t;
tuple#9770768a {t:Type} {n:#} [t] = Tuple t n;

dictionaryField {t:Type} key:string value:t = DictionaryField t;
dictionary#1f4c618f {t:Type} %(Vector %(DictionaryField t)) = Dictionary t;

netPID ip:# port_pid:# utime:# = NetPID;

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
  return_view_number:flags.27?%True // GMS2 related parameter: return view number in response
  = RpcInvokeReqExtra;

rpcReqResultExtra#c5011709
    flags:#

    binlog_pos:flags.0?long  binlog_time:flags.1?long
    engine_pid:flags.2?%netPID
    request_size:flags.3?int response_size:flags.3?int
    failed_subqueries:flags.4?int
    compression_version:flags.5?int
    stats:flags.6?%(Dictionary string)
    view_number:flags.27?%long
    = RpcReqResultExtra;

reqError#b527877d error_code:int error:string = ReqError;
reqResultHeader#8cc84ce1 extra:%RpcReqResultExtra = ReqResultHeader;
//reqResultFalse {X:Type} = ReqResult X;
//_ {X:Type} result:X = ReqResult X;

rpcReqResultHeader#63aeda4e query_id:long = RpcReqResultHeader;
rpcReqResultError#7ae432f5 query_id:long error_code:int error:string = RpcReqResultError;

rpcPong#8430eaa7 ping_id:long = RpcPong;
rpcPing#5730a2df ping_id:long = RpcPing;

rpcDestFlags#e352035e extra:%(RpcInvokeReqExtra) = RpcDestFlags;
rpcDestActor#7568aabd actor_id:long = RpcDestActor;
rpcDestActorFlags#f0a5acf7 actor_id:long extra:%(RpcInvokeReqExtra) = RpcDestActorFlags;

rpcInvokeReqHeader#2374df3d query_id:long = RpcInvokeReqHeader;

// Experimental. Top-level packet magic. Client sends on request cancel, so server can free context associated with request.
// No response from server is sent. Required for correctly cancelling long poll requests.
// Supported by vkgo rpc.Server/rpc.Client for now. Support is advertised during PacketConn handshake with flag bit.
rpcCancelReq#193f1b22 query_id:long = RpcCancelReq;

// Experimental. Top-level packet magic. Server sends once per connection to ask for gracefully close connection.
// No response from client is sent. Client should finish sending last request and then send FIN (::shutdown(SHUT_WR)).
// Server will send all responses, then close connection. Client will send all new requests to a new connection.
// Supported by vkgo rpc.Server/rpc.Client for now. Support is advertised during PacketConn handshake with flag bit.
rpcServerWantsFin#a8ddbc46 = RpcServerWantsFin;

netUdpPacket.unencHeader#00a8e945
  flags:#
  remote_pid:flags.0?%NetPID local_pid:flags.0?%NetPID generation:flags.0?int
  pid_hash:flags.2?long
  crypto_init:flags.3?int
  random_key:flags.5?%(Tuple # 8)
  = netUdpPacket.UnencHeader;

netUdpPacket.encHeader#251a7bfd
  flags:#
  time:flags.9?int
  version:flags.10?int
  packet_ack_prefix:flags.13?int
  packet_ack_from:flags.14?int packet_ack_to:flags.14?int
  packet_ack_set:flags.15?%(Vector int)
  packet_num:flags.20?int
  packets_from:flags.21?int packets_to:flags.21?int
  prev_parts:flags.22?int
  next_parts:flags.23?int
  = netUdpPacket.EncHeader;

// Definitions requried for RPC server, which uses TL for rpc request/response wrapper
// This file is used to generate parsing/serialisation code (cd pkg/vktl && make gen_rpc)

// Big TODO - replace definitions in combined.tl with definitions here

vector#1cb5c415 {t:Type} # [t] = Vector t;
tuple#9770768a {t:Type} {n:#} [t] = Tuple t n;

string#b5286e24 ? = String;

dictionaryField {t:Type} key:string value:t = DictionaryField t;
dictionary#1f4c618f {t:Type} %(Vector %(DictionaryField t)) = Dictionary t;

stat#9d56e6b2 %(Dictionary string) = Stat;

net.pid#46409ccf ip:# port_pid:# utime:# = net.Pid;
// mutation of below
// pid   host:int port_pid:int utime:int = Pid;
// net.pid ip:int port_pid:int utime:int = net.Pid;

// Boolean emulation
boolFalse#bc799737 = Bool;
boolTrue#997275b5 = Bool;

true = True;

exactlyOnce.uuid#c97c16b2 lo:long hi:long = exactlyOnce.Uuid;

// if the query header specifies exactlyOnce.prepareRequest, then the engine will respond with this type
exactlyOnce.slotResponse#95f25c81 persistent_query_uuid:%exactlyOnce.Uuid persistent_slot_uuid:%exactlyOnce.Uuid = exactlyOnce.SlotResponse;

// if the query header specifies exactlyOnce.commitRequest, then the engine will respond with this type
exactlyOnce.ackResponse#17641550 persistent_query_uuid:%exactlyOnce.Uuid = exactlyOnce.AckResponse;

exactlyOnce.prepareRequest#c8d71b66 persistent_query_uuid:%exactlyOnce.Uuid = exactlyOnce.PersistentRequest;
exactlyOnce.commitRequest#6836b983 persistent_query_uuid:%exactlyOnce.Uuid persistent_slot_uuid:%exactlyOnce.Uuid = exactlyOnce.PersistentRequest;

allocSlotEvent#2abb2c70
  key:exactlyOnce.uuid
  slot:exactlyOnce.uuid
= AllocSlotEvent;

releaseSlotEvent#7f045ccc
  key:exactlyOnce.uuid
= ReleaseSlotEvent;

tracing.traceID#2f4ac855 lo:long hi:long = tracing.TraceID;

tracing.traceContext#c463a95c
    fields_mask: #
    trace_id: %(tracing.TraceID)
    parent_id: fields_mask.2?long
    source_id: fields_mask.3?string

    // 0 = drop, 1 = record, 2 = defer
    reserved_status_0: fields_mask.0?true
    reserved_status_1: fields_mask.1?true

    reserved_level_0: fields_mask.4?true
    reserved_level_1: fields_mask.5?true
    reserved_level_2: fields_mask.6?true

    debug_flag: fields_mask.7?true

   = tracing.TraceContext;

rpcInvokeReqExtra#f3ef81a9
  flags:#

  return_binlog_pos:flags.0?%True
  return_binlog_time:flags.1?%True
  return_pid:flags.2?%True
  return_request_sizes:flags.3?%True
  return_failed_subqueries:flags.4?%True
  return_query_stats:flags.6?%True
  no_result:flags.7?%True // Currently for proxy only. Client goes to proxy, it clears this bit and sends query to engine. Client does not wait for answer.
  return_shards_binlog_pos:flags.14?%True
  // Bits 17, 22, 24 was used in before, but their support was dropped
  wait_shards_binlog_pos:flags.15?%(Dictionary long)
  wait_binlog_pos:flags.16?long // Perform query only after position in binlog is at least this
  string_forward_keys:flags.18?%(Vector string) // For cluster that are split by string (like pmemcached in some modes) - first specified string is used to choose target, then it is deleted from vector
  int_forward_keys:flags.19?%(Vector long) // First long is used to choose target. Then it is deleted from vector
  string_forward:flags.20?string // Same as string_forward_keys, but it is not deleted
  int_forward:flags.21?long // Same as int_forward_keys, but it is not deleted
  custom_timeout_ms:flags.23?int  // Custom timeout for query
  supported_compression_version:flags.25?int // note, that client support compression, to possibly compress answers
  random_delay:flags.26?double // starting query would be delayed by random number, not grater than given
  return_view_number:flags.27?%True // Barsic related parameter: return view number in response
  persistent_query:flags.28?exactlyOnce.PersistentRequest
  trace_context:flags.29?%(tracing.TraceContext)
  execution_context:flags.30?string // contains serialized execution context.
  = RpcInvokeReqExtra;

rpcReqResultExtra#c5011709
    flags:#

    binlog_pos:flags.0?long  binlog_time:flags.1?long
    engine_pid:flags.2?%net.Pid
    request_size:flags.3?int response_size:flags.3?int
    failed_subqueries:flags.4?int
    compression_version:flags.5?int
    stats:flags.6?%(Dictionary string)
    shards_binlog_pos:flags.14?%(Dictionary long)
    epoch_number:flags.27?%long view_number:flags.27?%long
    = RpcReqResultExtra;

reqError#b527877d error_code:int error:string = ReqError;
reqResultHeader#8cc84ce1 extra:%RpcReqResultExtra = ReqResultHeader;
//reqResultFalse {X:Type} = ReqResult X;
//_ {X:Type} result:X = ReqResult X;

rpcReqResultHeader#63aeda4e query_id:long = RpcReqResultHeader;
rpcReqResultError#7ae432f5 query_id:long error_code:int error:string = RpcReqResultError;
rpcReqResultErrorWrapped#7ae432f6 error_code:int error:string = RpcReqResultErrorWrapped;

rpcPong#8430eaa7 ping_id:long = RpcPong;
rpcPing#5730a2df ping_id:long = RpcPing;

rpcDestFlags#e352035e extra:%(RpcInvokeReqExtra) = RpcDestFlags;
rpcDestActor#7568aabd actor_id:long = RpcDestActor;
rpcDestActorFlags#f0a5acf7 actor_id:long extra:%(RpcInvokeReqExtra) = RpcDestActorFlags;

rpcInvokeReqHeader#2374df3d query_id:long = RpcInvokeReqHeader;

// We wrap body of TL2 request with this marker.
// Any response is parsed in the context of request, so we decided to not wrap response.
// We will remove all wrapping later as part of rpcInvokeReq2 transition.
rpcTL2Marker#29324c54 = RpcTL2Marker;

// Experimental. Top-level packet magic. Client sends on request cancel, so server can free context associated with request.
// No response from server is sent. Required for correctly cancelling long poll requests.
// Supported by vkgo rpc.Server/rpc.Client for now. Support is advertised during PacketConn handshake with flag bit.
rpcCancelReq#193f1b22 query_id:long = RpcCancelReq;

// Top-level packet magic. Server sends RpcServerWantsFin once per connection to ask for gracefully closing connection.
// Client should finish sending current request, send RpcClientWantsFin, send no more requests and wait connection to be closed by server.
// Server will send all non-longpoll responses, then close connection. Client will send all new requests to a new connection.
// Long poll requests make this protocol more complicated, as client does not know which requests have been added to long poll map,
// hence client cannot rely on waiting to finish all responses.
// Supported by vkgo rpc.Server/rpc.Client for now. Support is advertised during PacketConn handshake with flag bit.
rpcServerWantsFin#a8ddbc46 = RpcServerWantsFin;
rpcClientWantsFin#0b73429e = RpcClientWantsFin;

netUdpPacket.unencHeader#00a8e945
  flags:#
  local_pid:flags.0?%net.Pid remote_pid:flags.0?%net.Pid  generation:flags.0?# // TODO generation is nat or int???
  pid_hash:flags.2?long
  // if pid_hash is not set,then crypto_flags includes key_id (12 bits starting with the 20-th bit),
  // also includes mode_aes bit and handshake bit
  crypto_flags:flags.3?#
  crypto_sha:flags.4?%True            // must always be set
  crypto_random:flags.5?%(Tuple # 8) // must always be set
  encrypted_data:flags.7?%True        // must always be set
  support_msg_offsets:flags.8?%True   // does peer support packet_offset, prev_length and next_length in netUdpPacket.encHeader
  = netUdpPacket.UnencHeader;

netUdpPacket.encHeader#251a7bfd
  flags:#
  time:flags.9?int
  version:flags.10?int
  packet_ack_prefix:flags.13?#
  packet_ack_from:flags.14?# packet_ack_to:flags.14?#
  packet_ack_set:flags.15?%(Vector #)
  packet_num:flags.20?#
  packets_from:flags.21?# packets_count:flags.21?#
  prev_parts:flags.22?#
  next_parts:flags.23?#
  prev_length:flags.24?#
  next_length:flags.25?#
  single_rpc_msg:flags.26?%True
  multiple_rpc_msgs:flags.27?%True
  zero_padding_4_bytes:flags.28?%True
  zero_padding_8_bytes:flags.29?%True
  packet_offset:flags.30?long
  window_control:flags.31?#
  = netUdpPacket.EncHeader;

netUdpPacket.resendRange#5efaad4a
  packet_num_from:#
  packet_num_to:#
  = netUdpPacket.ResendRange;

netUdpPacket.resendRequest#643736d9
  ranges:(vector netUdpPacket.resendRange)
  = netUdpPacket.ResendRequest;

netUdpPacket.obsoleteGeneration#b340010b
  pid:net.Pid
  generation:#
  = netUdpPacket.ObsoleteGeneration;

netUdpPacket.obsoleteHash#1adb0f4e
  hash:#
  pid:net.Pid
  = netUdpPacket.ObsoleteHash;

netUdpPacket.obsoletePid#6f4ac134
  obsolete_pid:net.Pid
  actual_pid:net.Pid
  generation:#
  = netUdpPacket.ObsoletePid;

netUdpPacket.wait#6e321c96
  = netUdpPacket.Wait;

---functions---
//@any @internal engine.rpcTest
//    dummy:string // dummy string to test various request body sizes
//    return_size:# // return string of this size to test various response body sizes.
//    error_format:# // 0 - success, 1 - , 2 - , 3 -
//    make_longpoll:long // if != 0, this request is added as a long poll request with ID
//    release_longpoll:long // if there is outstanding long poll request with this ID, return it
//      = String;

@any @internal engine.sleep time_ms:int = Bool;
@any @internal engine.asyncSleep time_ms:int = Bool;

@any engine.stat = Stat;
@any engine.filteredStat stat_names:%(Vector string) = Stat;
@any engine.pid = net.Pid;
@any engine.version = String;
@any @internal engine.setVerbosity verbosity:int = True;

@readwrite go.pprof params:string = String;

// definitions for invoke req 2
// we keep identical extra definition to save complexity costs
// it will contain unused 'flags' field in TL2 format, but this is not a big deal, maximum 1 byte wasted

---types---

// we decided to switch to rpcInokeReq2 later. For now, we use wrapped TL2 marker.

// bodyFormatTL1#d44fa8d0 = BodyFormat;
// bodyFormatTL2#c532c2a9 = BodyFormat;

// invokeReqHeader2#02b5c109 query_id:long actor_id:long extra:rpcInvokeReqExtra method:string format:BodyFormat = InvokeReqHeader2;
// request body follows after rpc2.invokeReqHeader
// for TL1, method field is optional, body contains method magic
// for TL2, correct method field is mandatory, body does not contain method identification information
// diagonal and other wrapped requests are not yet supported, they will be supported later

// reqResultHeader2#4d173f97 query_id:long extra:rpcReqResultExtra error_code:int error_text:string = ReqResultHeader2;
// response body follows after reqResultHeader2 (usually ignored if error_code != 0, but can contain custom error data in the future)

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package tlerrorcodes

const (
	Syntax       int32 = -1000 // TL_ERROR_SYNTAX
	ExtraData    int32 = -1001 // TL_ERROR_EXTRA_DATA
	Header       int32 = -1002 // TL_ERROR_HEADER
	WrongQueryID int32 = -1003 // TL_ERROR_WRONG_QUERY_ID

	NoHandler           int32 = -2000 // TL_ERROR_UNKNOWN_FUNCTION_ID
	ProxyNoTarget       int32 = -2001 // TL_ERROR_PROXY_NO_TARGET
	WrongActorID        int32 = -2002 // TL_ERROR_WRONG_ACTOR_ID
	TooLongString       int32 = -2003 // TL_ERROR_TOO_LONG_STRING
	ValueNotInRange     int32 = -2004 // TL_ERROR_VALUE_NOT_IN_RANGE
	QueryIncorrect      int32 = -2005 // TL_ERROR_QUERY_INCORRECT
	BadValue            int32 = -2006 // TL_ERROR_BAD_VALUE
	BinlogDisabled      int32 = -2007 // TL_ERROR_BINLOG_DISABLED
	FeatureDisabled     int32 = -2008 // TL_ERROR_FEATURE_DISABLED
	QueryIsEmpty        int32 = -2009 // TL_ERROR_QUERY_IS_EMPTY
	InvalidConnectionID int32 = -2010 // TL_ERROR_INVALID_CONNECTION_ID
	GracefulShutdown    int32 = -2014 // TL_ERROR_GRACEFUL_SHUTDOWN

	Timeout                  int32 = -3000 // TL_ERROR_QUERY_TIMEOUT
	ProxyInvalidResponse     int32 = -3001 // TL_ERROR_PROXY_INVALID_RESPONSE
	NoConnections            int32 = -3002 // TL_ERROR_NO_CONNECTIONS
	Internal                 int32 = -3003 // TL_ERROR_INTERNAL
	AIOFail                  int32 = -3004 // TL_ERROR_AIO_FAIL
	AIOTimeout               int32 = -3005 // TL_ERROR_AIO_TIMEOUT
	BinlogWaitTimeout        int32 = -3006 // TL_ERROR_BINLOG_WAIT_TIMEOUT
	AIOMaxRetryExceeded      int32 = -3007 // TL_ERROR_AIO_MAX_RETRY_EXCEEDED
	TTL                      int32 = -3008 // TL_ERROR_TTL
	OutOfMemory              int32 = -3009 // TL_ERROR_OUT_OF_MEMORY
	BadMetaFile              int32 = -3010 // TL_ERROR_BAD_METAFILE
	ResultToLarge            int32 = -3011 // TL_ERROR_RESULT_TOO_LARGE
	TooManyBadResponses      int32 = -3012 // TL_ERROR_TOO_MANY_BAD_RESPONSES
	FloodControl             int32 = -3013 // TL_ERROR_FLOOD_CONTROL
	CongestionControlDiscard int32 = -3017 // TL_ERROR_CONGESTION_CONTROL_DISCARD
	TooManyRequests          int32 = -3029 // TL_ERROR_TOO_MANY_REQUESTS

	Unknown                 int32 = -4000 // TL_ERROR_UNKNOWN
	ResponseSyntax          int32 = -4101 // TL_ERROR_RESPONSE_SYNTAX
	ResponseNotFound        int32 = -4102 // TL_ERROR_RESPONSE_NOT_FOUND
	TimeoutInRpcClient      int32 = -4103 // TL_ERROR_TIMEOUT_IN_RPC_CLIENT
	NoConnectionInRpcClient int32 = -4104 // TL_ERROR_NO_CONNECTIONS_IN_RPC_CLIENT
)

// rpc-error-codes.h
func IsUserError(errCode int32) bool {
	return errCode > -3000 && errCode <= -1000
}

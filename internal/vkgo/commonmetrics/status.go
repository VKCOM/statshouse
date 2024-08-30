// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package commonmetrics

import (
	"context"
	"errors"
	"strconv"

	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

const (
	StatusOk              = "ok"
	StatusOkPartial       = "ok_partial"
	StatusTimeout         = "timeout"
	StatusError           = "error"
	StatusErrorAccess     = "error_access"
	StatusErrorLimit      = "error_limit"
	StatusErrorBadRequest = "error_bad_request"
	StatusErrorNotFound   = "error_not_found"
	StatusErrorInternal   = "error_internal"
)

const (
	errCodeUnknown           = -4000
	errCodeQueryTimeout      = -3000
	errCodeNoConnections     = -3002
	errCodeInternal          = -3003
	errCodeFloodControl      = -3013
	errCodeUnknownFunctionID = -2000
	errCodeSyntax            = -1000
	errCodeExtraData         = -1001
	errCodeHeader            = -1002
	errCodeWrongQueryID      = -1003
)

type Status struct {
	Description, Code string
}

func StatusFromError(err error) Status {
	if err == nil {
		return Status{Description: StatusOk, Code: "0"}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Status{Description: StatusTimeout}
	}
	if errors.Is(err, context.Canceled) {
		return Status{Description: StatusTimeout}
	}
	if rpcError, ok := err.(*rpc.Error); ok {
		var status string
		switch code := rpcError.Code; code {
		case errCodeUnknownFunctionID:
			status = StatusErrorNotFound
		case errCodeQueryTimeout:
			status = StatusTimeout
		case errCodeNoConnections:
			status = StatusErrorInternal
		case errCodeInternal:
			status = StatusErrorInternal
		case errCodeUnknown:
			status = StatusError
		case errCodeFloodControl:
			status = StatusErrorLimit
		case errCodeSyntax, errCodeExtraData, errCodeHeader, errCodeWrongQueryID:
			status = StatusErrorBadRequest
		default:
			status = StatusError
		}
		return Status{Description: status, Code: strconv.Itoa(int(rpcError.Code))}
	}
	return Status{Description: StatusError}
}

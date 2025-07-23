// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package internal

import (
	"net/http"
	"strconv"

	"github.com/VKCOM/statshouse/internal/vkgo/commonmetrics"
)

func ParseHTTPStatusCode(code int) (status, statusCode string) {
	statusCode = strconv.Itoa(code)

	switch {
	case code < 200:
		status = commonmetrics.StatusOkPartial

	case code < 300:
		status = commonmetrics.StatusOk

	case code < 400:
		status = commonmetrics.StatusOkPartial

	case code == http.StatusRequestTimeout ||
		code == http.StatusGatewayTimeout:
		status = commonmetrics.StatusTimeout

	case code == http.StatusBadRequest ||
		code == http.StatusMethodNotAllowed ||
		code == http.StatusNotAcceptable ||
		code == http.StatusTeapot:
		status = commonmetrics.StatusErrorBadRequest

	case code == http.StatusUnauthorized ||
		code == http.StatusPaymentRequired ||
		code == http.StatusForbidden ||
		code == http.StatusProxyAuthRequired ||
		code == http.StatusUnavailableForLegalReasons ||
		code == http.StatusNetworkAuthenticationRequired:
		status = commonmetrics.StatusErrorAccess

	case code == http.StatusNotFound ||
		code == http.StatusGone:
		status = commonmetrics.StatusErrorNotFound

	case code == http.StatusTooManyRequests:
		status = commonmetrics.StatusErrorLimit

	case code >= 500:
		status = commonmetrics.StatusErrorInternal

	default:
		status = commonmetrics.StatusError
	}

	return
}

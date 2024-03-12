// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/promql"
)

const (
	MaxSeriesRows = 10_000_000
)

var errTooManyRows = fmt.Errorf("can't fetch more than %v rows", MaxSeriesRows)

func HttpErr(code int, err error) HttpError {
	return HttpError{
		Code: code,
		Err:  err,
	}
}

type HttpError struct {
	Code int
	Err  error
}

func (e HttpError) Error() string {
	return e.Err.Error()
}

func (e HttpError) Unwrap() error {
	return e.Err
}

func HttpCode(err error) int {
	code := http.StatusOK
	if err != nil {
		var httpErr HttpError
		var promErr promql.Error
		switch {
		case errors.Is(err, data_model.ErrEntityNotExists):
			code = http.StatusNotFound
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			code = http.StatusGatewayTimeout // 504
		case errors.As(err, &httpErr):
			code = httpErr.Code
		case errors.Is(err, errTooManyRows):
			code = http.StatusBadRequest
		case errors.As(err, &promErr):
			if promErr.EngineFailure() {
				code = http.StatusInternalServerError
			} else {
				code = http.StatusBadRequest
			}
		default:
			code = http.StatusInternalServerError // 500
		}
	}
	return code
}

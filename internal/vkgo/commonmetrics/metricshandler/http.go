// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metricshandler

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/commonmetrics"
	"github.com/vkcom/statshouse/internal/vkgo/commonmetrics/internal"
)

var (
	_ http.ResponseWriter = &responseWriterWrapper{}
	_ http.Hijacker       = &responseWriterWrapper{}
	_ http.Flusher        = &responseWriterWrapper{}
)

type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
	errorOnWrite bool
}

func (w *responseWriterWrapper) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *responseWriterWrapper) Write(p []byte) (int, error) {
	w.bytesWritten += len(p)
	n, err := w.ResponseWriter.Write(p)
	w.errorOnWrite = err != nil
	return n, err
}

func (w *responseWriterWrapper) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("http.ResponseWriter not a http.Hijacker")
	}
	return h.Hijack()
}

func (w *responseWriterWrapper) Flush() {
	f, ok := w.ResponseWriter.(http.Flusher)
	if !ok {
		return
	}

	f.Flush()
}

func WrapHttpHandlerFunc(h http.HandlerFunc, method commonmetrics.Method) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		wi := &responseWriterWrapper{
			statusCode:     http.StatusOK,
			ResponseWriter: w,
		}
		start := time.Now()
		defer func() {
			httpStatusCode := wi.statusCode
			r := recover()
			if r != nil {
				httpStatusCode = http.StatusInternalServerError
			}

			var status, statusCode string
			if wi.errorOnWrite {
				status = commonmetrics.StatusError
			} else {
				status, statusCode = internal.ParseHTTPStatusCode(httpStatusCode)
			}

			inReq := InputRequest{
				Method:     method,
				Protocol:   commonmetrics.ProtocolHTTP,
				Status:     status,
				StatusCode: statusCode,
			}
			ResponseTime(inReq, time.Since(start))
			ResponseSize(inReq, wi.bytesWritten)
			RequestSize(inReq, int(req.ContentLength))

			if r != nil {
				panic(r)
			}
		}()
		h(wi, req)
	}
}

func WrapHttpHandler(h http.Handler, method commonmetrics.Method) http.Handler {
	return WrapHttpHandlerFunc(h.ServeHTTP, method)
}

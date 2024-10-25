// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mailru/easyjson/jwriter"

	"github.com/vkcom/statshouse/internal/data_model"

	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql"
)

const (
	cacheMinAgeSeconds = 1
	cacheMaxAgeSeconds = 31536000

	widthAutoRes = 0
	widthLODRes  = 1
)

func httpErr(code int, err error) httpError {
	return httpError{
		code: code,
		err:  err,
	}
}

type httpError struct {
	code int
	err  error
}

func (e httpError) Error() string {
	return e.err.Error()
}

func (e httpError) Unwrap() error {
	return e.err
}

//easyjson:json
type Response struct {
	Data  interface{} `json:"data,omitempty"`
	Error string      `json:"error,omitempty"`
}

func httpCode(err error) int {
	code := http.StatusOK
	if err != nil {
		var httpErr httpError
		var promErr promql.Error
		switch {
		case errors.Is(err, data_model.ErrEntityNotExists):
			code = http.StatusNotFound
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			code = http.StatusGatewayTimeout // 504
		case errors.As(err, &httpErr):
			code = httpErr.code
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

func cacheSeconds(d time.Duration) int {
	s := int(d.Seconds() + 0.5)
	if s < cacheMinAgeSeconds {
		s = cacheMinAgeSeconds
	}
	if s > cacheMaxAgeSeconds {
		s = cacheMaxAgeSeconds
	}
	return s
}

func exportCSV(w *HTTPRequestHandler, resp *SeriesResponse, metric string) {
	w.endpointStat.reportServiceTime(http.StatusOK, nil)

	w.Header().Set(
		"Content-Disposition",
		fmt.Sprintf("attachment; filename=%s-%s.csv", metric, time.Now().Format("2006-01-02")),
	)
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	writer := csv.NewWriter(w)
	writer.Comma = ';'
	defer writer.Flush()

	err := writer.Write([]string{"Date", "Value", "Label"})
	if err != nil {
		log.Printf("[error] failed to write CSV header: %v", err)
		return
	}

	uniqueWhat := make(map[string]struct{})
	for _, meta := range resp.Series.SeriesMeta {
		uniqueWhat[meta.What] = struct{}{}
	}

	for li, data := range resp.Series.SeriesData {
		if data == nil {
			continue
		}

		label := MetaToLabel(resp.Series.SeriesMeta[li], len(uniqueWhat), 0)
		for di, p := range *data {
			if math.IsNaN(p) {
				continue
			}

			err := writer.Write([]string{
				time.Unix(resp.Series.Time[di], 0).Format("2006-01-02 15:04:05"),
				strconv.FormatFloat(p, 'f', -1, 64),
				label,
			})
			if err != nil {
				log.Printf("[error] failed to write CSV string: %v", err)
				return
			}
		}
	}
}

func respondJSON(w *HTTPRequestHandler, resp interface{}, cache time.Duration, cacheStale time.Duration, err error) {
	code := httpCode(err)
	r := Response{}

	w.endpointStat.reportServiceTime(code, nil)

	if err != nil {
		if code == 500 {
			log.Println("[error]", err.Error())
		}
		r.Error = err.Error()
	} else {
		r.Data = resp
	}
	start := time.Now()
	var jw jwriter.Writer
	r.MarshalEasyJSON(&jw)
	if jw.Error != nil {
		log.Printf("[error] failed to marshal JSON response for %q: %v", w.endpointStat.user, jw.Error)
		msg := `{"error": "failed to marshal JSON response"}`
		w.Header().Set("Content-Length", strconv.Itoa(len(msg)))
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(ServerTimingHeaderKey, w.endpointStat.timings.String())
		code = http.StatusInternalServerError
		w.WriteHeader(code)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Printf("[error] failed to write HTTP response for %q: %v", w.endpointStat.user, err)
		}
	} else {
		size := jw.Size()
		if w.verbose {
			log.Printf("[debug] serialized %v bytes of JSON for %q in %v", size, w.endpointStat.user, time.Since(start))
		}
		w.Header().Set("Content-Length", strconv.Itoa(size))
		w.Header().Set("Content-Type", "application/json")
		if code == http.StatusOK {
			switch {
			case cache == 0:
				w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			case cacheStale == 0:
				w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", cacheSeconds(cache)))
			default:
				w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d, stale-while-revalidate=%d", cacheSeconds(cache), cacheSeconds(cacheStale)))
			}
		}
		w.Header().Set(ServerTimingHeaderKey, w.endpointStat.timings.String())
		w.WriteHeader(code)
		start := time.Now()
		_, err := jw.DumpTo(w)
		if w.verbose && err == nil {
			log.Printf("[debug] dumped %v bytes of JSON for %q in %v", size, w.endpointStat.user, time.Since(start))
		}
		if err != nil {
			log.Printf("[error] failed to write HTTP response for %q: %v", w.endpointStat.user, err)
		}
	}
}

func respondPlot(w *HTTPRequestHandler, format string, resp []byte, cache time.Duration, cacheStale time.Duration, user string) {
	code := http.StatusOK
	w.endpointStat.reportServiceTime(code, nil)
	w.Header().Set(ServerTimingHeaderKey, w.endpointStat.timings.String())

	w.Header().Set("Content-Length", strconv.Itoa(len(resp)))
	switch format {
	case dataFormatPNG:
		w.Header().Set("Content-Type", "image/png")
	case dataFormatSVG:
		w.Header().Set("Content-Type", "image/svg+xml; charset=utf-8")
	case dataFormatText:
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	switch {
	case cache == 0:
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	case cacheStale == 0:
		w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", cacheSeconds(cache)))
	default:
		w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d, stale-while-revalidate=%d", cacheSeconds(cache), cacheSeconds(cacheStale)))
	}
	w.WriteHeader(code)

	start := time.Now()
	_, err := w.Write(resp)
	if w.verbose && err == nil {
		log.Printf("[debug] dumped %v bytes of %s render for %q in %v", len(resp), format, user, time.Since(start))
	}
	if err != nil {
		log.Printf("[error] failed to write HTTP response for %q: %v", user, err)
	}
}

func parseFromTo(fromTS string, toTS string) (from time.Time, to time.Time, err error) {
	fromN, err := strconv.ParseInt(fromTS, 10, 64)
	if err != nil {
		return time.Time{}, time.Time{}, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse UNIX timestamp: %w", err))
	}
	toN, err := strconv.ParseInt(toTS, 10, 64)
	if err != nil {
		return time.Time{}, time.Time{}, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse UNIX timestamp: %w", err))
	}
	to, err = parseUnixTimeTo(toN)
	if err != nil {
		return time.Time{}, time.Time{}, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse UNIX timestamp: %w", err))
	}
	from, err = parseUnixTimeFrom(fromN, to)
	if err != nil {
		return time.Time{}, time.Time{}, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse UNIX timestamp: %w", err))
	}
	if to.Before(from) {
		err = httpErr(http.StatusBadRequest, fmt.Errorf("%q %v is before %q %v", ParamToTime, to, ParamFromTime, from))
	}
	return
}

func parseUnixTimeFrom(u int64, to time.Time) (time.Time, error) {
	if u <= 0 {
		return to.Add(time.Duration(u) * time.Second), nil
	}

	return time.Unix(u, 0).UTC(), nil
}

func parseUnixTimeTo(u int64) (time.Time, error) {
	if u <= 0 {
		return time.Now().UTC().Add(time.Duration(u) * time.Second), nil
	}
	return time.Unix(u, 0).UTC(), nil
}

func parseWidth(w string, g string) (int, int, error) {
	if w == "1M" {
		return _1M, widthLODRes, nil
	}

	const lodSuffix = "s"

	s := w
	widthKind := widthAutoRes
	if g != "" {
		s = g
		widthKind = widthLODRes
	} else if strings.HasSuffix(w, lodSuffix) {
		s = w[:len(w)-len(lodSuffix)]
		widthKind = widthLODRes
	}

	u, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse width: %w", err))
	}
	if u < 0 {
		return 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse width: must be >= 0"))
	}

	return int(u), widthKind, nil
}

func parseTimeShifts(ts []string) ([]time.Duration, error) {
	ds := make([]int64, 0, len(ts))
	for _, s := range ts {
		d, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse time shift %q: %w", s, err))
		}
		ds = append(ds, d)
	}
	return verifyTimeShifts(ds)

}

func verifyTimeShifts(ts []int64) ([]time.Duration, error) {
	ds := []time.Duration{0} // implicit 0s
	for _, d := range ts {
		if d >= 0 {
			return nil, httpErr(http.StatusBadRequest, fmt.Errorf("time shift %d is not negative", d))
		}
		ds = append(ds, time.Duration(d)*time.Second)
	}
	if len(ds) > maxTimeShifts {
		return nil, httpErr(http.StatusBadRequest, fmt.Errorf("too many time shifts specified (%v, max=%v)", len(ds), maxTimeShifts))
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	return ds, nil
}

func parseNumResults(s string, max int) (int, error) {
	u, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse number of results: %w", err))
	}

	n := int(u)
	if n < 0 {
		if -n > max {
			n = -max
		}
	} else if n > max {
		n = max
	}

	return n, nil
}

func parseTagID(tagID string) (string, error) {
	res, err := format.APICompatNormalizeTagID(tagID)
	if err == nil {
		return res, nil
	}
	return "", httpErr(http.StatusBadRequest, err)
}

func parseVersion(s string) (string, error) {
	if s == "" {
		return Version1, nil
	}

	for _, v := range []string{Version1, Version2, Version3} {
		if s == v {
			return v, nil
		}
	}

	return "", httpErr(http.StatusBadRequest, fmt.Errorf("invalid version: %q", s))
}

func parseRenderWidth(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	u, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse render width: %w", err))
	}
	if u < 0 {
		return 0, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse render width: must be >= 0"))
	}
	return int(u), nil
}

func parseRenderFormat(s string) (string, error) {
	switch s {
	case "":
		return dataFormatPNG, nil
	case dataFormatPNG:
		return dataFormatPNG, nil
	case dataFormatSVG:
		return dataFormatSVG, nil
	case dataFormatText:
		return dataFormatText, nil
	default:
		return "", httpErr(http.StatusBadRequest, fmt.Errorf("unknown render format %q", s))
	}
}

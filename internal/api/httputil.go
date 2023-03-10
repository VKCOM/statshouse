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

	"github.com/vkcom/statshouse/internal/format"
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
		switch {
		case errors.As(err, &httpErr):
			code = httpErr.code
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			code = http.StatusGatewayTimeout // 504
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

func exportCSV(w http.ResponseWriter, resp *GetQueryResp, metric string, es *endpointStat) {
	es.serviceTime(http.StatusOK)
	defer es.responseTime(http.StatusOK)

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

	uniqueWhat := make(map[queryFn]struct{})
	for _, meta := range resp.Series.SeriesMeta {
		uniqueWhat[meta.What] = struct{}{}
	}

	for li, data := range resp.Series.SeriesData {
		if data == nil {
			continue
		}

		label := MetaToLabel(resp.Series.SeriesMeta[li], len(uniqueWhat))
		for di, p := range *data {
			if math.IsNaN(p) {
				continue
			}

			err := writer.Write([]string{
				time.Unix(resp.Series.Time[di], 0).Format("2006-01-02 15:04:05"),
				strconv.FormatFloat(p, 'f', 2, 64),
				label,
			})
			if err != nil {
				log.Printf("[error] failed to write CSV string: %v", err)
				return
			}
		}
	}
}

func respondJSON(w http.ResponseWriter, resp interface{}, cache time.Duration, cacheStale time.Duration, err error, verbose bool, user string, es *endpointStat) {
	code := httpCode(err)
	r := Response{}

	if es != nil {
		es.serviceTime(code)
	}

	if err != nil {
		r.Error = err.Error()
	} else {
		r.Data = resp
	}
	start := time.Now()
	var jw jwriter.Writer
	r.MarshalEasyJSON(&jw)
	if jw.Error != nil {
		log.Printf("[error] failed to marshal JSON response for %q: %v", user, jw.Error)
		msg := `{"error": "failed to marshal JSON response"}`
		w.Header().Set("Content-Length", strconv.Itoa(len(msg)))
		w.Header().Set("Content-Type", "application/json")
		code = http.StatusInternalServerError
		w.WriteHeader(code)
		_, err := w.Write([]byte(msg))
		if err != nil {
			log.Printf("[error] failed to write HTTP response for %q: %v", user, err)
		}
	} else {
		size := jw.Size()
		if verbose {
			log.Printf("[debug] serialized %v bytes of JSON for %q in %v", size, user, time.Since(start))
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
		w.WriteHeader(code)
		start := time.Now()
		_, err := jw.DumpTo(w)
		if verbose && err == nil {
			log.Printf("[debug] dumped %v bytes of JSON for %q in %v", size, user, time.Since(start))
		}
		if err != nil {
			log.Printf("[error] failed to write HTTP response for %q: %v", user, err)
		}
	}

	if es != nil {
		es.responseTime(code)
	}
}

func respondPlot(w http.ResponseWriter, format string, resp []byte, cache time.Duration, cacheStale time.Duration, verbose bool, user string, es *endpointStat) {
	code := http.StatusOK
	if es != nil {
		es.serviceTime(code)
	}

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
	if verbose && err == nil {
		log.Printf("[debug] dumped %v bytes of %s render for %q in %v", len(resp), format, user, time.Since(start))
	}
	if err != nil {
		log.Printf("[error] failed to write HTTP response for %q: %v", user, err)
	}

	if es != nil {
		es.responseTime(code)
	}
}

func parseFromTo(fromTS string, toTS string) (from time.Time, to time.Time, err error) {
	from, err = parseUnixTime(fromTS)
	if err != nil {
		return
	}
	to, err = parseUnixTime(toTS)
	if err != nil {
		return
	}
	if to.Before(from) {
		err = httpErr(http.StatusBadRequest, fmt.Errorf("%q %v is before %q %v", ParamToTime, to, ParamFromTime, from))
	}
	return
}

func parseUnixTime(s string) (time.Time, error) {
	u, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse UNIX timestamp: %w", err))
	}
	if u < 0 {
		return time.Time{}, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse UNIX timestamp: must be >= 0"))
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

func parseTimeShifts(ts []string, width int) ([]time.Duration, error) {
	ds := []time.Duration{0} // implicit 0s
	for _, s := range ts {
		d, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse time shift %q: %w", s, err))
		}
		if d >= 0 {
			return nil, httpErr(http.StatusBadRequest, fmt.Errorf("time shift %q is not negative", s))
		}
		if width == _1M && d%_1M != 0 {
			return nil, httpErr(http.StatusBadRequest, fmt.Errorf("time shift %q can't be used with month interval", s))
		}
		ds = append(ds, time.Duration(d)*time.Second)
	}
	if len(ds) > maxTimeShifts {
		return nil, httpErr(http.StatusBadRequest, fmt.Errorf("too many time shifts specified (%v, max=%v)", len(ds), maxTimeShifts))
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	return ds, nil
}

func parseNumResults(s string, def int, max int, isNegativeAllowed bool) (int, error) {
	if s == "" || s == "0" {
		return def, nil
	}

	u, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, httpErr(http.StatusBadRequest, fmt.Errorf("failed to parse number of results: %w", err))
	}
	if !isNegativeAllowed && u < 0 {
		return 0, httpErr(http.StatusBadRequest, fmt.Errorf("negative number of results isn't allowed: %v", u))
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
	if tagID == format.StringTopTagID || format.ParseTagIDForAPI(tagID) >= 0 {
		return tagID, nil
	}
	if i, err := strconv.Atoi(tagID); err == nil && 0 <= i && i < format.MaxTags {
		return format.TagID(i), nil
	}
	return "", httpErr(http.StatusBadRequest, fmt.Errorf("invalid tag ID: %q", tagID))
}

func parseVersion(s string) (string, error) {
	if s == "" {
		return Version1, nil
	}

	for _, v := range []string{Version1, Version2} {
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

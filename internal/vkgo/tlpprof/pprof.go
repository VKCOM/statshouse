// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package tlpprof

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"time"
)

// Most code is ported from the https://golang.org/src/net/http/pprof/pprof.go.
//
// TODO(i.sharipov): move to a separate package.

// Index responds with the pprof-formatted profile named by the request.
// For example, "/debug/pprof/heap" serves the "heap" profile.
// Index responds to a request for "/debug/pprof/" with an HTML page
// listing the available profiles (when the name is "").
func Index(w io.Writer, name string, u *url.URL) error {
	if name != "" {
		ctx := context.TODO()
		return handler(name).Serve(ctx, w, u)
	}

	// TODO(i.sharipov): index page contents?
	return nil
}

// Profile responds with the pprof-formatted cpu profile.
// Profiling lasts for duration specified in seconds GET parameter, or for 30 seconds if not specified.
// URL is /debug/pprof/profile.
func Profile(w io.Writer, u *url.URL) error {
	sec, err := strconv.ParseInt(u.Query().Get("seconds"), 10, 64)
	if sec <= 0 || err != nil {
		sec = 30
	}

	if err := pprof.StartCPUProfile(w); err != nil {
		return fmt.Errorf("could not enable CPU profiling: %w", err)
	}
	defer pprof.StopCPUProfile()

	// TODO: handle clientGone?
	time.Sleep(time.Duration(sec) * time.Second)

	return nil
}

// Cmdline responds with the running program's
// command line, with arguments separated by NUL bytes.
// URL is /debug/pprof/cmdline.
func Cmdline(w io.Writer) {
	_, _ = io.WriteString(w, strings.Join(os.Args, "\x00"))
}

// Trace responds with the execution trace in binary form.
// Tracing lasts for duration specified in seconds GET parameter, or for 1 second if not specified.
// URL is /debug/pprof/trace.
func Trace(w io.Writer, u *url.URL) error {
	sec, err := strconv.ParseFloat(u.Query().Get("seconds"), 64)
	if sec <= 0 || err != nil {
		sec = 1
	}

	if err := trace.Start(w); err != nil {
		return fmt.Errorf("could not enable tracing: %w", err)
	}
	defer trace.Stop()

	// TODO: handle clientGone?
	time.Sleep(time.Duration(sec * float64(time.Second)))

	return nil
}

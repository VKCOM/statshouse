// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package tlpprof

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/tlpprof/profile"
)

var profileSupportsDelta = map[handler]bool{
	"allocs":       true,
	"block":        true,
	"goroutine":    true,
	"heap":         true,
	"mutex":        true,
	"threadcreate": true,
}

type handler string

func (name handler) Serve(ctx context.Context, w io.Writer, u *url.URL) error {
	p := pprof.Lookup(string(name))

	if p == nil {
		return fmt.Errorf("unknown profile: %s", name)
	}

	if sec := u.Query().Get("seconds"); sec != "" {
		return name.serveDeltaProfile(ctx, w, u, p, sec)
	}

	gc, _ := strconv.Atoi(u.Query().Get("gc"))
	if name == "heap" && gc > 0 {
		runtime.GC()
	}

	debug, _ := strconv.Atoi(u.Query().Get("debug"))

	return p.WriteTo(w, debug)
}

func (name handler) serveDeltaProfile(ctx context.Context, w io.Writer, u *url.URL, p *pprof.Profile, secStr string) error {
	sec, err := strconv.ParseInt(secStr, 10, 64)
	if err != nil || sec <= 0 {
		return fmt.Errorf(`invalid value for "seconds": %v`, err)
	}

	if !profileSupportsDelta[name] {
		return errors.New(`"seconds" parameter is not supported for this profile type`)
	}

	// 'name' should be a key in profileSupportsDelta.

	debug, _ := strconv.Atoi(u.Query().Get("debug"))
	if debug != 0 {
		return errors.New("seconds and debug params are incompatible")
	}

	p0, err := collectProfile(p)
	if err != nil {
		return fmt.Errorf("failed to collect p0: %w", err)
	}

	t := time.NewTimer(time.Duration(sec) * time.Second)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	}

	p1, err := collectProfile(p)
	if err != nil {
		return fmt.Errorf("failed to collect p1: %w", err)
	}

	ts := p1.TimeNanos
	dur := p1.TimeNanos - p0.TimeNanos
	p0.Scale(-1)
	p1, err = profile.Merge([]*profile.Profile{p0, p1})
	if err != nil {
		return fmt.Errorf("failed to compute delta: %w", err)
	}

	p1.TimeNanos = ts // set since we don't know what profile.Merge set for TimeNanos.
	p1.DurationNanos = dur
	return p1.Write(w)
}

func collectProfile(p *pprof.Profile) (*profile.Profile, error) {
	var buf bytes.Buffer
	if err := p.WriteTo(&buf, 0); err != nil {
		return nil, err
	}
	ts := time.Now().UnixNano()
	p0, err := profile.Parse(&buf)
	if err != nil {
		return nil, err
	}
	p0.TimeNanos = ts
	return p0, nil
}

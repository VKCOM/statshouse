// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tltracing"
)

func TestErrorTag(t *testing.T) {
	foo := fmt.Errorf("foo")
	bar := fmt.Errorf("foo: %w", foo)
	baz := &tagError{tag: "baz", err: bar}
	bzz := fmt.Errorf("bzz: %w", baz)
	tag := ErrorTag(bzz)
	assert.Equal(t, tag, "baz:foo")
	assert.Empty(t, ErrorTag(nil))
	assert.Empty(t, ErrorTag(&tagError{tag: ""}))
	assert.Equal(t, ErrorTag(&tagError{tag: "", err: &net.OpError{Err: context.DeadlineExceeded}}), "timeout")
}

func TestTraceID(t *testing.T) {
	// case by case test is enough here
	tEmpty := tltracing.TraceID{}
	str := TraceIDToString(tEmpty)
	assert.Equal(t, str, "")
	t1, err := TraceIDFromString(str)
	assert.NoError(t, err)
	assert.Equal(t, t1, tEmpty)

	t0 := tltracing.TraceID{
		Lo: 0x0123456789abcdef,
		Hi: 0x123456789abcdef0,
	}
	t0strCanonical := "123456789abcdef00123456789abcdef"
	t0strUppercase := "123456789ABCDEF00123456789ABCDEF"
	t0strMixed := "123456789ABCDEF00123456789abcdef"
	str = TraceIDToString(t0)
	assert.Equal(t, str, t0strCanonical)

	t1, err = TraceIDFromString(t0strCanonical)
	assert.NoError(t, err)
	assert.Equal(t, t1, t0)
	t1, err = TraceIDFromString(t0strUppercase)
	assert.NoError(t, err)
	assert.Equal(t, t1, t0)
	t1, err = TraceIDFromString(t0strMixed)
	assert.NoError(t, err)
	assert.Equal(t, t1, t0)

	t1, err = TraceIDFromString("hren") // wrong length
	assert.Error(t, err)
	assert.Equal(t, t1, tEmpty)
	t1, err = TraceIDFromString("G0123456789ABCDE0123456789ABCDEF") // wrong char
	assert.Error(t, err)
	assert.Equal(t, t1, tEmpty)
}

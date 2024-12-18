// Copyright 2024 V Kontakte LLC
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

// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetAlignedBuffer(t *testing.T) {
	assert.Equal(t, 4, len(getAlignedBuffer(make([]byte, 4))))
	assert.Equal(t, 4, len(getAlignedBuffer(make([]byte, 5))))
	assert.Equal(t, 4, len(getAlignedBuffer(make([]byte, 6))))
	assert.Equal(t, 4, len(getAlignedBuffer(make([]byte, 7))))
	assert.Equal(t, 8, len(getAlignedBuffer(make([]byte, 8))))
	assert.Equal(t, 8, len(getAlignedBuffer(make([]byte, 9))))
}

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rowbinary

import (
	"bytes"
	"testing"

	pkgUUID "github.com/google/uuid"
)

func Test_swapBytesForUUID(t *testing.T) {
	src := pkgUUID.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	dst := pkgUUID.UUID{8, 7, 6, 5, 4, 3, 2, 1, 16, 15, 14, 13, 12, 11, 10, 9}

	swapBytesForUUID(src[:])

	if !bytes.Equal(src[:], dst[:]) {
		t.Fatalf(`Expect %v got %v`, dst, src)
	}
}

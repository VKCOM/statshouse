// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package mem

import (
	"bytes"
	"testing"

	"pgregory.net/rapid"
)

func TestToByteSlice(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		s := rapid.String().Draw(t, "s")
		b := []byte(s)
		ub := toByteSlice(s)

		if !bytes.Equal(ub, b) {
			t.Fatalf("got %q instead of %q", ub, b)
		}
	})
}

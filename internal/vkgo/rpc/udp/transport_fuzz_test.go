// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"testing"
)

// standard go approach
func FuzzTransportWithoutRestarts(f *testing.F) {
	f.Fuzz(func(t *testing.T, commands []byte) {
		_ = FuzzDyukov(commands, false)
	})
}

// standard go approach
func FuzzTransportWithRestarts(f *testing.F) {
	f.Fuzz(func(t *testing.T, commands []byte) {
		_ = FuzzDyukov(commands, true)
	})
}

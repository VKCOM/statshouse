// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package algo

func CompareStrings(x, y string) int {
	// TODO: make some unsafe magic to speed up?
	if x < y {
		return -1
	}
	if x > y {
		return 1
	}
	return 0
}

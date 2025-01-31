// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build cgo && asan && linux && (arm64 || amd64 || riscv64 || ppc64le)

package lsan

/*
#include <sanitizer/lsan_interface.h>
*/
import "C"
import "os"

func ExitOnLeaks() {
	leak := C.__lsan_do_recoverable_leak_check() // does not seem to work under clang
	if leak == 1 {
		os.Exit(23) // default LSan exit code
	} else {
		C.__lsan_do_leak_check() // does not seem to work under GCC, *unless* we do a recoverable check first
	}
}

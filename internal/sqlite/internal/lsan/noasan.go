// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !(cgo && asan && linux && (arm64 || amd64 || riscv64 || ppc64le))

package lsan

func ExitOnLeaks() {}
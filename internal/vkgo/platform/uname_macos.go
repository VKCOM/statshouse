// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build darwin

package platform

import (
	"bytes"
	"os/exec"
)

var (
	unameGot bool
	uname    string
)

func Uname() (string, error) {
	if unameGot {
		return uname, nil
	}

	out, err := exec.Command(`uname`, `-n`).Output()
	if err != nil {
		return ``, err
	}

	uname = string(bytes.TrimSpace(out))
	unameGot = true

	return uname, nil
}

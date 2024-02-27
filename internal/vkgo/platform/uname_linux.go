// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build linux

package platform

import (
	"fmt"
	"os"
	"syscall"
)

var (
	unameGot bool
	uname    string
)

func Uname() (string, error) {
	if unameGot {
		return uname, nil
	}

	var utsname syscall.Utsname
	if err := syscall.Uname(&utsname); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, `Could not get uname: %s`, err.Error())
		return ``, err
	}

	var buf []byte
	for _, ch := range utsname.Nodename {
		if ch == 0 {
			break
		}
		buf = append(buf, byte(ch))
	}

	uname = string(buf)
	unameGot = true

	return uname, nil
}

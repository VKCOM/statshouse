// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package util

import (
	"net"
)

func ListenFirstAvailablePort(address string, numberOfProbes int) (net.Listener, error) {
	l, err := net.Listen("tcp", address)
	if err == nil {
		return l, nil
	}
	if numberOfProbes <= 0 {
		return nil, err
	}
	a, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}
	for i := 0; i < numberOfProbes; i++ {
		a.Port++
		if l, err = net.Listen("tcp", a.String()); err == nil {
			return l, nil
		}
	}
	return nil, err
}

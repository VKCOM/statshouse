// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"net"
	"sync"
)

type closeOnceListener struct {
	net.Listener

	closeOnce sync.Once
	closeErr  error
}

func (ln *closeOnceListener) Close() error {
	ln.closeOnce.Do(func() {
		ln.closeErr = ln.Listener.Close()
	})
	return ln.closeErr
}

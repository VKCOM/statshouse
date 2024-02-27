// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

type InvokeReqExtra struct {
	tl.RpcInvokeReqExtra

	FailIfNoConnection bool // Experimental. Not serialized. Requests fail immediately when connection fails, so that switch to fallback is faster
}

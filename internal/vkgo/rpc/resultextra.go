// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

type ReqResultExtra = tl.RpcReqResultExtra

// TODO - we added EpochNumber to bit 27 with backward compatibility trick
// if bit is set, we read long number,
//     if it has top bit set, we clear bit, treat value as EpochNumber, then read ViewNumber
//     else we treat this value as ViewNumber and set EpochNumber to 0

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

// Lifecycle of call context:
// After setupCall, context is owned both by 'calls' and 'writeQ': sent=false, stale=false
//    If cancelCall is made in this state, than context is owned by 'writeQ' only: sent=false, stale=true
//    In this case, context is freed during the next acquireBuffer, otherwise
// After acquireBuffer, context is owned by 'calls' only: sent=true, stale=false
//    If cancelCall is made in this case, than context is freed and removed from 'calls'.
// After finishCall, context is (shortly) owned by receiver goroutine, then owned by result channel
//    cancelCall is NOP in this state
//    If receiver reads context from channel, it must pass it to putCallContext
//    If receiver does not read from channel, context is GC-ed with the channel

func (resp *Response) deliverResult(c *ClientImpl) {
	cb := resp.cb
	if cb == nil {
		resp.result <- resp // cctx owned by channel
		return
	}
	// TODO - catch panic in callback, write to rare log
	cb(c, resp, resp.err)
}

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import "github.com/vkcom/statshouse/internal/vkgo/basictl"

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

type callContext struct {
	queryID            int64
	failIfNoConnection bool // set in setupCall call and never changes. Allows to quickly try multiple servers without waiting timeout

	sent  bool
	stale bool // Was cancelled before sending. Instead of removing from the write queue, we set the flag

	singleResult chan *callContext // channel for single caller is reused here
	result       chan *callContext // can point to singleResult or multiResult if used by MultiClient

	// Response
	resp *Response // if set, has priority
	err  error     // otherwise err must be set. May point to rpcErr

	hooksState any
}

func (cctx *callContext) parseResponse(resp *Response, toplevelError bool, logf LoggerFunc) (err error) {
	if toplevelError {
		var rpcErr Error
		if resp.Body, err = basictl.IntRead(resp.Body, &rpcErr.Code); err != nil {
			return err
		}
		if resp.Body, err = basictl.StringRead(resp.Body, &rpcErr.Description); err != nil {
			return err
		}
		return rpcErr
	}

	var tag uint32
	var afterTag []byte
	extraSet := 0
	for {
		if afterTag, err = basictl.NatRead(resp.Body, &tag); err != nil {
			return err
		}
		if tag != reqResultHeaderTag {
			break
		}
		var extra ReqResultExtra
		if resp.Body, err = extra.Read(afterTag); err != nil {
			return err
		}
		if extraSet == 0 {
			resp.Extra = extra
		}
		extraSet++
	}
	if extraSet > 1 {
		logf("rpc: ResultExtra set more than once (%d) for result tag #%08d; please report to infrastructure team", extraSet, tag)
	}

	if tag == reqResultErrorTag {
		var unused int64 // excess query_id erroneously saved by incorrect serialization of RpcReqResult object tree
		if afterTag, err = basictl.LongRead(afterTag, &unused); err != nil {
			return err
		}
	}
	if tag == reqResultErrorTag || tag == reqResultErrorWrappedTag {
		var rpcErr Error
		if resp.Body, err = basictl.IntRead(afterTag, &rpcErr.Code); err != nil {
			return err
		}
		if resp.Body, err = basictl.StringRead(resp.Body, &rpcErr.Description); err != nil {
			return err
		}
		return rpcErr
	}
	cctx.resp = resp
	return nil
}

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

const MaxTestResponseSize = 10 << 20
const MinTestResponseSize = 10

const MaxTestResponseTimeoutSec = 86400

var testResponse = make([]byte, MaxTestResponseSize)

func init() {
	_, _ = rand.New().Read(testResponse)
}

type TestConnection struct {
	clientsMu             sync.Mutex // Always taken after mu
	testConnectionClients map[*rpc.HandlerContext]tlstatshouse.TestConnection2Bytes
}

func MakeTestConnection() *TestConnection {
	result := &TestConnection{testConnectionClients: map[*rpc.HandlerContext]tlstatshouse.TestConnection2Bytes{}}
	go result.goRun()
	return result
}

func (ms *TestConnection) goRun() {
	for {
		time.Sleep(time.Second)
		ms.broadcastResponses()
	}
}

func (ms *TestConnection) CancelHijack(hctx *rpc.HandlerContext) {
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	delete(ms.testConnectionClients, hctx)
}

func (ms *TestConnection) broadcastResponses() {
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	now := time.Now()
	for hctx, args := range ms.testConnectionClients {
		if now.Sub(hctx.RequestTime) < time.Duration(args.ResponseTimeoutSec)*time.Second { // still waiting, copy to start of array
			continue
		}
		delete(ms.testConnectionClients, hctx)
		var err error
		hctx.Response, err = args.WriteResult(hctx.Response, testResponse[:args.ResponseSize])
		hctx.SendHijackedResponse(err)
	}
}

func (ms *TestConnection) handleTestConnection(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.TestConnection2Bytes) (err error) {
	if args.ResponseSize > MaxTestResponseSize {
		return fmt.Errorf("max supported response_size is %d", MaxTestResponseSize)
	}
	if args.ResponseTimeoutSec > MaxTestResponseTimeoutSec {
		return fmt.Errorf("max supported response_timeout_sec is %d", MaxTestResponseTimeoutSec)
	}
	if args.ResponseTimeoutSec <= 0 {
		var buf []byte
		if args.ResponseSize >= MinTestResponseSize {
			buf = binary.AppendVarint(buf, time.Now().UnixNano())
			args.ResponseSize -= int32(len(buf))
		}
		buf = append(buf, testResponse[:args.ResponseSize]...)
		hctx.Response, err = args.WriteResult(hctx.Response, buf)
		return err
	}
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	ms.testConnectionClients[hctx] = args
	return hctx.HijackResponse(ms)
}

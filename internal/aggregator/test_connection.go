// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

const MaxTestResponseSize = 10 << 20
const MaxTestResponseTimeoutSec = 86400

var testResponse = make([]byte, MaxTestResponseSize)

func init() {
	_, _ = rand.New().Read(testResponse)
}

type TestConnectionClient struct {
	args        tlstatshouse.TestConnection2Bytes // We remember both field mask to correctly serialize response
	hctx        *rpc.HandlerContext
	requestTime time.Time
}

type TestConnection struct {
	clientsMu             sync.Mutex // Always taken after mu
	testConnectionClients []TestConnectionClient
}

func MakeTestConnection() *TestConnection {
	result := &TestConnection{}
	go result.goRun()
	return result
}

func (ms *TestConnection) goRun() {
	for {
		time.Sleep(time.Second)
		ms.broadcastResponses()
	}
}

func (ms *TestConnection) broadcastResponses() {
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	keepPos := 0
	now := time.Now()
	for _, c := range ms.testConnectionClients {
		if now.Sub(c.requestTime) < time.Duration(c.args.ResponseTimeoutSec)*time.Second { // still waiting, copy to start of array
			ms.testConnectionClients[keepPos] = c
			keepPos++
			continue
		}
		var err error
		c.hctx.Response, err = c.args.WriteResult(c.hctx.Response, testResponse[:c.args.ResponseSize])
		c.hctx.SendHijackedResponse(err)
	}
	ms.testConnectionClients = ms.testConnectionClients[:keepPos]
}

func (ms *TestConnection) handleTestConnection(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.TestConnection2Bytes) (err error) {
	if args.ResponseSize > MaxTestResponseSize {
		return fmt.Errorf("max supported response_size is %d", MaxTestResponseSize)
	}
	if args.ResponseTimeoutSec > MaxTestResponseTimeoutSec {
		return fmt.Errorf("max supported response_timeout_sec is %d", MaxTestResponseTimeoutSec)
	}
	if args.ResponseTimeoutSec <= 0 {
		hctx.Response, err = args.WriteResult(hctx.Response, testResponse[:args.ResponseSize])
		return err
	}
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	ms.testConnectionClients = append(ms.testConnectionClients, TestConnectionClient{args: args, hctx: hctx, requestTime: time.Now()})
	return hctx.HijackResponse()
}

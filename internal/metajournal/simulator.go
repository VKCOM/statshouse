// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
)

func TestMapper(aggAddr []string, mapString string, client rpc.Client) {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	for i, addr := range aggAddr {
		statshouseClient := tlstatshouse.Client{
			Client:  client,
			Network: "tcp4",
			Address: addr,
			ActorID: 0,
		}
		args := tlstatshouse.GetMetrics3Bytes{
			Header: tlstatshouse.CommonProxyHeaderBytes{
				ShardReplica:      int32(i),
				ShardReplicaTotal: int32(len(aggAddr)),
				HostName:          nil,
				ComponentTag:      0,
				BuildArch:         0,
			},
			From:  0,
			Limit: 1,
		}
		args.Header.SetAgentEnvStaging0(true, &args.FieldsMask) // let's consider such requests staging
		var ret tlmetadata.GetJournalResponsenewBytes
		// We do not need timeout for long poll, RPC has disconnect detection via ping-pong

		now := time.Now()
		err := statshouseClient.GetMetrics3Bytes(context.Background(), args, &extra, &ret)
		if err != nil {
			log.Printf("check metric mapping via %s ERROR: %v elapsed=%v", addr, err, time.Since(now))
		} else {
			log.Printf("check metric mapping via %s ok, entries=%d, version=%d elapsed=%v", addr, len(ret.Events), ret.CurrentVersion, time.Since(now))
		}

		if mapString != "" {
			args2 := tlstatshouse.GetTagMapping2{
				Header: tlstatshouse.CommonProxyHeader{
					ShardReplica:      int32(i),
					ShardReplicaTotal: int32(len(aggAddr)),
					HostName:          "",
					ComponentTag:      0,
					BuildArch:         0,
				},
				Key:    mapString,
				Metric: "", // we are not going to create mappings
			}
			args.Header.SetAgentEnvStaging0(true, &args.FieldsMask) // let's consider such requests staging

			var ret2 tlstatshouse.GetTagMappingResult

			now = time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), data_model.AgentMappingTimeout2)
			err = statshouseClient.GetTagMapping2(ctx, args2, &extra, &ret2)
			cancel()

			if err != nil {
				log.Printf("         tag mapping via %s ERROR: %v elapsed=%v", addr, err, time.Since(now))
			} else {
				log.Printf("         tag mapping via %s ok, ttl=%v value=%d elapsed=%v", addr, time.Duration(ret2.TtlNanosec), ret2.Value, time.Since(now))
			}
		}
		testLongPoll(addr, i, len(aggAddr), client, 0)
	}
}

func testLongPoll(addr string, shardReplica int, shardsTotal int, client rpc.Client, timeoutSec int32) {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	statshouseClient := tlstatshouse.Client{
		Client:  client,
		Network: "tcp4",
		Address: addr,
		ActorID: 0,
	}
	args := tlstatshouse.TestConnection2{
		Header: tlstatshouse.CommonProxyHeader{
			ShardReplica:      int32(shardReplica),
			ShardReplicaTotal: int32(shardsTotal),
			HostName:          "",
			ComponentTag:      0,
			BuildArch:         0,
		},
		Payload:            strings.Repeat("t", 64<<10),
		ResponseSize:       128,
		ResponseTimeoutSec: timeoutSec,
	}
	args.Header.SetAgentEnvStaging0(true, &args.FieldsMask) // let's consider such requests staging
	var ret string
	// We do not need timeout for long poll, RPC has disconnect detection via ping-pong

	now := time.Now()
	err := statshouseClient.TestConnection2(context.Background(), args, &extra, &ret)
	if err != nil {
		log.Printf("test connection via %s ERROR: %v elapsed=%v", addr, err, time.Since(now))
	} else {
		log.Printf("test connection via %s ok sent %d received %d/%d bytes, elapsed=%v", addr, len(args.Payload), len(ret), args.ResponseSize, time.Since(now))
	}
}

func TestLongpoll(aggAddr []string, client rpc.Client, timeoutSec int32) {
	var wg sync.WaitGroup
	wg.Add(len(aggAddr))
	log.Printf("will test longpoll, nothing should happen for %d seconds", timeoutSec)
	for i, addr := range aggAddr {
		go func(shardReplica int, address string) {
			testLongPoll(address, shardReplica, len(aggAddr), client, timeoutSec)
			wg.Done()
		}(i, addr)
	}
	wg.Wait()
}

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"

	"pgregory.net/rand"
)

func TestMapper(aggAddr []string, mapString string, client *rpc.Client) {
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
		args.Header.SetAgentEnvStaging(true, &args.FieldsMask) // let's consider such requests staging
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
			args.Header.SetAgentEnvStaging(true, &args.FieldsMask) // let's consider such requests staging

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

func testLongPoll(addr string, shardReplica int, shardsTotal int, client *rpc.Client, timeoutSec int32) {
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
	args.Header.SetAgentEnvStaging(true, &args.FieldsMask) // let's consider such requests staging
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

func TestLongpoll(aggAddr []string, client *rpc.Client, timeoutSec int32) {
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

func RunSimulator(simID int, metricsStorage *metajournal.MetricsStorage, storageDir string, aesPwd string, config agent.Config) {
	logF := log.Printf
	if storageDir != "" {
		storageDir = filepath.Join(storageDir, "sim"+strconv.Itoa(simID))
		_ = os.Mkdir(storageDir, os.ModePerm)
		f, err := os.OpenFile(storageDir+"sim.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		logF = log.New(f, "", log.LstdFlags|log.Lshortfile|log.Lmicroseconds).Printf
	}
	sh2, err := agent.MakeAgent("tcp4", storageDir, aesPwd, config, "simulator_"+strconv.Itoa(simID+1),
		format.TagValueIDComponentAgent, metricsStorage, logF, nil, nil)
	if err != nil {
		log.Panicf("Cannot create simulator, %v", err)
	}
	sh2.Run(0, 0, 0)
	generateStats(simID, metricsStorage, sh2, 500, 2, 8, 2)
}

func generateStats(simID int, journal *metajournal.MetricsStorage, s *agent.Agent, totalRange int, key1range int32, key2range int32, key3range int32) {
	mag := s.CreateBuiltInItemValue(data_model.Key{Metric: 100, Keys: [16]int32{0, int32(simID)}})

	metricInfo1 := journal.GetMetaMetricByName(data_model.SimulatorMetricPrefix + "1")
	metricInfo2 := journal.GetMetaMetricByName(data_model.SimulatorMetricPrefix + "2")
	metricInfo3 := journal.GetMetaMetricByName(data_model.SimulatorMetricPrefix + "3")
	metricInfo4 := journal.GetMetaMetricByName(data_model.SimulatorMetricPrefix + "4")
	metricInfo5 := journal.GetMetaMetricByName(data_model.SimulatorMetricPrefix + "5")
	metricInfo6 := journal.GetMetaMetricByName(data_model.SimulatorMetricPrefix + "6")
	metricInfo7 := journal.GetMetaMetricByName(data_model.SimulatorMetricPrefix + "7")
	metricInfo8 := journal.GetMetaMetricByName(data_model.SimulatorMetricPrefix + "8")
	metricInfo9 := journal.GetMetaMetricByName(data_model.SimulatorMetricPrefix + "9")
	metricInfo10 := journal.GetMetaMetricByName(data_model.SimulatorMetricPrefix + "10")
	metricInfo11 := journal.GetMetaMetricByName(data_model.SimulatorMetricPrefix + "11")

	stop := [][]byte{[]byte("Masha"), []byte("Petya"), []byte("Vasya"), []byte("Petya"), []byte("Petya"), []byte("Masha")}
	rng := rand.New()
	for {
		spike := rng.Intn(10) == 0
		total := rng.Intn(totalRange)
		superTotal := total

		mag.AddValueCounter(float64(superTotal), 1)
		oneCounter := 0
		for i := 0; i < total; i++ {
			kCounter1 := data_model.Key{
				Metric: metricInfo1.MetricID,
			}
			kCounter1.Keys[1] = rng.Int31n(key1range)
			kCounter1.Keys[2] = rng.Int31n(key2range)
			kCounter1.Keys[3] = rng.Int31n(key3range)
			cc := 1 + rng.Intn(4)
			oneCounter += cc
			s.AddCounterHost(kCounter1, float64(cc), 0, metricInfo1)
		}
		if spike {
			for i := 0; i < totalRange; i++ {
				kCounter1 := data_model.Key{
					Metric: metricInfo1.MetricID,
				}
				kCounter1.Keys[4] = rng.Int31n(int32(totalRange))
				cc := 1 + rng.Intn(4)
				oneCounter += cc
				s.AddCounterHost(kCounter1, float64(cc), 0, metricInfo1)
			}
		}
		if oneCounter < 3*totalRange*5 {
			kCounter1 := data_model.Key{
				Metric: metricInfo1.MetricID,
			}
			s.AddCounterHost(kCounter1, float64(3*totalRange*5-oneCounter), 0, metricInfo1)
		}

		total = rng.Intn(totalRange)
		superTotal += total
		for i := 0; i < total; i++ {
			kValue2 := data_model.Key{
				Metric: metricInfo2.MetricID,
			}
			kValue2.Keys[1] = rng.Int31n(key1range)
			kValue2.Keys[2] = rng.Int31n(key2range)
			kValue2.Keys[3] = rng.Int31n(key3range)
			kValue3 := kValue2
			kValue3.Metric = metricInfo3.MetricID
			kValue10 := data_model.Key{
				Metric: metricInfo10.MetricID,
			}
			kValue10.Keys[1] = kValue2.Keys[1]
			kValue10.Keys[2] = kValue2.Keys[2]
			kValue11 := kValue10
			kValue11.Metric = metricInfo11.MetricID
			sKey := stop[int(kValue2.Keys[3])%len(stop)]
			cc := 1 + rng.Intn(4)
			for ci := 0; ci < cc; ci++ {
				value := rng.Float64() * 100
				s.AddValueCounter(kValue2, value, 1, metricInfo2)
				s.AddValueCounter(kValue3, value, 1, metricInfo3)
				s.AddValueArrayCounterHostStringBytes(kValue10, []float64{value}, 1, 0, sKey, metricInfo10)
				s.AddValueArrayCounterHostStringBytes(kValue11, []float64{value}, 1, 0, sKey, metricInfo11)
			}
		}
		total = rng.Intn(totalRange)
		superTotal += total
		for i := 0; i < total; i++ {
			kUnique4 := data_model.Key{
				Metric: metricInfo4.MetricID,
			}
			kUnique4.Keys[1] = rng.Int31n(key1range)
			kUnique4.Keys[2] = rng.Int31n(key2range)
			kUnique4.Keys[3] = rng.Int31n(key3range)
			kUnique7 := kUnique4
			kUnique7.Metric = metricInfo7.MetricID
			kUnique8 := data_model.Key{
				Metric: metricInfo8.MetricID,
			}
			kUnique8.Keys[1] = kUnique4.Keys[1]
			kUnique8.Keys[2] = kUnique4.Keys[2]
			sKey := stop[int(kUnique4.Keys[3])%len(stop)]
			cc := 1 + rng.Intn(4)
			for ci := 0; ci < cc; ci++ {
				const D = 1000000
				r := D + int64(rng.Intn(D))*int64(rng.Intn(D))
				s.AddUniqueHostStringBytes(kUnique4, 0, nil, []int64{r}, 1, metricInfo4)
				s.AddUniqueHostStringBytes(kUnique7, 0, nil, []int64{r}, 1, metricInfo7)
				s.AddUniqueHostStringBytes(kUnique8, 0, sKey, []int64{r}, 1, metricInfo8)
			}
		}

		total = rng.Intn(totalRange)
		superTotal += total
		for i := 0; i < total; i++ {
			kValue5 := data_model.Key{
				Metric: metricInfo5.MetricID,
			}
			kValue5.Keys[2] = rng.Int31n(key2range)
			sid := rng.Intn(len(stop) * 100)
			if sid < len(stop) {
				s.AddCounterHostStringBytes(kValue5, stop[sid], 1, 0, metricInfo5)
				continue
			}
			cc := 1 + rng.Intn(100000)
			s.AddCounterHostStringBytes(kValue5, []byte(fmt.Sprintf("A_%d", cc)), 1, 0, metricInfo5)
		}
		total = rng.Intn(totalRange)
		superTotal += total
		for i := 0; i < total*100; i++ {
			kCounter6 := data_model.Key{
				Metric: metricInfo6.MetricID,
			}
			kCounter6.Keys[1] = rng.Int31n(key1range)
			kCounter6.Keys[2] = rng.Int31n(key2range)
			kCounter6.Keys[3] = rng.Int31n(key3range)
			kCounter6.Keys[4] = rng.Int31n(1000)
			s.AddCounterHost(kCounter6, 1, 0, metricInfo6)
		}

		s.AddCounterHost(data_model.Key{Metric: metricInfo9.MetricID}, float64(superTotal), 0, metricInfo9)
		// msec := rng.Int() % 1000
		// time.Sleep(time.Duration(msec) * time.Millisecond)
		time.Sleep(time.Second)
	}
}

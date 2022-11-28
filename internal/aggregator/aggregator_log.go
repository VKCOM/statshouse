// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"encoding/binary"
	"log"
	"strconv"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/rowbinary"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

func (a *Aggregator) appendInternalLogLocked(typ string, key0 string, key1 string, key2 string, key3 string, key4 string, key5 string, message string) {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[0:], a.recentNow)

	a.internalLog = append(a.internalLog, tmp[:]...)
	a.internalLog = rowbinary.AppendString(a.internalLog, srvfunc.HostnameForStatshouse())
	a.internalLog = rowbinary.AppendString(a.internalLog, typ)
	a.internalLog = rowbinary.AppendString(a.internalLog, key0)
	a.internalLog = rowbinary.AppendString(a.internalLog, key1)
	a.internalLog = rowbinary.AppendString(a.internalLog, key2)
	a.internalLog = rowbinary.AppendString(a.internalLog, key3)
	a.internalLog = rowbinary.AppendString(a.internalLog, key4)
	a.internalLog = rowbinary.AppendString(a.internalLog, key5)
	a.internalLog = rowbinary.AppendString(a.internalLog, message)
}

func (a *Aggregator) appendInternalLog(typ string, key0 string, key1 string, key2 string, key3 string, key4 string, key5 string, message string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.appendInternalLogLocked(typ, key0, key1, key2, key3, key4, key5, message)
}

func (a *Aggregator) goInternalLog() {
	httpClient := makeHTTPClient(data_model.ClickHouseTimeout)
	var localLog []byte
	for {
		time.Sleep(data_model.InternalLogInsertInterval)

		a.mu.Lock()
		if len(a.internalLog) != 0 {
			localLog, a.internalLog = a.internalLog, localLog
		}
		a.mu.Unlock()

		if len(localLog) != 0 {
			status, exception, _, err := sendToClickhouse(httpClient, a.config.KHAddr, "statshouse_internal_log_buffer(time,host,type,key0,key1,key2,key3,key4,key5,message)", localLog)
			if err != nil {
				a.appendInternalLog("insert_error", "", strconv.Itoa(status), strconv.Itoa(exception), "statshouse_internal_log_buffer", "", "", err.Error()) // Hopefully will insert next time
				log.Printf("error inserting internal log - %v", err)
			}
			localLog = localLog[:0] // Will be swapped on the next iteration
		}
	}
}

func (a *Aggregator) reporInsert(bucketTime uint32, conveyorTag int32, err error, dur float64, bodySize int) {
	key := data_model.AggKey(bucketTime, format.BuiltinMetricIDAggInsertTime, [16]int32{0, 0, 0, 0, conveyorTag, format.TagValueIDInsertTimeOK}, a.aggregatorHost, a.shardKey, a.replicaKey)
	if err != nil {
		key.Keys[5] = format.TagValueIDInsertTimeError
	}
	a.sh2.AddValueCounterHost(key, dur, 1, 0)
	a.sh2.AddValueCounterHost(data_model.Key{Timestamp: 0, Metric: format.BuiltinMetricIDAggInsertTimeReal, Keys: key.Keys}, dur, 1, 0)
	a.sh2.AddValueCounterHost(data_model.Key{Timestamp: 0, Metric: format.BuiltinMetricIDAggInsertSizeReal, Keys: key.Keys}, float64(bodySize), 1, 0)
}

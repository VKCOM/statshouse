// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"encoding/binary"
	"log"
	"strconv"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/rowbinary"
	"github.com/VKCOM/statshouse/internal/vkgo/srvfunc"
)

func (a *Aggregator) appendInternalLogLocked(typ string, key0 string, key1 string, key2 string, key3 string, key4 string, key5 string, message string) {
	nowUnix := uint32(time.Now().Unix())
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[0:], nowUnix)

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

// We do not want to wait this func to finish, so no attempts to cancel
func (a *Aggregator) goInternalLog() {
	httpClient := makeHTTPClient()
	var localLog []byte
	for {
		time.Sleep(data_model.InternalLogInsertInterval)

		a.mu.Lock()
		if len(a.internalLog) != 0 {
			localLog, a.internalLog = a.internalLog, localLog
		}
		a.mu.Unlock()

		if len(localLog) != 0 {
			ctx, cancel := context.WithTimeout(context.Background(), data_model.ClickHouseTimeoutInsert)
			status, exception, _, err := sendToClickhouse(ctx, httpClient, a.config.KHAddr, a.config.KHUser, a.config.KHPassword, "statshouse_internal_log_buffer(time,host,type,key0,key1,key2,key3,key4,key5,message)", localLog, "")
			cancel()
			if err != nil {
				a.appendInternalLog("insert_error", "", strconv.Itoa(status), strconv.Itoa(exception), "statshouse_internal_log_buffer", "", "", err.Error()) // Hopefully will insert next time
				log.Printf("error inserting internal log - %v", err)
			}
			localLog = localLog[:0] // Will be swapped on the next iteration
		}
	}
}

func (a *Aggregator) reportInsertMetric(t uint32, metricInfo *format.MetricMetaValue, historic bool, err error, status int, exception int, v3Format bool, inflightType int32, value float64) {
	v3FormatTag := int32(format.TagValueIDAggInsertV2)
	if v3Format {
		v3FormatTag = format.TagValueIDAggInsertV3
	}
	historicTag := int32(format.TagValueIDConveyorRecent)
	if historic {
		historicTag = format.TagValueIDConveyorHistoric
	}
	statusTag := int32(format.TagValueIDStatusOK)
	if err != nil {
		statusTag = format.TagValueIDStatusError
	}
	a.sh2.AddValueCounter(t, metricInfo,
		[]int32{0, 0, 0, 0, historicTag, statusTag, int32(status), int32(exception), v3FormatTag, inflightType},
		value, 1)
}

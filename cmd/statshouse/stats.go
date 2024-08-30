// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"strconv"

	"github.com/vkcom/statshouse/internal/metajournal"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/receiver"
)

type statsHandler struct {
	receiversUDP   []*receiver.UDP
	receiverRPC    *receiver.RPCReceiver
	sh2            *agent.Agent
	metricsStorage *metajournal.MetricsStorage
}

func (h statsHandler) handleStats(stats map[string]string) {
	var statPacketsTotal uint64
	var statBytesTotal uint64
	var statBatchesTotalOK uint64
	var statBatchesTotalErr uint64
	for i, u := range h.receiversUDP {
		v := u.StatPacketsTotal()
		statPacketsTotal += v
		if len(h.receiversUDP) > 1 {
			stats[fmt.Sprintf("statshouse_udp_recv_%d_packets", i)] = strconv.FormatUint(v, 10)
		}
		v = u.StatBytesTotal()
		statBytesTotal += v
		if len(h.receiversUDP) > 1 {
			stats[fmt.Sprintf("statshouse_udp_recv_%d_bytes", i)] = strconv.FormatUint(v, 10)
		}
		v = u.StatBatchesTotalOK()
		statBatchesTotalOK += v
		if len(h.receiversUDP) > 1 {
			stats[fmt.Sprintf("statshouse_udp_recv_%d_batches_ok", i)] = strconv.FormatUint(v, 10)
		}
		v = u.StatBatchesTotalErr()
		statBatchesTotalErr += v
		if len(h.receiversUDP) > 1 {
			stats[fmt.Sprintf("statshouse_udp_recv_%d_batches_err", i)] = strconv.FormatUint(v, 10)
		}
	}
	stats["statshouse_udp_recv_packets"] = strconv.FormatUint(statPacketsTotal, 10)
	stats["statshouse_udp_recv_bytes"] = strconv.FormatUint(statBytesTotal, 10)
	stats["statshouse_udp_recv_batches_ok"] = strconv.FormatUint(statBatchesTotalOK, 10)
	stats["statshouse_udp_recv_batches_err"] = strconv.FormatUint(statBatchesTotalErr, 10)

	stats["statshouse_rpc_recv_calls_ok"] = strconv.FormatUint(h.receiverRPC.StatBatchesTotalOK(), 10)
	stats["statshouse_rpc_recv_calls_err"] = strconv.FormatUint(h.receiverRPC.StatBatchesTotalErr(), 10)

	stats["statshouse_journal_version"] = strconv.FormatInt(h.metricsStorage.Version(), 10)
	for i, s := range h.sh2.Shards {
		t, u := s.HistoricBucketsDataSizeDisk()
		stats[fmt.Sprintf("statshouse_queue_size_disk_total_%d", i)] = fmt.Sprintf("%d", t)
		stats[fmt.Sprintf("statshouse_queue_size_disk_sent_%d", i)] = fmt.Sprintf("%d", t-u)
		stats[fmt.Sprintf("statshouse_queue_size_disk_unsent_%d", i)] = fmt.Sprintf("%d", u)
		stats[fmt.Sprintf("statshouse_queue_size_memory_%d", i)] = fmt.Sprintf("%d", s.HistoricBucketsDataSizeMemory())
		stats[fmt.Sprintf("historic_out_of_window_dropped_%d", i)] = strconv.FormatInt(s.HistoricOutOfWindowDropped.Load(), 10)
	}
	for i, s := range h.sh2.ShardReplicas {
		stats[fmt.Sprintf("statshouse_shard_replica_alive_%d", i)] = fmt.Sprintf("%v", s.IsAlive())
		s.FillStats(stats)
	}
	t, u := h.sh2.HistoricBucketsDataSizeDiskSum()
	stats["statshouse_queue_size_disk_total"] = fmt.Sprintf("%d", t)
	stats["statshouse_queue_size_disk_sent"] = fmt.Sprintf("%d", t-u)
	stats["statshouse_queue_size_disk_unsent"] = fmt.Sprintf("%d", u)
	stats["statshouse_queue_size_memory"] = fmt.Sprintf("%d", h.sh2.HistoricBucketsDataSizeMemorySum())

}

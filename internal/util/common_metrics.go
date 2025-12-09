package util

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/VKCOM/statshouse-go"

	"github.com/VKCOM/statshouse/internal/env"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/commonmetrics/metricshandler"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
)

type RPCServerMetrics struct {
	сonnCount       statshouse.MetricRef
	workerPoolSize  statshouse.MetricRef
	longPollWaiting statshouse.MetricRef
	requestMemory   statshouse.MetricRef
	responseMemory  statshouse.MetricRef
	commonTags      statshouse.Tags
}

func NewRPCServerMetrics(service string) *RPCServerMetrics {
	env := env.ReadEnvironment(service)
	tag := statshouse.Tags{
		env.Name,
		env.Service,
		env.Cluster,
		env.DataCenter,
	}
	return &RPCServerMetrics{
		сonnCount:       statshouse.GetMetricRef("common_rpc_server_conn", tag),
		workerPoolSize:  statshouse.GetMetricRef("common_rpc_server_worker_pool_size", tag),
		longPollWaiting: statshouse.GetMetricRef("common_rpc_server_longpoll_waiting", tag),
		requestMemory:   statshouse.GetMetricRef("common_rpc_server_request_mem", tag),
		responseMemory:  statshouse.GetMetricRef("common_rpc_server_response_mem", tag),
		commonTags:      tag,
	}
}

func (s *RPCServerMetrics) ServerWithMetrics(so *rpc.ServerOptions) {
	so.AcceptErrHandler = s.handleAcceptError
	so.ConnErrHandler = s.handleConnError
	so.ResponseHook = s.handleResponse
}

func (s *RPCServerMetrics) Run(server *rpc.Server) func() {
	id := statshouse.StartRegularMeasurement(func(client *statshouse.Client) {
		s.сonnCount.Value(float64(server.ConnectionsCurrent()))
		workerPoolSize, _ := server.WorkersPoolSize()
		s.workerPoolSize.Value(float64(workerPoolSize))
		s.longPollWaiting.Value(float64(server.LongPollsWaiting()))
		requestMemory, _ := server.RequestsMemory()
		s.requestMemory.Value(float64(requestMemory))
		responseMemory, _ := server.ResponsesMemory()
		s.responseMemory.Value(float64(responseMemory))
	})
	return func() {
		statshouse.StopRegularMeasurement(id)
	}
}

func (s *RPCServerMetrics) handleAcceptError(err error) {
	tags := s.commonTags
	tags[4] = rpc.ErrorTag(err)
	statshouse.Count("common_rpc_server_accept_error", tags, 1)
}

func (s *RPCServerMetrics) handleConnError(err error) {
	tags := s.commonTags // copy
	tags[4] = rpc.ErrorTag(err)
	statshouse.Count("common_rpc_server_conn_error", tags, 1)
}

func (s *RPCServerMetrics) handleResponse(hctx *rpc.HandlerContext, err error) {
	tags := s.commonTags // copy
	metricshandler.AttachRPC(tags[:], hctx, err)
	metricshandler.ResponseTimeRaw(tags, time.Since(hctx.RequestTime()))
	metricshandler.ResponseSizeRaw(tags, len(hctx.Response))
	metricshandler.RequestSizeRaw(tags, len(hctx.Request))
}

func HeartbeatVersionArgTags() statshouse.Tags {
	args := os.Args[1:]
	argMap := make(map[string]string)

	for i := 0; i < len(args); i++ {
		key := strings.TrimPrefix(strings.TrimPrefix(args[i], "-"), "-")
		var value string
		if strings.Contains(key, "=") {
			parts := strings.SplitN(key, "=", 2)
			key = parts[0]
			value = parts[1]
		} else if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			value = args[i+1]
			i++
		} else {
			argMap[key] = key
			continue
		}
		if _, ok := argMap[key]; ok {
			argMap[key] += "," + value
		} else {
			argMap[key] = fmt.Sprintf("%s=%s", key, value)
		}
	}
	idKeys := map[int][]string{
		30: {"agg-addr"},
		31: {"cache-dir", "historic-storage", "disk-cache", "db-path", "log-file", "l"},
		32: {"p", "ingress-addr", "listen-addr", "listen-addr-ipv6", "listen-addr-unix", "ingress-addr-ipv6"},
		33: {"disable-remote-config", "hardware-metric-scrape-disable", "prometheus-push-remote", "pprof-http", "auto-create-default-namespace", "auto-create", "version", "show-invisible", "clickhouse-v2-debug", "clickhouse-v1-debug", "access-log", "verbose", "readonly", "query-sequential", "insecure-mode", "local-mode", "send-source-bucket2"},
		34: {"cluster", "hostname", "u", "g"},
		35: {"aes-pwd-file", "ingress-pwd-dir", "kh-password-file", "env-file-path"},
		36: {"sample-budget", "clickhouse-max-queries", "clickhouse-v1-max-conns", "clickhouse-v2-max-hardware-slow-conns", "clickhouse-v2-max-hardware-fast-conns", "clickhouse-v2-max-heavy-slow-conns", "clickhouse-v2-max-heavy-conns", "clickhouse-v2-max-light-slow-conns", "clickhouse-v2-max-conns", "historic-inserters", "max-budget"},
		37: {"ingress-external-addr", "ingress-external-addr-ipv6"},
		38: {"max-open-files", "cores", "cores-udp", "buffer-size-udp", "max-chunks-count", "default-num-series"},
		39: {"clickhouse-v2-addrs", "clickhouse-v1-addrs", "kh", "kh-v1", "kh-user", "kh-v1-user"},
	}
	tags := statshouse.Tags{}
	for id, keys := range idKeys {
		for _, k := range keys {
			if v, ok := argMap[k]; ok {
				tags[id+1] += v + " " // id+1 builtin shift host
				delete(argMap, k)
			}
		}
	}
	var remain []string
	for _, v := range argMap {
		remain = append(remain, v)
	}
	if len(remain) > 0 {
		tags[40+1] = strings.Join(remain, " ") // id+1 builtin shift host
	}
	for i := 0; i < len(tags); i++ {
		tags[i] = string(format.ForceValidStringValue(tags[i]))
	}
	return tags
}

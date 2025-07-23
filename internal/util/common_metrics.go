package util

import (
	"time"

	"github.com/VKCOM/statshouse-go"
	"github.com/VKCOM/statshouse/internal/env"
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
	metricshandler.ResponseTimeRaw(tags, time.Since(hctx.RequestTime))
	metricshandler.ResponseSizeRaw(tags, len(hctx.Response))
	metricshandler.RequestSizeRaw(tags, len(hctx.Request))
}

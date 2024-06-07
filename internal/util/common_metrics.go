package util

import (
	"errors"
	"net"
	"time"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/env"
	"github.com/vkcom/statshouse/internal/vkgo/commonmetrics/metricshandler"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type RPCServerMetrics struct {
	сonnCount  *statshouse.MetricRef
	commonTags statshouse.Tags
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
		сonnCount:  statshouse.Metric("common_rpc_server_conn", tag),
		commonTags: tag,
	}
}

func (s *RPCServerMetrics) ServerWithMetrics(so *rpc.ServerOptions) {
	so.AcceptErrHandler = s.handleAcceptError
	so.ConnErrHandler = s.handleConnError
	so.ResponseHandler = s.handleResponse
}

func (s *RPCServerMetrics) Run(server *rpc.Server) func() {
	id := statshouse.StartRegularMeasurement(func(client *statshouse.Client) {
		s.сonnCount.Count(float64(server.ConnectionsCurrent()))
	})
	return func() {
		statshouse.StopRegularMeasurement(id)
	}
}

func (s *RPCServerMetrics) handleAcceptError(err error) {
	tags := s.commonTags
	tags[4] = errTagValue(err)
	statshouse.Metric("common_rpc_server_accept_error", tags).Count(1)
}

func (s *RPCServerMetrics) handleConnError(err error) {
	tags := s.commonTags // copy
	tags[4] = errTagValue(err)
	statshouse.Metric("common_rpc_server_conn_error", tags).Count(1)
}

func (s *RPCServerMetrics) handleResponse(hctx *rpc.HandlerContext, err error) {
	tags := s.commonTags // copy
	metricshandler.AttachRPC(tags[:], hctx, err)
	metricshandler.ResponseTimeRaw(tags, time.Since(hctx.RequestTime))
	metricshandler.ResponseSizeRaw(tags, len(hctx.Response))
	metricshandler.RequestSizeRaw(tags, len(hctx.Request))
}

func errTagValue(err error) string {
	// tag value should not include connection specific information
	var oe *net.OpError
	if errors.As(err, &oe) && oe.Err != nil {
		// drop IP address
		return oe.Err.Error()

	}
	return err.Error()
}

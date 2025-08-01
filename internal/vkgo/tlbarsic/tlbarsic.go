// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Code generated by vktl/cmd/tlgen2; DO NOT EDIT.
package tlbarsic

import (
	"context"
	"time"

	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/internal"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tl/tlTrue"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tl/tlVectorBarsicSnapshotDependency"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tl/tlVectorBarsicSnapshotExternalFile"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicApplyPayload"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicChangeRole"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicCommit"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicEngineStarted"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicEngineStatus"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicEngineWantsRestart"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicReindex"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicRevert"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicShutdown"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicSkip"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicSnapshotDependency"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicSnapshotExternalFile"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicSnapshotHeader"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicSplit"
	"github.com/VKCOM/statshouse/internal/vkgo/internal/tlbarsic/tlBarsicStart"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
)

type (
	ApplyPayload                          = tlBarsicApplyPayload.BarsicApplyPayload
	ApplyPayloadBytes                     = tlBarsicApplyPayload.BarsicApplyPayloadBytes
	ChangeRole                            = tlBarsicChangeRole.BarsicChangeRole
	Commit                                = tlBarsicCommit.BarsicCommit
	CommitBytes                           = tlBarsicCommit.BarsicCommitBytes
	EngineStarted                         = tlBarsicEngineStarted.BarsicEngineStarted
	EngineStartedBytes                    = tlBarsicEngineStarted.BarsicEngineStartedBytes
	EngineStatus                          = tlBarsicEngineStatus.BarsicEngineStatus
	EngineStatusBytes                     = tlBarsicEngineStatus.BarsicEngineStatusBytes
	EngineWantsRestart                    = tlBarsicEngineWantsRestart.BarsicEngineWantsRestart
	Reindex                               = tlBarsicReindex.BarsicReindex
	Revert                                = tlBarsicRevert.BarsicRevert
	Shutdown                              = tlBarsicShutdown.BarsicShutdown
	Skip                                  = tlBarsicSkip.BarsicSkip
	SnapshotDependency                    = tlBarsicSnapshotDependency.BarsicSnapshotDependency
	SnapshotDependencyBytes               = tlBarsicSnapshotDependency.BarsicSnapshotDependencyBytes
	SnapshotExternalFile                  = tlBarsicSnapshotExternalFile.BarsicSnapshotExternalFile
	SnapshotExternalFileBytes             = tlBarsicSnapshotExternalFile.BarsicSnapshotExternalFileBytes
	SnapshotHeader                        = tlBarsicSnapshotHeader.BarsicSnapshotHeader
	SnapshotHeaderBytes                   = tlBarsicSnapshotHeader.BarsicSnapshotHeaderBytes
	Split                                 = tlBarsicSplit.BarsicSplit
	SplitBytes                            = tlBarsicSplit.BarsicSplitBytes
	Start                                 = tlBarsicStart.BarsicStart
	StartBytes                            = tlBarsicStart.BarsicStartBytes
	VectorBarsicSnapshotDependency        = tlVectorBarsicSnapshotDependency.VectorBarsicSnapshotDependency
	VectorBarsicSnapshotDependencyBytes   = tlVectorBarsicSnapshotDependency.VectorBarsicSnapshotDependencyBytes
	VectorBarsicSnapshotExternalFile      = tlVectorBarsicSnapshotExternalFile.VectorBarsicSnapshotExternalFile
	VectorBarsicSnapshotExternalFileBytes = tlVectorBarsicSnapshotExternalFile.VectorBarsicSnapshotExternalFileBytes

	ApplyPayload__Result       = tlTrue.True
	ChangeRole__Result         = tlTrue.True
	Commit__Result             = tlTrue.True
	EngineStarted__Result      = tlTrue.True
	EngineStatus__Result       = tlTrue.True
	EngineWantsRestart__Result = tlTrue.True
	Reindex__Result            = tlTrue.True
	Revert__Result             = tlTrue.True
	Shutdown__Result           = tlTrue.True
	Skip__Result               = tlTrue.True
	Split__Result              = tlTrue.True
	Start__Result              = tlTrue.True
)

type Client struct {
	Client  rpc.Client
	Network string // should be either "tcp4" or "unix"
	Address string
	ActorID int64         // should be >0 for routing via rpc-proxy
	Timeout time.Duration // set to extra.CustomTimeoutMs, if not already set
}

func (c *Client) ApplyPayloadBytes(ctx context.Context, args ApplyPayloadBytes, extra *rpc.InvokeReqExtra, ret *ApplyPayload__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.applyPayload"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.applyPayload", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.applyPayload", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.applyPayload", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) ApplyPayload(ctx context.Context, args ApplyPayload, extra *rpc.InvokeReqExtra, ret *ApplyPayload__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.applyPayload"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.applyPayload", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.applyPayload", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.applyPayload", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) ChangeRole(ctx context.Context, args ChangeRole, extra *rpc.InvokeReqExtra, ret *ChangeRole__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.changeRole"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.changeRole", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.changeRole", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.changeRole", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) CommitBytes(ctx context.Context, args CommitBytes, extra *rpc.InvokeReqExtra, ret *Commit__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.commit"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.commit", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.commit", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.commit", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) Commit(ctx context.Context, args Commit, extra *rpc.InvokeReqExtra, ret *Commit__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.commit"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.commit", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.commit", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.commit", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) EngineStartedBytes(ctx context.Context, args EngineStartedBytes, extra *rpc.InvokeReqExtra, ret *EngineStarted__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.engineStarted"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.engineStarted", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.engineStarted", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.engineStarted", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) EngineStarted(ctx context.Context, args EngineStarted, extra *rpc.InvokeReqExtra, ret *EngineStarted__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.engineStarted"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.engineStarted", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.engineStarted", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.engineStarted", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) EngineStatusBytes(ctx context.Context, args EngineStatusBytes, extra *rpc.InvokeReqExtra, ret *EngineStatus__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.engineStatus"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.engineStatus", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.engineStatus", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.engineStatus", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) EngineStatus(ctx context.Context, args EngineStatus, extra *rpc.InvokeReqExtra, ret *EngineStatus__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.engineStatus"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.engineStatus", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.engineStatus", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.engineStatus", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) EngineWantsRestart(ctx context.Context, args EngineWantsRestart, extra *rpc.InvokeReqExtra, ret *EngineWantsRestart__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.engineWantsRestart"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.engineWantsRestart", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.engineWantsRestart", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.engineWantsRestart", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) Reindex(ctx context.Context, args Reindex, extra *rpc.InvokeReqExtra, ret *Reindex__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.reindex"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.reindex", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.reindex", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.reindex", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) Revert(ctx context.Context, args Revert, extra *rpc.InvokeReqExtra, ret *Revert__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.revert"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.revert", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.revert", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.revert", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) Shutdown(ctx context.Context, args Shutdown, extra *rpc.InvokeReqExtra, ret *Shutdown__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.shutdown"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.shutdown", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.shutdown", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.shutdown", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) Skip(ctx context.Context, args Skip, extra *rpc.InvokeReqExtra, ret *Skip__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.skip"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.skip", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.skip", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.skip", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) SplitBytes(ctx context.Context, args SplitBytes, extra *rpc.InvokeReqExtra, ret *Split__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.split"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.split", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.split", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.split", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) Split(ctx context.Context, args Split, extra *rpc.InvokeReqExtra, ret *Split__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.split"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.split", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.split", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.split", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) StartBytes(ctx context.Context, args StartBytes, extra *rpc.InvokeReqExtra, ret *Start__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.start"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.start", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.start", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.start", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

func (c *Client) Start(ctx context.Context, args Start, extra *rpc.InvokeReqExtra, ret *Start__Result) (err error) {
	req := c.Client.GetRequest()
	req.ActorID = c.ActorID
	req.FunctionName = "barsic.start"
	if extra != nil {
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
	}
	rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
	req.Body, err = args.WriteBoxedGeneral(req.Body)
	if err != nil {
		return internal.ErrorClientWrite("barsic.start", err)
	}
	resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
	if extra != nil && resp != nil {
		extra.ResponseExtra = resp.Extra
	}
	defer c.Client.PutResponse(resp)
	if err != nil {
		return internal.ErrorClientDo("barsic.start", c.Network, c.ActorID, c.Address, err)
	}
	if ret != nil {
		resp.Body, err = args.ReadResult(resp.Body, ret)
		if err != nil {
			return internal.ErrorClientReadResult("barsic.start", c.Network, c.ActorID, c.Address, err)
		}
	}
	return nil
}

type Handler struct {
	ApplyPayload       func(ctx context.Context, args ApplyPayload) (ApplyPayload__Result, error)             // barsic.applyPayload
	ChangeRole         func(ctx context.Context, args ChangeRole) (ChangeRole__Result, error)                 // barsic.changeRole
	Commit             func(ctx context.Context, args Commit) (Commit__Result, error)                         // barsic.commit
	EngineStarted      func(ctx context.Context, args EngineStarted) (EngineStarted__Result, error)           // barsic.engineStarted
	EngineStatus       func(ctx context.Context, args EngineStatus) (EngineStatus__Result, error)             // barsic.engineStatus
	EngineWantsRestart func(ctx context.Context, args EngineWantsRestart) (EngineWantsRestart__Result, error) // barsic.engineWantsRestart
	Reindex            func(ctx context.Context, args Reindex) (Reindex__Result, error)                       // barsic.reindex
	Revert             func(ctx context.Context, args Revert) (Revert__Result, error)                         // barsic.revert
	Shutdown           func(ctx context.Context, args Shutdown) (Shutdown__Result, error)                     // barsic.shutdown
	Skip               func(ctx context.Context, args Skip) (Skip__Result, error)                             // barsic.skip
	Split              func(ctx context.Context, args Split) (Split__Result, error)                           // barsic.split
	Start              func(ctx context.Context, args Start) (Start__Result, error)                           // barsic.start

	RawApplyPayload       func(ctx context.Context, hctx *rpc.HandlerContext) error // barsic.applyPayload
	RawChangeRole         func(ctx context.Context, hctx *rpc.HandlerContext) error // barsic.changeRole
	RawCommit             func(ctx context.Context, hctx *rpc.HandlerContext) error // barsic.commit
	RawEngineStarted      func(ctx context.Context, hctx *rpc.HandlerContext) error // barsic.engineStarted
	RawEngineStatus       func(ctx context.Context, hctx *rpc.HandlerContext) error // barsic.engineStatus
	RawEngineWantsRestart func(ctx context.Context, hctx *rpc.HandlerContext) error // barsic.engineWantsRestart
	RawReindex            func(ctx context.Context, hctx *rpc.HandlerContext) error // barsic.reindex
	RawRevert             func(ctx context.Context, hctx *rpc.HandlerContext) error // barsic.revert
	RawShutdown           func(ctx context.Context, hctx *rpc.HandlerContext) error // barsic.shutdown
	RawSkip               func(ctx context.Context, hctx *rpc.HandlerContext) error // barsic.skip
	RawSplit              func(ctx context.Context, hctx *rpc.HandlerContext) error // barsic.split
	RawStart              func(ctx context.Context, hctx *rpc.HandlerContext) error // barsic.start
}

func (h *Handler) Handle(ctx context.Context, hctx *rpc.HandlerContext) (err error) {
	tag, r, _ := basictl.NatReadTag(hctx.Request) // keep hctx.Request intact for handler chaining
	switch tag {
	case 0x8c981e13: // barsic.applyPayload
		hctx.RequestFunctionName = "barsic.applyPayload"
		if h.RawApplyPayload != nil && !hctx.BodyFormatTL2() {
			hctx.Request = r
			err = h.RawApplyPayload(ctx, hctx)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.applyPayload", err)
			}
			return nil
		}
		if h.ApplyPayload != nil {
			var args ApplyPayload
			if hctx.BodyFormatTL2() {
				tctx := basictl.TL2ReadContext{}
				_, err = args.ReadTL2(r, &tctx)
			} else {
				_, err = args.Read(r)
			}
			if err != nil {
				return internal.ErrorServerRead("barsic.applyPayload", err)
			}
			ctx = hctx.WithContext(ctx)
			ret, err := h.ApplyPayload(ctx, args)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.applyPayload", err)
			}
			hctx.Response, err = args.WriteResult(hctx.Response, ret)
			if err != nil {
				return internal.ErrorServerWriteResult("barsic.applyPayload", err)
			}
			return nil
		}
	case 0xecb3db89: // barsic.changeRole
		hctx.RequestFunctionName = "barsic.changeRole"
		if h.RawChangeRole != nil && !hctx.BodyFormatTL2() {
			hctx.Request = r
			err = h.RawChangeRole(ctx, hctx)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.changeRole", err)
			}
			return nil
		}
		if h.ChangeRole != nil {
			var args ChangeRole
			if hctx.BodyFormatTL2() {
				tctx := basictl.TL2ReadContext{}
				_, err = args.ReadTL2(r, &tctx)
			} else {
				_, err = args.Read(r)
			}
			if err != nil {
				return internal.ErrorServerRead("barsic.changeRole", err)
			}
			ctx = hctx.WithContext(ctx)
			ret, err := h.ChangeRole(ctx, args)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.changeRole", err)
			}
			hctx.Response, err = args.WriteResult(hctx.Response, ret)
			if err != nil {
				return internal.ErrorServerWriteResult("barsic.changeRole", err)
			}
			return nil
		}
	case 0x12357324: // barsic.commit
		hctx.RequestFunctionName = "barsic.commit"
		if h.RawCommit != nil && !hctx.BodyFormatTL2() {
			hctx.Request = r
			err = h.RawCommit(ctx, hctx)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.commit", err)
			}
			return nil
		}
		if h.Commit != nil {
			var args Commit
			if hctx.BodyFormatTL2() {
				tctx := basictl.TL2ReadContext{}
				_, err = args.ReadTL2(r, &tctx)
			} else {
				_, err = args.Read(r)
			}
			if err != nil {
				return internal.ErrorServerRead("barsic.commit", err)
			}
			ctx = hctx.WithContext(ctx)
			ret, err := h.Commit(ctx, args)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.commit", err)
			}
			hctx.Response, err = args.WriteResult(hctx.Response, ret)
			if err != nil {
				return internal.ErrorServerWriteResult("barsic.commit", err)
			}
			return nil
		}
	case 0x4798167a: // barsic.engineStarted
		hctx.RequestFunctionName = "barsic.engineStarted"
		if h.RawEngineStarted != nil && !hctx.BodyFormatTL2() {
			hctx.Request = r
			err = h.RawEngineStarted(ctx, hctx)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.engineStarted", err)
			}
			return nil
		}
		if h.EngineStarted != nil {
			var args EngineStarted
			if hctx.BodyFormatTL2() {
				tctx := basictl.TL2ReadContext{}
				_, err = args.ReadTL2(r, &tctx)
			} else {
				_, err = args.Read(r)
			}
			if err != nil {
				return internal.ErrorServerRead("barsic.engineStarted", err)
			}
			ctx = hctx.WithContext(ctx)
			ret, err := h.EngineStarted(ctx, args)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.engineStarted", err)
			}
			hctx.Response, err = args.WriteResult(hctx.Response, ret)
			if err != nil {
				return internal.ErrorServerWriteResult("barsic.engineStarted", err)
			}
			return nil
		}
	case 0xbfe7b094: // barsic.engineStatus
		hctx.RequestFunctionName = "barsic.engineStatus"
		if h.RawEngineStatus != nil && !hctx.BodyFormatTL2() {
			hctx.Request = r
			err = h.RawEngineStatus(ctx, hctx)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.engineStatus", err)
			}
			return nil
		}
		if h.EngineStatus != nil {
			var args EngineStatus
			if hctx.BodyFormatTL2() {
				tctx := basictl.TL2ReadContext{}
				_, err = args.ReadTL2(r, &tctx)
			} else {
				_, err = args.Read(r)
			}
			if err != nil {
				return internal.ErrorServerRead("barsic.engineStatus", err)
			}
			ctx = hctx.WithContext(ctx)
			ret, err := h.EngineStatus(ctx, args)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.engineStatus", err)
			}
			hctx.Response, err = args.WriteResult(hctx.Response, ret)
			if err != nil {
				return internal.ErrorServerWriteResult("barsic.engineStatus", err)
			}
			return nil
		}
	case 0xf0ef3d68: // barsic.engineWantsRestart
		hctx.RequestFunctionName = "barsic.engineWantsRestart"
		if h.RawEngineWantsRestart != nil && !hctx.BodyFormatTL2() {
			hctx.Request = r
			err = h.RawEngineWantsRestart(ctx, hctx)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.engineWantsRestart", err)
			}
			return nil
		}
		if h.EngineWantsRestart != nil {
			var args EngineWantsRestart
			if hctx.BodyFormatTL2() {
				tctx := basictl.TL2ReadContext{}
				_, err = args.ReadTL2(r, &tctx)
			} else {
				_, err = args.Read(r)
			}
			if err != nil {
				return internal.ErrorServerRead("barsic.engineWantsRestart", err)
			}
			ctx = hctx.WithContext(ctx)
			ret, err := h.EngineWantsRestart(ctx, args)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.engineWantsRestart", err)
			}
			hctx.Response, err = args.WriteResult(hctx.Response, ret)
			if err != nil {
				return internal.ErrorServerWriteResult("barsic.engineWantsRestart", err)
			}
			return nil
		}
	case 0x6a8e47c1: // barsic.reindex
		hctx.RequestFunctionName = "barsic.reindex"
		if h.RawReindex != nil && !hctx.BodyFormatTL2() {
			hctx.Request = r
			err = h.RawReindex(ctx, hctx)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.reindex", err)
			}
			return nil
		}
		if h.Reindex != nil {
			var args Reindex
			if hctx.BodyFormatTL2() {
				tctx := basictl.TL2ReadContext{}
				_, err = args.ReadTL2(r, &tctx)
			} else {
				_, err = args.Read(r)
			}
			if err != nil {
				return internal.ErrorServerRead("barsic.reindex", err)
			}
			ctx = hctx.WithContext(ctx)
			ret, err := h.Reindex(ctx, args)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.reindex", err)
			}
			hctx.Response, err = args.WriteResult(hctx.Response, ret)
			if err != nil {
				return internal.ErrorServerWriteResult("barsic.reindex", err)
			}
			return nil
		}
	case 0x7e05a9de: // barsic.revert
		hctx.RequestFunctionName = "barsic.revert"
		if h.RawRevert != nil && !hctx.BodyFormatTL2() {
			hctx.Request = r
			err = h.RawRevert(ctx, hctx)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.revert", err)
			}
			return nil
		}
		if h.Revert != nil {
			var args Revert
			if hctx.BodyFormatTL2() {
				tctx := basictl.TL2ReadContext{}
				_, err = args.ReadTL2(r, &tctx)
			} else {
				_, err = args.Read(r)
			}
			if err != nil {
				return internal.ErrorServerRead("barsic.revert", err)
			}
			ctx = hctx.WithContext(ctx)
			ret, err := h.Revert(ctx, args)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.revert", err)
			}
			hctx.Response, err = args.WriteResult(hctx.Response, ret)
			if err != nil {
				return internal.ErrorServerWriteResult("barsic.revert", err)
			}
			return nil
		}
	case 0x708fd8d4: // barsic.shutdown
		hctx.RequestFunctionName = "barsic.shutdown"
		if h.RawShutdown != nil && !hctx.BodyFormatTL2() {
			hctx.Request = r
			err = h.RawShutdown(ctx, hctx)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.shutdown", err)
			}
			return nil
		}
		if h.Shutdown != nil {
			var args Shutdown
			if hctx.BodyFormatTL2() {
				tctx := basictl.TL2ReadContext{}
				_, err = args.ReadTL2(r, &tctx)
			} else {
				_, err = args.Read(r)
			}
			if err != nil {
				return internal.ErrorServerRead("barsic.shutdown", err)
			}
			ctx = hctx.WithContext(ctx)
			ret, err := h.Shutdown(ctx, args)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.shutdown", err)
			}
			hctx.Response, err = args.WriteResult(hctx.Response, ret)
			if err != nil {
				return internal.ErrorServerWriteResult("barsic.shutdown", err)
			}
			return nil
		}
	case 0x961de3bd: // barsic.skip
		hctx.RequestFunctionName = "barsic.skip"
		if h.RawSkip != nil && !hctx.BodyFormatTL2() {
			hctx.Request = r
			err = h.RawSkip(ctx, hctx)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.skip", err)
			}
			return nil
		}
		if h.Skip != nil {
			var args Skip
			if hctx.BodyFormatTL2() {
				tctx := basictl.TL2ReadContext{}
				_, err = args.ReadTL2(r, &tctx)
			} else {
				_, err = args.Read(r)
			}
			if err != nil {
				return internal.ErrorServerRead("barsic.skip", err)
			}
			ctx = hctx.WithContext(ctx)
			ret, err := h.Skip(ctx, args)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.skip", err)
			}
			hctx.Response, err = args.WriteResult(hctx.Response, ret)
			if err != nil {
				return internal.ErrorServerWriteResult("barsic.skip", err)
			}
			return nil
		}
	case 0xaf4c92d8: // barsic.split
		hctx.RequestFunctionName = "barsic.split"
		if h.RawSplit != nil && !hctx.BodyFormatTL2() {
			hctx.Request = r
			err = h.RawSplit(ctx, hctx)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.split", err)
			}
			return nil
		}
		if h.Split != nil {
			var args Split
			if hctx.BodyFormatTL2() {
				tctx := basictl.TL2ReadContext{}
				_, err = args.ReadTL2(r, &tctx)
			} else {
				_, err = args.Read(r)
			}
			if err != nil {
				return internal.ErrorServerRead("barsic.split", err)
			}
			ctx = hctx.WithContext(ctx)
			ret, err := h.Split(ctx, args)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.split", err)
			}
			hctx.Response, err = args.WriteResult(hctx.Response, ret)
			if err != nil {
				return internal.ErrorServerWriteResult("barsic.split", err)
			}
			return nil
		}
	case 0x85ca2340: // barsic.start
		hctx.RequestFunctionName = "barsic.start"
		if h.RawStart != nil && !hctx.BodyFormatTL2() {
			hctx.Request = r
			err = h.RawStart(ctx, hctx)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.start", err)
			}
			return nil
		}
		if h.Start != nil {
			var args Start
			if hctx.BodyFormatTL2() {
				tctx := basictl.TL2ReadContext{}
				_, err = args.ReadTL2(r, &tctx)
			} else {
				_, err = args.Read(r)
			}
			if err != nil {
				return internal.ErrorServerRead("barsic.start", err)
			}
			ctx = hctx.WithContext(ctx)
			ret, err := h.Start(ctx, args)
			if rpc.IsHijackedResponse(err) {
				return err
			}
			if err != nil {
				return internal.ErrorServerHandle("barsic.start", err)
			}
			hctx.Response, err = args.WriteResult(hctx.Response, ret)
			if err != nil {
				return internal.ErrorServerWriteResult("barsic.start", err)
			}
			return nil
		}
	}
	return rpc.ErrNoHandler
}

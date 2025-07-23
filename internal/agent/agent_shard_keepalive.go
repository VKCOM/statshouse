// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"context"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
)

func (s *ShardReplica) IsAlive() bool {
	return s.alive.Load()
}

func (s *ShardReplica) appendlastSendSuccessfulLocked(success bool) int { // returns how many successes in the list
	if len(s.lastSendSuccessful) >= s.config.LivenessResponsesWindowLength {
		s.lastSendSuccessful = append(s.lastSendSuccessful[:0], s.lastSendSuccessful[1:s.config.LivenessResponsesWindowLength]...)
	}
	s.lastSendSuccessful = append(s.lastSendSuccessful, success)
	succ := 0
	for _, ss := range s.lastSendSuccessful {
		if ss {
			succ++
		}
	}
	return succ
}

func (s *ShardReplica) recordSendResult(success bool) {
	if !s.alive.Load() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	succ := s.appendlastSendSuccessfulLocked(success)
	if len(s.lastSendSuccessful) == s.config.LivenessResponsesWindowLength && succ < s.config.LivenessResponsesWindowSuccesses {
		s.alive.Store(false)
		s.lastSendSuccessful = s.lastSendSuccessful[:0]
		s.agent.logF("Aggregator Dead: # of successful recent sends dropped below %d out of %d for shard %d replica %d", s.config.LivenessResponsesWindowSuccesses, s.config.LivenessResponsesWindowLength, s.ShardKey, s.ReplicaKey)
	}
}

func (s *ShardReplica) recordKeepLiveResult(err error, dur time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	success := err == nil && dur < s.config.KeepAliveSuccessTimeout // we require strict response time here

	succ := s.appendlastSendSuccessfulLocked(success)
	if succ == s.config.LivenessResponsesWindowLength {
		s.agent.logF("Aggregator Alive: # of successful keepalive sends reached %d out of %d for shard %d replica %d", s.config.LivenessResponsesWindowLength, s.config.LivenessResponsesWindowLength, s.ShardKey, s.ReplicaKey)
		s.alive.Store(true)
	}
}

func (s *ShardReplica) sendKeepLive() error {
	now := time.Now()
	ctx, cancel := context.WithDeadline(context.Background(), now.Add(time.Second*60)) // Relatively large timeout here
	defer cancel()

	args := tlstatshouse.SendKeepAlive2{}
	s.fillProxyHeader(&args.FieldsMask, &args.Header)
	// It is important that keep alive will not be successful when shards are not configured correctly
	// We do not use FailIfNoConnect:true, because we want exponential backoff to connection attempts in rpc Client.
	// We do not want to implement yet another exponential backoff here.
	var ret string
	client := s.client()
	err := client.SendKeepAlive2(ctx, args, nil, &ret)
	dur := time.Since(now)
	s.recordKeepLiveResult(err, dur)
	return err
}

// We see no reason to carefully stop/wait this loop at shutdown
func (s *ShardReplica) goLiveChecker() {
	// We have separate loops instead of using flushBuckets agent loop, because sendKeepLive can block on connect for
	// very long time, and it is convenient if it blocks there
	now := time.Now()
	backoffTimeout := time.Duration(0)
	for {
		tick := time.After(data_model.TillStartOfNextSecond(now))
		now = <-tick // We synchronize with calendar second boundary
		if s.alive.Load() {
			continue
		}
		if err := s.sendKeepLive(); err == nil {
			backoffTimeout = 0
			continue
		}
		backoffTimeout = data_model.NextBackoffDuration(backoffTimeout)
		time.Sleep(backoffTimeout)
	}
}

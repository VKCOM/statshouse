// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"cmp"
	"context"
	"sync"
	"time"

	"github.com/google/btree"
)

// longpoll tree is used to find the longpoll with the earliest deadline
type longpollTree struct {
	mu        sync.Mutex // Protects timeouts
	timeouts  *btree.BTreeG[LongpollHandle]
	updatesCh chan struct{}
}

func handleLess(l, r LongpollHandle) bool {
	if res := cmp.Compare(l.Deadline, r.Deadline); res != 0 {
		return cmp.Less(l.Deadline, r.Deadline)
	}
	if res := cmp.Compare(l.QueryID, r.QueryID); res != 0 {
		return cmp.Less(l.QueryID, r.QueryID)
	}
	// Deadlines could be same
	// queries ids could be same in different connections
	return l.CommonConn.ConnectionID() < r.CommonConn.ConnectionID()
}

func newLongpollTree() *longpollTree {
	return &longpollTree{
		timeouts:  btree.NewG(10, handleLess),
		updatesCh: make(chan struct{}, 1),
	}
}

func (lt *longpollTree) AddLongpoll(lh LongpollHandle) {
	lt.mu.Lock()
	oldMin, exists := lt.timeouts.Min()
	lt.timeouts.ReplaceOrInsert(lh)
	lt.mu.Unlock()

	if exists && oldMin.Deadline <= lh.Deadline {
		// Don't need to send an update, anyway already waiting
		return
	}

	select {
	case lt.updatesCh <- struct{}{}:
		// This updates ch actually works as a condvar and also the channel is buffered
		// so if there is already a value in the channel there is no sense to write more
	default:
	}
}

func (lt *longpollTree) DeleteLongpoll(lh LongpollHandle) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	// It might delete the longpoll request that we are already waiting
	// for, but in this case, nothing bad will happen. This is because
	// delete is only called once the handle has been removed from the
	// longpolls map, so sending an empty response will do nothing.
	lt.timeouts.Delete(lh)
}

func (lt *longpollTree) Size() int {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	return lt.timeouts.Len()
}

func (lt *longpollTree) deleteExpiredUnlocked(expired []LongpollHandle) []LongpollHandle {
	// NOTE: be careful here, unixNano should be used, not unix
	now := time.Now().UnixNano()
	for lt.timeouts.Len() != 0 {
		if lh, exists := lt.timeouts.Min(); exists {
			if now >= lh.Deadline {
				// longpoll is expired
				expired = append(expired, lh)
				lt.timeouts.DeleteMin()
			} else {
				// There are no more expired longpolls
				return expired
			}
		}
	}

	return expired
}

func (lt *longpollTree) LongpollCheckLoop(ctx context.Context) {
	var expired []LongpollHandle
	longpollTimer := time.NewTimer(time.Second)
	longpollTimer.Stop()
	defer longpollTimer.Stop()

	for {
		lt.mu.Lock()
		expired = lt.deleteExpiredUnlocked(expired[:0])
		minLp, exists := lt.timeouts.Min()
		lt.mu.Unlock()

		// Send responses to all expired longpolls
		for _, lh := range expired {
			lh.CommonConn.SendEmptyResponse(lh)
		}

		if exists {
			// There are some waiting longpolls
			longpollTimer.Reset(time.Until(time.Unix(0, minLp.Deadline)))
		}

		select {
		case <-longpollTimer.C:
			continue
		case <-lt.updatesCh:
			continue
		case <-ctx.Done():
			return
		}
	}
}

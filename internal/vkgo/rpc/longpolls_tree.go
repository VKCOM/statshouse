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

type longpollTreeItem struct {
	lh       LongpollHandle
	deadline int64 // UnixNano() timestamp
}

// longpoll tree is used to find the longpoll with the earliest deadline
type longpollTree struct {
	mu        sync.Mutex // Protects timeouts
	timeouts  *btree.BTreeG[longpollTreeItem]
	updatesCh chan struct{}
}

func handleLess(l, r longpollTreeItem) bool {
	if res := cmp.Compare(l.deadline, r.deadline); res != 0 {
		return cmp.Less(l.deadline, r.deadline)
	}
	if res := cmp.Compare(l.lh.QueryID, r.lh.QueryID); res != 0 {
		return cmp.Less(l.lh.QueryID, r.lh.QueryID)
	}
	// Deadlines could be same
	// queries ids could be same in different connections
	return l.lh.CommonConn.ConnectionID() < r.lh.CommonConn.ConnectionID()
}

func newLongpollTree() *longpollTree {
	return &longpollTree{
		timeouts:  btree.NewG(10, handleLess),
		updatesCh: make(chan struct{}, 1),
	}
}

func (lt *longpollTree) AddLongpoll(lh LongpollHandle, deadline int64) {
	lti := longpollTreeItem{lh: lh, deadline: deadline}
	lt.mu.Lock()
	oldMin, exists := lt.timeouts.Min()
	lt.timeouts.ReplaceOrInsert(lti)
	lt.mu.Unlock()

	if exists && oldMin.deadline <= lti.deadline {
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

func (lt *longpollTree) DeleteLongpoll(lh LongpollHandle, deadline int64) {
	lti := longpollTreeItem{lh: lh, deadline: deadline}
	lt.mu.Lock()
	defer lt.mu.Unlock()

	// It might delete the longpoll request that we are already waiting
	// for, but in this case, nothing bad will happen. This is because
	// delete is only called once the handle has been removed from the
	// longpolls map, so sending an empty response will do nothing.
	lt.timeouts.Delete(lti)
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
		if lti, exists := lt.timeouts.Min(); exists {
			if now < lti.deadline {
				// There are no more expired longpolls
				return expired
			}
			// longpoll is expired
			expired = append(expired, lti.lh)
			lt.timeouts.DeleteMin()
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
			longpollTimer.Reset(time.Until(time.Unix(0, minLp.deadline)))
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

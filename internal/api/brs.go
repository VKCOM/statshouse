// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouseApi"
)

func NewBigResponseStorage(maxInMemoryChunksCount int, flushInterval time.Duration) *BigResponseStorage {
	brs := &BigResponseStorage{
		m:               map[int64]*BigResponseEntry{},
		sem:             semaphore.NewWeighted(int64(maxInMemoryChunksCount)),
		cleanTicker:     time.NewTicker(flushInterval),
		cleanTickerStop: make(chan struct{}),
	}

	go brs.run()

	return brs
}

type BigResponseEntry struct {
	chunks []tlstatshouseApi.Series
	owner  string
	ttl    time.Time
}

type BigResponseStorage struct {
	mu              sync.Mutex
	m               map[int64]*BigResponseEntry
	sem             *semaphore.Weighted
	cleanTicker     *time.Ticker
	cleanTickerStop chan struct{}
	closeOnce       sync.Once
}

func (brs *BigResponseStorage) Set(ctx context.Context, rid int64, owner string, chunks []tlstatshouseApi.Series, lifeDuration time.Duration) error {
	err := brs.sem.Acquire(ctx, int64(len(chunks)))
	if err != nil {
		return err
	}

	brs.mu.Lock()
	defer brs.mu.Unlock()

	brs.m[rid] = &BigResponseEntry{
		chunks: chunks,
		owner:  owner,
		ttl:    time.Now().Add(lifeDuration),
	}

	return nil
}

func (brs *BigResponseStorage) Get(id int64) (*BigResponseEntry, bool) {
	brs.mu.Lock()
	defer brs.mu.Unlock()

	r, ok := brs.m[id]
	return r, ok
}

func (brs *BigResponseStorage) Release(id int64) int {
	brs.mu.Lock()
	defer brs.mu.Unlock()

	r, ok := brs.m[id]
	if !ok {
		return 0
	}

	released := len(r.chunks)

	delete(brs.m, id)
	brs.sem.Release(int64(released))

	return released
}

func (brs *BigResponseStorage) flushExpired() {
	brs.mu.Lock()
	defer brs.mu.Unlock()

	now := time.Now()
	cleaned := 0

	for rid, br := range brs.m {
		if now.After(br.ttl) {
			cleaned += len(br.chunks)
			delete(brs.m, rid)
		}
	}

	brs.sem.Release(int64(cleaned))
}

func (brs *BigResponseStorage) run() {
	for {
		select {
		case <-brs.cleanTicker.C:
			brs.flushExpired()
		case <-brs.cleanTickerStop:
			return
		}
	}
}

func (brs *BigResponseStorage) Close() {
	brs.closeOnce.Do(func() {
		close(brs.cleanTickerStop)
	})
}

func (brs *BigResponseStorage) Count() int {
	brs.mu.Lock()
	defer brs.mu.Unlock()

	count := 0
	for _, br := range brs.m {
		count += len(br.chunks)
	}

	return count
}

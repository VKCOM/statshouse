// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package mapping

import (
	"sync"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
)

type mapRequest struct {
	metric tlstatshouse.MetricBytes      // in
	result data_model.MappedMetricHeader // out, contains probable error in IngestionStatus
	cb     data_model.MapCallbackFunc    // will be called only when processing required enqueue
}

type metricQueue struct {
	name     string
	progress func(*mapRequest)      // called with mutex unlocked
	isDone   func(*mapRequest) bool // called with mutex locked, should be wait-free
	finish   func(*mapRequest)      // called with mutex unlocked

	// separate progress and done are because progress in one mapping request can make others done

	cond              sync.Cond
	mu                sync.Mutex
	queue             map[int32][]*mapRequest // map iteration is the source of quasi-fairness. length of slices is never 0 here
	size              int
	maxMetrics        int
	maxMetricRequests int
	shouldStop        bool
	stopped           chan struct{}

	pool []tlstatshouse.MetricBytes // for reuse
}

func newMetricQueue(
	name string,
	progress func(*mapRequest),
	isDone func(*mapRequest) bool,
	finish func(*mapRequest),
	maxMetrics int,
	maxMetricRequests int,
) *metricQueue {
	q := &metricQueue{
		name:              name,
		progress:          progress,
		isDone:            isDone,
		finish:            finish,
		queue:             map[int32][]*mapRequest{},
		maxMetrics:        maxMetrics,
		maxMetricRequests: maxMetricRequests,
		stopped:           make(chan struct{}),
	}
	q.cond.L = &q.mu
	go q.run()
	return q
}

func (mq *metricQueue) stop() {
	mq.mu.Lock()
	mq.shouldStop = true
	mq.mu.Unlock()
	mq.cond.Signal()
	<-mq.stopped
}

// to avoid allocations in fastpath, returns ingestion statuses only. They are converted to errors before printing, if needed
// we do not want allocation if queues overloaded
func (mq *metricQueue) enqueue(metric *tlstatshouse.MetricBytes, result *data_model.MappedMetricHeader, cb data_model.MapCallbackFunc) (done bool) {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	if mq.shouldStop {
		return false
	}

	q, ok := mq.queue[result.Key.Metric]
	if !ok {
		if len(mq.queue) >= mq.maxMetrics {
			result.IngestionStatus = format.TagValueIDSrcIngestionStatusErrMapGlobalQueueOverload
			return true
		}
	}
	if len(q) >= mq.maxMetricRequests {
		result.IngestionStatus = format.TagValueIDSrcIngestionStatusErrMapPerMetricQueueOverload
		return true
	}

	mq.queue[result.Key.Metric] = append(q, &mapRequest{metric: *metric, result: *result, cb: cb})
	// We now own all slices inside metric. We replace them from our pool, or set to nil
	if len(mq.pool) != 0 {
		*metric = mq.pool[len(mq.pool)-1]
		mq.pool = mq.pool[:len(mq.pool)-1]
	} else {
		*metric = tlstatshouse.MetricBytes{}
	}
	mq.size++
	mq.cond.Signal()

	return false
}

func (mq *metricQueue) run() {
	var localPool []tlstatshouse.MetricBytes
	var newQ []*mapRequest // for reuse
	var done []*mapRequest // for reuse
	for {
		mq.mu.Lock()
		for !(mq.shouldStop || mq.size > 0) {
			mq.cond.Wait()
		}
		if mq.shouldStop {
			mq.mu.Unlock()
			close(mq.stopped)
			return
		}
		mq.pool = append(mq.pool, localPool...) // move from local pool under lock
		localPool = localPool[:0]
		var (
			chosenM int32
			chosenR *mapRequest
		)
		for m, q := range mq.queue {
			chosenM = m
			chosenR = q[0]
			break
		}
		mq.mu.Unlock()

		mq.progress(chosenR)

		mq.mu.Lock()
		curQ := mq.queue[chosenM]
		// mapping of single key can release many items from the queue
		for _, r := range curQ {
			if mq.isDone(r) {
				done = append(done, r)
			} else {
				newQ = append(newQ, r)
			}
		}
		if len(newQ) > 0 {
			mq.queue[chosenM] = newQ
			newQ = curQ[:0]
		} else {
			delete(mq.queue, chosenM)
		}
		mq.size -= len(done)
		mq.mu.Unlock()

		for _, r := range done {
			mq.finish(r)
			r.metric.Reset()                        // strictly not required
			localPool = append(localPool, r.metric) // We pool fields, not metric itself
		}
		done = done[:0]
	}
}

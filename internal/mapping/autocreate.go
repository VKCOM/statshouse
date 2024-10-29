// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package mapping

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
)

const (
	// interval between attempts to create a specific metric
	autoCreateRetryInterval = 5 * time.Minute
	// maximum number of tasks in work
	autoCreateTaskLimit = 5_000
)

var (
	errAutoCreateTaskLimitExceeded = fmt.Errorf("task limit exceeded")
	errAutoCreateTagLimitExceeded  = fmt.Errorf("tag limit exceeded")
)

type AutoCreateFunc func(context.Context, tlstatshouse.AutoCreate) error

type AutoCreate struct {
	journal    format.MetaStorageInterface
	fn         AutoCreateFunc
	mu         sync.RWMutex
	work       map[string]*autoCreateTask // by metric name, protected by "mu"
	ctx        context.Context
	shutdownFn func()
}

type autoCreateTask struct {
	kind        string
	tags        map[string]int
	dueTime     time.Time
	resolution  int
	description string
}

func NewAutoCreate(journal format.MetaStorageInterface, fn AutoCreateFunc) *AutoCreate {
	ac := AutoCreate{
		journal: journal,
		fn:      fn,
		work:    map[string]*autoCreateTask{},
	}
	ac.ctx, ac.shutdownFn = context.WithCancel(context.Background())
	go ac.goWork()
	return &ac
}

func (ac *AutoCreate) Shutdown() {
	ac.shutdownFn()
}

func (ac *AutoCreate) AutoCreateMetric(bytes *tlstatshouse.MetricBytes, description string, resolution int, now time.Time) error {
	ac.mu.RLock()
	task := ac.work[string(bytes.Name)]
	taskCount := len(ac.work)
	ac.mu.RUnlock()
	if task != nil {
		return nil // fast path: RLock, no allocations
	}
	if taskCount >= autoCreateTaskLimit {
		return errAutoCreateTaskLimitExceeded // fast path: RLock, no allocations
	}
	// slow path: Lock, might allocate
	ac.mu.Lock()
	defer ac.mu.Unlock()
	task = ac.work[string(bytes.Name)]
	if task != nil {
		return nil
	}
	if len(ac.work) >= autoCreateTaskLimit {
		return errAutoCreateTaskLimitExceeded
	}
	ac.work[string(bytes.Name)] = newAutoCreateTask(bytes, description, resolution, now)
	return nil
}

func (ac *AutoCreate) AutoCreateTag(bytes *tlstatshouse.MetricBytes, tagBytes []byte, now time.Time) error {
	ac.mu.RLock()
	task := ac.work[string(bytes.Name)]
	done := false
	tagCount := 0
	if task != nil {
		_, done = task.tags[string(tagBytes)]
		tagCount = len(task.tags)
	}
	taskCount := len(ac.work)
	ac.mu.RUnlock()
	if done {
		return nil // fast path: RLock, no allocations
	}
	if task == nil && taskCount >= autoCreateTaskLimit {
		return errAutoCreateTaskLimitExceeded // fast path: RLock, no allocations
	}
	if tagCount >= format.MaxDraftTags {
		return errAutoCreateTagLimitExceeded // fast path: RLock, no allocations
	}
	// slow path: Lock, might allocate
	ac.mu.Lock()
	defer ac.mu.Unlock()
	task = ac.work[string(bytes.Name)]
	if task == nil {
		if len(ac.work) >= autoCreateTaskLimit {
			return errAutoCreateTaskLimitExceeded
		}
		task = newAutoCreateTask(bytes, "", 0, now)
		ac.work[string(bytes.Name)] = task
	}
	if _, ok := task.tags[string(tagBytes)]; ok {
		return nil
	}
	if len(task.tags) >= format.MaxDraftTags {
		return errAutoCreateTagLimitExceeded
	}
	// remember tag insert order
	task.tags[string(tagBytes)] = len(task.tags)
	task.dueTime = now
	return nil
}

func newAutoCreateTask(bytes *tlstatshouse.MetricBytes, description string, resolution int, now time.Time) *autoCreateTask {
	kind := format.MetricKindCounter
	if bytes.IsSetUnique() {
		kind = format.MetricKindUnique
	} else if bytes.IsSetValue() {
		kind = format.MetricKindValue
	}
	return &autoCreateTask{
		kind:        kind,
		tags:        map[string]int{},
		dueTime:     now,
		resolution:  resolution,
		description: description,
	}
}

func (ac *AutoCreate) goWork() {
	// get a resettable timer
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	// main loop
	d := time.Second  // timer interval
	now := time.Now() // gets updated once per outer loop iteration
	for {
		// process items with due time up to now
		for {
			metric, task := ac.getMetricToCreate(now) // takes RLock
			if task == nil || ac.done() {
				// no work for now
				break
			}
			args, ok := ac.synchronizeWithJournal(metric, task, now) // takes Lock
			if !ok {
				// already in journal
				continue
			}
			err := ac.createMetric(args) // doesn't lock
			if err != nil {
				// failed, double timer interval
				d *= 2
				if d > data_model.KeepAliveMaxBackoff {
					d = data_model.KeepAliveMaxBackoff
				}
				if !data_model.SilentRPCError(err) {
					log.Printf("metric auto-create failed {name: %q, kind: %q, tag count: %d}: %v", args.Metric, args.Kind, len(args.Tags), err)
				}
				break
			}
			// succeeded, reset timer interval
			d = time.Second
		}
		// wait next second (or longer if backoff)
		timer.Reset(d)
		select {
		case <-ac.ctx.Done():
			return
		case now = <-timer.C:
		}
	}
}

func (ac *AutoCreate) getMetricToCreate(now time.Time) (string, *autoCreateTask) {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	for k, v := range ac.work {
		if !v.dueTime.After(now) {
			return k, v
		}
	}
	return "", nil
}

func (ac *AutoCreate) synchronizeWithJournal(metric string, task *autoCreateTask, now time.Time) (tlstatshouse.AutoCreate, bool) {
	meta := ac.journal.GetMetaMetricByName(metric)
	ac.mu.Lock()
	defer ac.mu.Unlock()
	if meta == nil {
		task.dueTime = now.Add(autoCreateRetryInterval)
	} else {
		// remove existing mappings from the task
		tags := make(map[string]int, len(task.tags))
		for k, v := range task.tags {
			if _, ok := meta.Name2Tag[k]; !ok {
				tags[k] = v
			}
		}
		task.tags = tags
		// dismiss task if no work left
		if len(task.tags) == 0 {
			delete(ac.work, metric)
			return tlstatshouse.AutoCreate{}, false
		}
		// set next attempt time
		task.dueTime = now.Add(autoCreateRetryInterval)
		// check there is at least one unmapped tag
		found := len(meta.Tags) < format.MaxTags
		for i := 1; !found && i < len(meta.Tags); i++ {
			found = len(meta.Tags[i].Name) == 0
		}
		if !found {
			// All tags are mapped.
			// It makes no sense to send a request - aggregator won't be able to handle it.
			// Don't remove the task so that all subsequent requests for these tags will
			// follow fast path (RLock, no allocations).
			return tlstatshouse.AutoCreate{}, false
		}
	}
	// build args
	args := tlstatshouse.AutoCreate{
		Metric: metric,
		Kind:   task.kind,
		Tags:   make([]string, 0, len(task.tags)),
	}
	for v := range task.tags {
		args.Tags = append(args.Tags, v)
	}
	sort.Slice(args.Tags, func(i, j int) bool {
		return task.tags[args.Tags[i]] < task.tags[args.Tags[j]]
	})
	if task.resolution != 0 {
		args.SetResolution(int32(task.resolution))
	}
	if len(task.description) != 0 {
		args.SetDescription(task.description)
	}
	return args, true
}

func (ac *AutoCreate) createMetric(args tlstatshouse.AutoCreate) error {
	ctx, cancel := context.WithTimeout(ac.ctx, time.Minute)
	defer cancel()
	return ac.fn(ctx, args)
}

func (ac *AutoCreate) done() bool {
	select {
	case <-ac.ctx.Done():
		return true
	default:
		return false
	}
}

package sqlite

import (
	"errors"
	"sync"
)

type applyQueue struct {
	q      []*delayedApply
	qBytes int
	cache  sync.Pool

	maxBytes int
	dbOffset int64
}

type delayedApply struct {
	body []byte
	skip int64
}

func newApplyQueue(old *applyQueue, dbOffset int64, maxBytes int) *applyQueue {
	if old != nil {
		old.maxBytes = maxBytes
		old.qBytes = 0
		old.dbOffset = dbOffset
		return old
	}
	return &applyQueue{
		dbOffset: dbOffset,
		maxBytes: maxBytes,
		cache: sync.Pool{
			New: func() interface{} {
				return &delayedApply{}
			},
		},
	}
}

func (q *applyQueue) applyAllChanges(applyFunc func(payload []byte) (newOffset int64, err error), skip func(skipLen int64) int64) error {
	for _, apply := range q.q {
		if apply.skip > 0 {
			_ = skip(apply.skip)
		} else {
			_, err := applyFunc(apply.body)
			if err != nil {
				return err
			}
		}
		apply.skip = 0
		apply.body = apply.body[:0]
		q.cache.Put(apply)
	}
	q.q = q.q[:0]
	return nil
}

func (q *applyQueue) addNewBody(body []byte, err error) (int64, error) {
	if q.qBytes+len(body) > q.maxBytes {
		return q.dbOffset, errors.New("buffer size limit exceeded")
	}
	apply := q.cache.Get().(*delayedApply)
	apply.body = append(apply.body, body...)
	q.q = append(q.q, apply)
	q.qBytes += len(body)
	q.dbOffset += int64(len(body))
	return q.dbOffset, err
}

func (q *applyQueue) addNewSkip(skip int64) int64 {
	apply := q.cache.Get().(*delayedApply)
	apply.skip = skip
	q.q = append(q.q, apply)
	q.dbOffset += skip
	return q.dbOffset
}

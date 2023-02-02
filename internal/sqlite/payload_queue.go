package sqlite

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/vkcom/statshouse-go"
)

type applyQueue struct {
	q      []delayedApply
	qBytes int64
	cache  sync.Pool

	maxBytes int64
	dbOffset int64
}

type delayedApply struct {
	body *[]byte
	skip int64
}

func newApplyQueue(old *applyQueue, dbOffset int64, maxBytes int64, stats *StatsOptions) *applyQueue {
	if old != nil {
		old.maxBytes = maxBytes
		atomic.StoreInt64(&old.qBytes, 0)
		old.qBytes = 0
		old.dbOffset = dbOffset
		return old
	}
	q := &applyQueue{
		dbOffset: dbOffset,
		maxBytes: maxBytes,
		cache: sync.Pool{
			New: func() interface{} {
				bytes := make([]byte, 0, 1024)
				return &bytes
			},
		},
	}
	statshouse.StartRegularMeasurement(func(registry *statshouse.Registry) {
		stats.applyQueueSize(registry, atomic.LoadInt64(&q.qBytes))
	})
	return q
}

func (q *applyQueue) applyAllChanges(applyFunc func(payload []byte) (newOffset int64, err error), skip func(skipLen int64) (int64, error)) error {
	for _, apply := range q.q {
		if apply.skip > 0 {
			_, err := skip(apply.skip)
			if err != nil {
				return err
			}
		} else {
			_, err := applyFunc(*apply.body)
			if err != nil {
				return err
			}
			reset := (*apply.body)[:0]
			q.cache.Put(&reset)
		}
	}
	q.q = q.q[:0]
	return nil
}

func (q *applyQueue) addNewBody(body []byte, err error) (int64, error) {
	bytesLen := atomic.LoadInt64(&q.qBytes)
	if bytesLen+int64(len(body)) > q.maxBytes {
		return q.dbOffset, errors.New("buffer size limit exceeded")
	}
	bytes := *(q.cache.Get().(*[]byte))
	bytes = append(bytes, body...)
	q.q = append(q.q, delayedApply{body: &bytes})
	atomic.AddInt64(&q.qBytes, int64(len(body)))
	q.dbOffset += int64(len(body))
	return q.dbOffset, err
}

func (q *applyQueue) addNewSkip(skip int64) int64 {
	q.q = append(q.q, delayedApply{skip: skip})
	q.dbOffset += skip
	return q.dbOffset
}

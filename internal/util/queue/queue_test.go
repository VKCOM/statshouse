package queue

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	rand2 "k8s.io/apimachinery/pkg/util/rand"
	"pgregory.net/rand"
)

// n горутин, захватывают и отпускают блокировку
func Queue_Race(t *testing.T, q *Queue, max, perClient int) {
	//q := NewQ(2)
	wg := sync.WaitGroup{}
	n := runtime.GOMAXPROCS(0)
	clients := 2 * perClient
	loops := 10000 / n
	currentActive := atomic.NewInt64(0)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			for i := 0; i < loops; i++ {
				err := q.Acquire(context.Background(), token)
				if err != nil {
					t.Error(err.Error())
				}
				currentActive.Add(1)
				require.LessOrEqual(t, currentActive.Load(), int64(max))
				time.Sleep(time.Duration(10000 + rand.Int63n(int64(time.Millisecond))))
				currentActive.Add(-1)
				q.Release()
			}
		}(strconv.FormatInt(int64(i%clients), 10))
	}
	wg.Wait()
	require.Equal(t, int64(0), q.activeQuery)
	require.Equal(t, 0, len(q.waitingUsersByName))
	require.Equal(t, 0, q.waitingUsersByPriority.Len())
}

// n горутин, захватывают и отпускают блокировку, некоторые таймаутят
func Queue_Random_Timeout_Race(t *testing.T, q *Queue, max, perClient int) {
	//q := NewQ(2)
	wg := sync.WaitGroup{}
	n := runtime.GOMAXPROCS(0)
	loops := 10000 / n
	clients := 2 * perClient
	mx := sync.Mutex{}
	currentActive := atomic.NewInt64(0)
	ctxMap := map[context.Context]func(){}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			for i := 0; i < loops; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				mx.Lock()
				ctxMap[ctx] = cancel
				mx.Unlock()
				releaseCtx := func() {
					mx.Lock()
					delete(ctxMap, ctx)
					mx.Unlock()
				}
				err := q.Acquire(ctx, token)
				if err != nil {
					require.ErrorIs(t, err, context.Canceled)
					releaseCtx()
					continue
				}
				currentActive.Add(1)
				require.LessOrEqual(t, currentActive.Load(), int64(max))
				time.Sleep(10000 + time.Duration(rand.Int63n(int64(time.Millisecond))))
				if rand2.Intn(10) < 3 {
					mx.Lock()
					for _, c := range ctxMap {
						cancel = c
						break
					}
					mx.Unlock()
					cancel()

				}
				time.Sleep(time.Duration(rand.Int63n(int64(time.Millisecond))))
				currentActive.Add(-1)
				q.Release()
				releaseCtx()

			}
		}(strconv.FormatInt(int64(i%clients), 10))
	}
	wg.Wait()
	require.Equal(t, int64(0), q.activeQuery)
	require.Equal(t, 0, len(q.waitingUsersByName))
	require.Equal(t, 0, q.waitingUsersByPriority.Len())

}

func timeoutCtx(duration time.Duration) context.Context {
	ctx, _ := context.WithTimeout(context.Background(), duration)
	return ctx
}

func Timeout(t *testing.T, q *Queue) {
	//q := NewQ(1)
	err := q.Acquire(context.Background(), "0")
	defer q.Release()
	require.NoError(t, err)
	err = q.Acquire(timeoutCtx(time.Second), "0")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TimeoutSeveral(t *testing.T, q *Queue) {
	//q := NewQ(1)
	err := q.Acquire(context.Background(), "0")
	defer q.Release()
	require.NoError(t, err)
	errGroup, _ := errgroup.WithContext(context.Background())
	errGroup.Go(func() error {
		err := q.Acquire(timeoutCtx(time.Second), "0")
		return err
	})
	errGroup.Go(func() error {
		err := q.Acquire(timeoutCtx(time.Second), "0")
		return err
	})
	require.ErrorIs(t, errGroup.Wait(), context.DeadlineExceeded)
}

func PushNewQuery(t *testing.T, q *Queue) {
	//q := NewQ(1)
	err := q.Acquire(context.Background(), "0")
	require.NoError(t, err)
	errGroup, _ := errgroup.WithContext(context.Background())
	errGroup.Go(func() error {
		err := q.Acquire(context.Background(), "0")
		return err
	})
	errGroup.Go(func() error {
		err := q.Acquire(context.Background(), "0")
		return err
	})
	time.Sleep(time.Second)
	q.Release()
	time.Sleep(time.Second)
	q.Release()
	require.NoError(t, errGroup.Wait())
}

func Timeout1(t *testing.T, q *Queue) {
	//q := NewQ(1)
	err := q.Acquire(context.Background(), "0")
	defer q.Release()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	group, _ := errgroup.WithContext(timeoutCtx(time.Second * 10))
	group.Go(func() error {
		err := q.Acquire(ctx, "0")
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	})
	group.Go(func() error {
		err := q.Acquire(context.Background(), "1")
		return err
	})
	time.Sleep(time.Millisecond * 5)
	cancel()
	time.Sleep(time.Millisecond * 5)
	q.Release()
	require.NoError(t, group.Wait())
}

package util

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	rand2 "k8s.io/apimachinery/pkg/util/rand"
	"pgregory.net/rand"
)

// n горутин, захватывают и отпускают блокировку
func Queue_Race(t *testing.T, q Queue, perClient int) {
	//q := NewQ(2)
	wg := sync.WaitGroup{}
	n := runtime.GOMAXPROCS(0)
	clients := n * perClient
	loops := 10000 / n
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			for i := 0; i < loops; i++ {
				qry, err := q.Acquire(context.Background(), token)
				time.Sleep(time.Duration(1000000 + rand.Int63n(int64(time.Millisecond))))
				if err != nil {
					t.Errorf(err.Error())
				}
				q.Release(qry)
			}
		}(strconv.FormatInt(int64(i%clients), 10))
	}
	wg.Wait()
}

// n горутин, захватывают и отпускают блокировку, некоторые таймаутят
func Queue_Random_Timeout_Race(t *testing.T, q Queue, perClient int) {
	//q := NewQ(2)
	wg := sync.WaitGroup{}
	n := runtime.GOMAXPROCS(0)
	loops := 10000 / n
	clients := n * perClient
	mx := sync.Mutex{}
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
				qry, err := q.Acquire(ctx, token)
				if err != nil {
					require.ErrorIs(t, err, context.Canceled)
					releaseCtx()
					continue
				}
				time.Sleep(1000000 + time.Duration(rand.Int63n(int64(time.Millisecond))))
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
				q.Release(qry)
				releaseCtx()

			}
		}(strconv.FormatInt(int64(i%clients), 10))
	}
	wg.Wait()
}

func timeoutCtx(duration time.Duration) context.Context {
	ctx, _ := context.WithTimeout(context.Background(), duration)
	return ctx
}

func Timeout(t *testing.T, q Queue) {
	//q := NewQ(1)
	qry, err := q.Acquire(context.Background(), "0")
	defer q.Release(qry)
	require.NoError(t, err)
	_, err = q.Acquire(timeoutCtx(time.Second), "0")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func Timeout1(t *testing.T, q Queue) {
	//q := NewQ(1)
	qry, err := q.Acquire(context.Background(), "0")
	defer q.Release(qry)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	group, _ := errgroup.WithContext(timeoutCtx(time.Second * 10))
	group.Go(func() error {
		_, err := q.Acquire(ctx, "0")
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	})
	group.Go(func() error {
		_, err := q.Acquire(context.Background(), "1")
		return err
	})
	time.Sleep(time.Millisecond * 5)
	cancel()
	time.Sleep(time.Millisecond * 5)
	q.Release(qry)
	require.NoError(t, group.Wait())
}

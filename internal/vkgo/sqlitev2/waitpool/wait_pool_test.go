package waitpool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func Test_priorityQ_deleteIfExists(t *testing.T) {
	pool := NewPool()
	n := 2
	k := 1000
	wg := sync.WaitGroup{}
	a := atomic.Int64{}
	for i := 0; i < n*3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for ; i < k; i++ {
				require.NoError(t, pool.Wait(context.Background(), int64(i)))
				a.Add(1)
			}

		}(i % n)
	}
	for i := 1; i < 1000; i++ {
		pool.Notify(int64(i))
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
}

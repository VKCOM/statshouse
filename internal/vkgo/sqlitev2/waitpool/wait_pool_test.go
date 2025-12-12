// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlitecache

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/VKCOM/statshouse/internal/pcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.uber.org/atomic"
)

func TestCacheIdentityMapping(t *testing.T) {
	const (
		cacheFilename      = "cache.db"
		diskTxDuration     = 100 * time.Millisecond
		diskCacheNamespace = "foo"
		maxMemCacheSize    = 100
		keyCount           = 1000
		getErrCount        = keyCount / 2
		cacheTTL           = 1 * time.Hour
		negativeCacheTTL   = -1 // disable negative caching
	)

	var loadTryCount atomic.Int32
	var loadOKCount atomic.Int32

	loader := func(_ context.Context, key string, _ interface{}) (pcache.Value, time.Duration, error) {
		if loadTryCount.Inc() <= getErrCount {
			return nil, 0, fmt.Errorf("sorry, try later")
		}
		loadOKCount.Inc()
		return pcache.StringToValue(key), time.Second * 100, nil
	}

	dc, err := OpenSqliteDiskCache(filepath.Join(t.TempDir(), cacheFilename), diskTxDuration)
	require.NoError(t, err)
	defer func() { _ = dc.Close() }()

	c := &pcache.Cache{
		Loader:                  loader,
		DiskCache:               dc,
		DiskCacheNamespace:      diskCacheNamespace,
		MaxMemCacheSize:         maxMemCacheSize,
		DefaultCacheTTL:         cacheTTL,
		DefaultNegativeCacheTTL: negativeCacheTTL,
		Empty: func() pcache.Value {
			var empty pcache.StringValue
			return &empty
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < getErrCount*2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			in := strconv.Itoa(i % getErrCount)
			out := c.GetOrLoad(time.Now(), in, nil)
			if out.Err == nil {
				assert.Equal(t, in, pcache.ValueToString(out.Value))
			}
		}(i)
	}
	wg.Wait() // no errors are possible after the wait

	for i := 0; i < keyCount*3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			in := strconv.Itoa(i % keyCount)
			out1 := c.GetOrLoad(time.Now(), in, nil)
			assert.NoError(t, out1.Err)
			assert.Equal(t, in, pcache.ValueToString(out1.Value))
			out2 := c.GetCachedString(time.Now(), in)
			assert.NoError(t, out2.Err)
			if out2.Found() { // can already be evicted by another goroutine
				assert.Equal(t, pcache.ValueToString(out1.Value), pcache.ValueToString(out2.Value))
			}
		}(i)
	}
	wg.Wait()

	// require.Equal(t, maxMemCacheSize, len(c.memCache)) - TODO, no more true, due to non-deterministic eviction
	require.Equal(t, int32(keyCount), loadOKCount.Load())

	c.Close()
}

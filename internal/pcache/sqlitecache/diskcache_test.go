// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlitecache

import (
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func BenchmarkDiskCacheSetGet(b *testing.B) {
	const (
		cacheFilename = "cache_bench.db"
		txDuration    = 100 * time.Millisecond
		namespace     = "ns"
		ttl           = 239 * time.Nanosecond
	)

	dc, err := OpenSqliteDiskCache(filepath.Join(b.TempDir(), cacheFilename), txDuration)
	require.NoError(b, err)
	defer func() { _ = dc.Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := time.Now()
		k := strconv.Itoa(i % (b.N/2 + 1)) // force upsert sometimes
		v := []byte(strconv.Itoa(i * i))
		err := dc.Set(namespace, k, v, t, ttl)
		require.NoError(b, err)
		vv, tt, dt, err, found := dc.Get(namespace, k)
		require.NoError(b, err)
		require.True(b, found)
		require.Equal(b, v, vv)
		require.WithinDuration(b, t, tt, 0)
		require.Equal(b, ttl, dt)
	}
}

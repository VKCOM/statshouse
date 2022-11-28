// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

func TestPMCCache(t *testing.T) {
	const (
		cacheFilename      = "cache.db"
		metricName         = "metric@cluster"
		diskTxDuration     = 100 * time.Millisecond
		maxMemCacheSize    = 100
		keyCount           = 1000
		cacheTTL           = 1 * time.Hour
		negativeCacheTTL   = -1            // disable negative caching
		getOrCreateTimeout = 1 * time.Hour // race detector can slow down code *a lot*
	)

	m := NewMetadataMock()
	ln, err := net.Listen("tcp4", "127.0.0.1:")
	require.NoError(t, err)
	s := &rpc.Server{
		Handler: m.Handle,
	}
	defer func() { _ = s.Close() }()
	go func() { _ = s.Serve(ln) }()

	c := &rpc.Client{
		Logf: t.Logf,
	}
	defer func() { _ = c.Close() }()

	dc, err := pcache.OpenDiskCache(filepath.Join(t.TempDir(), cacheFilename), diskTxDuration)
	require.NoError(t, err)
	defer func() { _ = dc.Close() }()

	cache := &pcache.Cache{
		Loader: NewMetricMetaLoader(&tlmetadata.Client{
			Client:  c,
			Network: "tcp4",
			Address: ln.Addr().String(),
		}, getOrCreateTimeout).LoadOrCreateMapping,
		DiskCache:               dc,
		MaxMemCacheSize:         maxMemCacheSize,
		DefaultCacheTTL:         cacheTTL,
		DefaultNegativeCacheTTL: negativeCacheTTL,
		Empty: func() pcache.Value {
			var empty pcache.Int32Value
			return &empty
		},
	}

	clientData := map[string]int32{}
	var clientDataMu sync.Mutex
	var wg errgroup.Group
	for i := 0; i < keyCount*5; i++ {
		i := i
		wg.Go(func() error {
			in := i % keyCount
			key := strconv.Itoa(in)
			out := cache.GetOrLoad(time.Now(), key, metricName)
			require.NoError(t, out.Err)
			clientDataMu.Lock()
			clientData[key] = pcache.ValueToInt32(out.Value)
			clientDataMu.Unlock()
			return err
		})
	}
	require.NoError(t, wg.Wait())
	require.Equal(t, m.Data, clientData)
}

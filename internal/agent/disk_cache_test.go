// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiskCache(t *testing.T) {
	var scratchPad []byte

	const (
		cacheDir = "statshow2_TestDiskCache"
		shardID  = 2
	)

	dir := filepath.Join(t.TempDir(), cacheDir)

	err := os.MkdirAll(dir, os.ModePerm)
	require.NoError(t, err)

	dc, err := MakeDiskBucketStorage(dir, 3, log.Printf)
	require.NoError(t, err)

	sec, ok := dc.ReadNextTailSecond(shardID)
	require.Equal(t, sec, uint32(0))
	require.False(t, ok)

	err = dc.PutBucket(shardID, 15, []byte("data15"))
	require.NoError(t, err)
	b15x, err := dc.GetBucket(shardID, 15, &scratchPad)
	require.NoError(t, err)
	require.Equal(t, string(b15x), "data15")

	err = dc.PutBucket(shardID, 20, []byte("data20"))
	require.NoError(t, err)
	b20x, err := dc.GetBucket(shardID, 20, &scratchPad)
	require.NoError(t, err)
	require.Equal(t, string(b20x), "data20")

	err = dc.Close()
	require.NoError(t, err)

	dc, err = MakeDiskBucketStorage(dir, 3, log.Printf)
	require.NoError(t, err)

	sec, ok = dc.ReadNextTailSecond(shardID)
	require.Equal(t, sec, uint32(15))
	require.True(t, ok)
	sec, ok = dc.ReadNextTailSecond(shardID)
	require.Equal(t, sec, uint32(20))
	require.True(t, ok)
	sec, ok = dc.ReadNextTailSecond(shardID)
	require.Equal(t, sec, uint32(0))
	require.False(t, ok)

	err = dc.EraseBucket(shardID, 20)
	require.NoError(t, err)
	err = dc.EraseBucket(shardID, 20)
	require.NoError(t, err)

	b20y, err := dc.GetBucket(shardID, 20, &scratchPad)
	require.Error(t, err)
	require.True(t, b20y == nil)
	b15y, err := dc.GetBucket(shardID, 15, &scratchPad)
	require.NoError(t, err)
	require.False(t, b15y == nil)

	err = dc.PutBucket(shardID, 24, []byte("data24"))
	require.NoError(t, err)
	b24x, err := dc.GetBucket(shardID, 24, &scratchPad)
	require.NoError(t, err)
	require.Equal(t, string(b24x), "data24")

	err = dc.EraseBucket(shardID, 15)
	require.NoError(t, err)

	err = dc.EraseBucket(shardID, 24)
	require.NoError(t, err)

	err = dc.Close()
	require.NoError(t, err)

	dc, err = MakeDiskBucketStorage(dir, 3, log.Printf)
	require.NoError(t, err)

	sec, ok = dc.ReadNextTailSecond(shardID)
	require.Equal(t, sec, uint32(0))
	require.False(t, ok)

	err = dc.Close()
	require.NoError(t, err)
}

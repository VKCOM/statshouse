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

func printSizes(ds *DiskBucketStorage, shardID int) {
	_, _ = ds.TotalFileSize(shardID) // check invariants
	// fmt.Printf("sizes: %d %d\n", t, u)
}

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
	printSizes(dc, shardID)

	sec, ok := dc.ReadNextTailSecond(shardID)
	require.Equal(t, sec, uint32(0))
	require.False(t, ok)
	printSizes(dc, shardID)

	err = dc.PutBucket(shardID, 15, []byte("data15"))
	require.NoError(t, err)
	printSizes(dc, shardID)

	b15x, err := dc.GetBucket(shardID, 15, &scratchPad)
	require.NoError(t, err)
	require.Equal(t, string(b15x), "data15")
	printSizes(dc, shardID)

	err = dc.PutBucket(shardID, 20, []byte("data20"))
	require.NoError(t, err)
	printSizes(dc, shardID)

	b20x, err := dc.GetBucket(shardID, 20, &scratchPad)
	require.NoError(t, err)
	require.Equal(t, string(b20x), "data20")
	printSizes(dc, shardID)

	err = dc.Close()
	require.NoError(t, err)

	dc, err = MakeDiskBucketStorage(dir, 3, log.Printf)
	require.NoError(t, err)
	printSizes(dc, shardID)

	sec, ok = dc.ReadNextTailSecond(shardID)
	require.Equal(t, sec, uint32(15))
	require.True(t, ok)
	printSizes(dc, shardID)

	sec, ok = dc.ReadNextTailSecond(shardID)
	require.Equal(t, sec, uint32(20))
	require.True(t, ok)
	printSizes(dc, shardID)

	sec, ok = dc.ReadNextTailSecond(shardID)
	require.Equal(t, sec, uint32(0))
	require.False(t, ok)
	printSizes(dc, shardID)

	err = dc.EraseBucket(shardID, 20)
	require.NoError(t, err)
	printSizes(dc, shardID)

	err = dc.EraseBucket(shardID, 20)
	require.NoError(t, err)
	printSizes(dc, shardID)

	b20y, err := dc.GetBucket(shardID, 20, &scratchPad)
	require.Error(t, err)
	require.True(t, b20y == nil)
	printSizes(dc, shardID)

	b15y, err := dc.GetBucket(shardID, 15, &scratchPad)
	require.NoError(t, err)
	require.False(t, b15y == nil)
	printSizes(dc, shardID)

	err = dc.PutBucket(shardID, 24, []byte("data24"))
	require.NoError(t, err)
	printSizes(dc, shardID)

	b24x, err := dc.GetBucket(shardID, 24, &scratchPad)
	require.NoError(t, err)
	require.Equal(t, string(b24x), "data24")
	printSizes(dc, shardID)

	err = dc.EraseBucket(shardID, 15)
	require.NoError(t, err)
	printSizes(dc, shardID)

	err = dc.EraseBucket(shardID, 24)
	require.NoError(t, err)
	printSizes(dc, shardID)

	err = dc.Close()
	require.NoError(t, err)

	dc, err = MakeDiskBucketStorage(dir, 3, log.Printf)
	require.NoError(t, err)
	printSizes(dc, shardID)

	sec, ok = dc.ReadNextTailSecond(shardID)
	require.Equal(t, sec, uint32(0))
	require.False(t, ok)
	printSizes(dc, shardID)

	err = dc.Close()
	require.NoError(t, err)
}

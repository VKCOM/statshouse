// Copyright 2025 V Kontakte LLC
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

func checkErase(t *testing.T, dc *DiskBucketStorage, shardID int, id int64) {
	err := dc.EraseBucket(shardID, id)
	require.NoError(t, err)
	printSizes(dc, shardID)
}

func checkPut(t *testing.T, dc *DiskBucketStorage, shardID int, time uint32, data string) int64 {
	id, err := dc.PutBucket(shardID, time, []byte(data))
	require.NoError(t, err)
	printSizes(dc, shardID)
	return id
}

func checkGet(t *testing.T, dc *DiskBucketStorage, shardID int, id int64, time uint32, shouldBeData string) {
	var scratchPad []byte
	data, err := dc.GetBucket(shardID, id, time, &scratchPad)
	if shouldBeData == "" {
		require.Error(t, err)
		printSizes(dc, shardID)
		return
	}
	require.NoError(t, err)
	require.Equal(t, string(data), shouldBeData)
	printSizes(dc, shardID)
}

func TestDiskCache(t *testing.T) {
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

	sec, id := dc.ReadNextTailBucket(shardID)
	require.Equal(t, sec, uint32(0))
	require.Equal(t, id, int64(0))
	printSizes(dc, shardID)

	id15 := checkPut(t, dc, shardID, 15, "data15")
	checkGet(t, dc, shardID, id15, 15, "data15")

	id20 := checkPut(t, dc, shardID, 20, "data20")
	id20x := checkPut(t, dc, shardID, 20, "data20x")

	checkGet(t, dc, shardID, id20, 20, "data20")
	checkGet(t, dc, shardID, id20x, 20, "data20x")

	err = dc.Close()
	require.NoError(t, err)

	dc, err = MakeDiskBucketStorage(dir, 3, log.Printf)
	require.NoError(t, err)
	printSizes(dc, shardID)

	sec, id15 = dc.ReadNextTailBucket(shardID)
	require.Equal(t, sec, uint32(15))
	printSizes(dc, shardID)

	sec, id20 = dc.ReadNextTailBucket(shardID)
	require.Equal(t, sec, uint32(20))
	printSizes(dc, shardID)

	sec, id20x = dc.ReadNextTailBucket(shardID)
	require.Equal(t, sec, uint32(20))
	printSizes(dc, shardID)

	sec, id = dc.ReadNextTailBucket(shardID)
	require.Equal(t, sec, uint32(0))
	require.Equal(t, id, int64(0))
	printSizes(dc, shardID)

	checkErase(t, dc, shardID, id20)
	checkErase(t, dc, shardID, id20)

	checkGet(t, dc, shardID, id15, 15, "data15")
	checkGet(t, dc, shardID, id20, 20, "")
	checkGet(t, dc, shardID, id20x, 20, "data20x")

	id24 := checkPut(t, dc, shardID, 24, "data24")
	checkGet(t, dc, shardID, id24, 24, "data24")

	checkErase(t, dc, shardID, id15)
	checkErase(t, dc, shardID, id20x)
	checkErase(t, dc, shardID, id24)

	checkGet(t, dc, shardID, id15, 15, "")
	checkGet(t, dc, shardID, id20, 20, "")
	checkGet(t, dc, shardID, id20x, 20, "")
	checkGet(t, dc, shardID, id24, 24, "")

	err = dc.Close()
	require.NoError(t, err)

	dc, err = MakeDiskBucketStorage(dir, 3, log.Printf)
	require.NoError(t, err)
	printSizes(dc, shardID)

	sec, id = dc.ReadNextTailBucket(shardID)
	require.Equal(t, sec, uint32(0))
	require.Equal(t, id, int64(0))
	printSizes(dc, shardID)

	err = dc.Close()
	require.NoError(t, err)
}

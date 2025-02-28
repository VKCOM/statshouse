package api

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestCache2BucketList(t *testing.T) {
	s := [100]cache2Bucket{}
	l := newCache2BucketList()
	require.Equal(t, 0, l.len())
	for i := 0; i < len(s); i++ {
		l.add(&s[i])
		require.Equal(t, i+1, l.len())
	}
	for i := 0; i < len(s); i++ {
		l.remove(&s[i])
		require.Equal(t, len(s)-i-1, l.len())
	}
}

func TestCache2TrimBucketHeapMaxSize(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		h := make(cache2TrimBucketHeap, 0, 10)
		for i := 0; i < 10; i++ {
			h = h.push(cache2TrimBucket{info: cache2BucketRuntimeInfo{
				size: rapid.Int().Draw(t, "sizeInBytes"),
			}})
		}
		sizeInBytes := h.min().info.size
		for h.len() > 1 {
			h = h.pop()
			require.GreaterOrEqual(t, sizeInBytes, h.min().info.size)
			sizeInBytes = h.min().info.size
		}
	})
}

func TestCache2TrimBucketHeapMinAccessTime(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		h := make(cache2TrimBucketHeap, 0, 1000)
		for i := 0; i < 1000; i++ {
			h = h.push(cache2TrimBucket{info: cache2BucketRuntimeInfo{
				lastAccessTime: rapid.Int64().Draw(t, "lastAccessTime"),
			}})
		}
		lastAccessTime := h.min().info.lastAccessTime
		for h.len() > 1 {
			h = h.pop()
			require.LessOrEqual(t, lastAccessTime, h.min().info.lastAccessTime)
			lastAccessTime = h.min().info.lastAccessTime
		}
	})
}

func TestCache2TrimBucketHeapMaxPlay(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		h := make(cache2TrimBucketHeap, 0, 1000)
		for i := 0; i < 1000; i++ {
			h = h.push(cache2TrimBucket{info: cache2BucketRuntimeInfo{
				playInterval: rapid.Int().Draw(t, "play"),
			}})
		}
		play := h.min().info.playInterval
		for h.len() > 1 {
			h = h.pop()
			require.GreaterOrEqual(t, play, h.min().info.playInterval)
			play = h.min().info.playInterval
		}
	})
}

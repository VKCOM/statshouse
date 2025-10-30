package buffer

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"pgregory.net/rand"
)

func DoubleRingBuffer_Race(t *testing.T, b *DoubleRingBuffer[int], loops int) {
	wg := sync.WaitGroup{}
	n := runtime.GOMAXPROCS(0)
	loopsPerGoroutine := loops / n
	totalAdded := atomic.NewInt64(0)
	totalRead := atomic.NewInt64(0)
	totalPopped := atomic.NewInt64(0)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < loopsPerGoroutine; j++ {
				b.PushUncommited(goroutineID*1000 + j)
				totalAdded.Add(1)

				b.ReadAndCommit(func(item int) {
					totalRead.Add(1)
				})

				b.PopF(func(item int) bool {
					totalPopped.Add(1)
					return true
				})
				time.Sleep(time.Duration(rand.Int63n(1000)) * time.Nanosecond)
			}
		}(i)
	}
	wg.Wait()
	require.GreaterOrEqual(t, totalAdded.Load(), int64(loops/2))
	require.GreaterOrEqual(t, totalRead.Load(), int64(loops/2))
}

func TestDoubleRingBuffer_Basic(t *testing.T) {
	b := NewDoubleRingBuffer[int](10)
	b.PushUncommited(1)
	b.PushUncommited(2)
	b.PushUncommited(3)

	var readItems []int
	b.ReadAndCommit(func(item int) {
		readItems = append(readItems, item)
	})
	require.Equal(t, []int{1, 2, 3}, readItems)

	var poppedItems []int
	err := b.PopF(func(item int) bool {
		poppedItems = append(poppedItems, item)
		return true
	})
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, poppedItems)
}

func TestDoubleRingBuffer_UncommitedItems(t *testing.T) {
	b := NewDoubleRingBuffer[int](10)

	b.PushUncommited(1)
	b.PushUncommited(2)

	err := b.PopF(func(item int) bool {
		return true
	})
	require.ErrorIs(t, err, UncommitedItemsFound)
}

func TestDoubleRingBuffer_BufferSwitch(t *testing.T) {
	b := NewDoubleRingBuffer[int](5)

	for i := 0; i < 5; i++ {
		b.PushUncommited(i)
	}
	b.ReadAndCommit(func(item int) {})
	for i := 5; i < 8; i++ {
		b.PushUncommited(i)
	}
	var readItems []int
	b.ReadAndCommit(func(item int) {
		readItems = append(readItems, item)
	})
	require.Equal(t, []int{5, 6, 7}, readItems)

	var poppedItems []int
	err := b.PopF(func(item int) bool {
		poppedItems = append(poppedItems, item)
		return true
	})
	require.NoError(t, err)
	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, poppedItems)
}

func TestDoubleRingBuffer_Reset(t *testing.T) {
	b := NewDoubleRingBuffer[int](10)

	b.PushUncommited(1)
	b.PushUncommited(2)
	b.Reset()

	b.PushUncommited(3)
	b.PushUncommited(4)

	var readItems []int
	b.ReadAndCommit(func(item int) {
		readItems = append(readItems, item)
	})
	require.Equal(t, []int{3, 4}, readItems)
}

func TestDoubleRingBuffer_SelectivePop(t *testing.T) {
	b := NewDoubleRingBuffer[int](10)

	for i := 0; i <= 5; i++ {
		b.PushUncommited(i)
	}
	b.ReadAndCommit(func(item int) {})

	var poppedItems []int
	err := b.PopF(func(item int) bool {
		if item < 4 {
			poppedItems = append(poppedItems, item)
			return true
		}
		return false
	})
	require.NoError(t, err)
	require.Equal(t, []int{0, 1, 2, 3}, poppedItems)

	var remainingItems []int
	err = b.PopF(func(item int) bool {
		remainingItems = append(remainingItems, item)
		return true
	})
	require.NoError(t, err)
	require.Equal(t, []int{4, 5}, remainingItems)
}

func TestDoubleRingBuffer_Performance(t *testing.T) {
	b := NewDoubleRingBuffer[int](1000)
	loops := 100000000

	start := time.Now()
	for i := 0; i < loops; i++ {
		b.PushUncommited(i)
		if i%100 == 0 {
			b.ReadAndCommit(func(item int) {})
			b.PopF(func(item int) bool {
				return true
			})
		}
	}
	duration := time.Since(start)
	t.Logf("Processed %d items in %v (%.2f items/sec)",
		loops, duration, float64(loops)/duration.Seconds())
}

func TestDoubleRingBuffer_RaceConditions(t *testing.T) {
	t.Run("BasicRace", func(t *testing.T) {
		b := NewDoubleRingBuffer[int](100)
		DoubleRingBuffer_Race(t, b, 10000)
	})
	t.Run("SmallBufferRace", func(t *testing.T) {
		b := NewDoubleRingBuffer[int](10)
		DoubleRingBuffer_Race(t, b, 5000)
	})
	t.Run("LargeBufferRace", func(t *testing.T) {
		b := NewDoubleRingBuffer[int](1000)
		DoubleRingBuffer_Race(t, b, 20000)
	})
}

func TestDoubleRingBuffer_DifferentTypes(t *testing.T) {
	t.Run("StringBuffer", func(t *testing.T) {
		b := NewDoubleRingBuffer[string](10)

		b.PushUncommited("hello")
		b.PushUncommited("world")

		var items []string
		b.ReadAndCommit(func(item string) {
			items = append(items, item)
		})

		require.Equal(t, []string{"hello", "world"}, items)
	})
	t.Run("StructBuffer", func(t *testing.T) {
		type TestStruct struct {
			ID   int
			Name string
		}

		b := NewDoubleRingBuffer[TestStruct](10)

		b.PushUncommited(TestStruct{ID: 1, Name: "test1"})
		b.PushUncommited(TestStruct{ID: 2, Name: "test2"})

		var items []TestStruct
		b.ReadAndCommit(func(item TestStruct) {
			items = append(items, item)
		})

		require.Len(t, items, 2)
		require.Equal(t, 1, items[0].ID)
		require.Equal(t, "test1", items[0].Name)
	})
}

func TestDoubleRingBuffer_Cases(t *testing.T) {
	t.Run("EmptyBuffer", func(t *testing.T) {
		b := NewDoubleRingBuffer[int](10)

		var items []int
		b.ReadAndCommit(func(item int) {
			items = append(items, item)
		})

		require.Empty(t, items)

		err := b.PopF(func(item int) bool {
			return true
		})
		require.NoError(t, err)
	})

	t.Run("SingleElement", func(t *testing.T) {
		b := NewDoubleRingBuffer[int](10)

		b.PushUncommited(42)

		var items []int
		b.ReadAndCommit(func(item int) {
			items = append(items, item)
		})

		require.Equal(t, []int{42}, items)

		var poppedItems []int
		err := b.PopF(func(item int) bool {
			poppedItems = append(poppedItems, item)
			return true
		})

		require.NoError(t, err)
		require.Equal(t, []int{42}, poppedItems)
	})

	t.Run("ZeroSizeBuffer", func(t *testing.T) {
		b := NewDoubleRingBuffer[int](0)

		b.PushUncommited(1)
		b.PushUncommited(2)

		var items []int
		b.ReadAndCommit(func(item int) {
			items = append(items, item)
		})

		require.Equal(t, []int{1, 2}, items)
	})
}

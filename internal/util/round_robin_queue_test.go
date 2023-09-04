package util

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func Test_Round_Robin_Queue_Race(t *testing.T) {
	t.Parallel()
	q := NewQ(2, 1)
	Queue_Race(t, q, 8)

}

func Test_Round_Robin_Queue_Random_Timeout_Race(t *testing.T) {
	t.Parallel()
	q := NewQ(2, 1)
	Queue_Random_Timeout_Race(t, q, 8)

}

func Test_Round_Robin_Queue_Race1(t *testing.T) {
	t.Parallel()
	q := NewQ(4, 2)
	Queue_Race(t, q, 8)

}

func Test_Round_Robin_Queue_Random_Timeout_Race1(t *testing.T) {
	t.Parallel()
	q := NewQ(4, 2)
	Queue_Random_Timeout_Race(t, q, 8)

}

func Test_Round_Robin_Timeout(t *testing.T) {
	t.Parallel()
	q := NewQ(1, 1)
	Timeout(t, q)

}

func Test_Round_Robin_Timeout1(t *testing.T) {
	t.Parallel()
	q := NewQ(1, 1)
	Timeout1(t, q)
}

func TestWait(t *testing.T) {
	t.Parallel()
	q := NewQ(1, 1)
	qry, err := q.Acquire(context.Background(), "0")
	require.NoError(t, err)
	errGroup, _ := errgroup.WithContext(context.Background())
	var qry1 Query
	errGroup.Go(func() error {
		var err error
		qry1, err = q.Acquire(context.Background(), "0")
		return err

	})
	time.Sleep(time.Millisecond * 5)
	q.Release(qry)
	require.NoError(t, errGroup.Wait())
	q.Release(qry1)
}

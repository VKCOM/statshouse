package util

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_severConnPool_Do(t *testing.T) {
	p := &severConnPool{
		ch:         nil,
		errCount:   0,
		state:      alive,
		stateStart: time.Now(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < maxFailErrAlive; i++ {
		p.passResult(ctx, &net.OpError{})
	}
	require.Equal(t, p.errCount, 0)
	require.Equal(t, halfOpen, p.state)
	for i := 0; i < maxFailErrHalfOpen; i++ {
		p.passResult(context.Background(), fmt.Errorf("FAIL"))
	}
	require.Equal(t, p.errCount, 0)
	require.Equal(t, dead, p.state)
}

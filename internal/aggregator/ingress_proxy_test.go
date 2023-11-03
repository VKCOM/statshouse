package aggregator

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_clientPool_getClient(t *testing.T) {
	maxConn := 2
	pool := &clientPool{
		clients: map[string]*cachedClient{},
		config: ConfigIngressProxy{
			MaxConnection:    maxConn,
			DeleteSampleSize: 1,
		},
	}
	var clients []*cachedClient
	n := 10
	for i := 0; i < n; i++ {
		c, _ := pool.getClient(strconv.FormatInt(int64(i), 10))
		clients = append(clients, c)
	}
	require.Len(t, pool.clients, n)
	for i, c := range clients {
		pool.releaseClient(c)
		if n-i-1 > maxConn {
			require.Len(t, pool.clients, n-i-1)
		} else {
			require.Len(t, pool.clients, maxConn)
		}
	}
}

package aggregator

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func Test_clientPool_getClient(t *testing.T) {
	n := 10
	pool := newClientPool("", []string{""}, 1, ConfigIngressProxy{
		MaxClientsPerShardReplica: n,
	})
	var clients []*cachedClient
	for i := 0; i < n; i++ {
		k, c, err := pool.getClient(0)
		require.NoError(t, err)
		require.Equal(t, i, k)
		clients = append(clients, c)
		require.Equal(t, 1, len(pool.fullClients))
		require.Equal(t, 1, len(pool.freeClients))
		require.Equal(t, len(clients), len(pool.fullClients[0]))
		require.Equal(t, 0, len(pool.freeClients[0]))
	}
	_, _, err := pool.getClient(0)
	require.Error(t, err)
	for i, c := range clients {
		pool.releaseClient(0, i, c)
		require.Equal(t, 1, len(pool.fullClients))
		require.Equal(t, 1, len(pool.freeClients))
		require.Equal(t, len(clients)-i-1, len(pool.fullClients[0]))
		require.Equal(t, i+1, len(pool.freeClients[0]))
	}
}

func Test_clientPool_getClient_reuse(t *testing.T) {
	pool := newClientPool("", []string{""}, 2, ConfigIngressProxy{
		MaxClientsPerShardReplica: 2,
	})
	var keys []int
	var clients []*cachedClient
	{
		k, c, err := pool.getClient(0)
		require.NoError(t, err)
		require.Equal(t, 0, k)
		keys = append(keys, k)
		clients = append(clients, c)
		require.Equal(t, 1, len(pool.fullClients))
		require.Equal(t, 1, len(pool.freeClients))
		require.Equal(t, 0, len(pool.fullClients[0]))
		require.Equal(t, 1, len(pool.freeClients[0]))
	}
	{
		k, c, err := pool.getClient(0)
		require.NoError(t, err)
		require.Equal(t, 0, k)
		keys = append(keys, k)
		clients = append(clients, c)
		require.Equal(t, 1, len(pool.fullClients))
		require.Equal(t, 1, len(pool.freeClients))
		require.Equal(t, 1, len(pool.fullClients[0]))
		require.Equal(t, 0, len(pool.freeClients[0]))
	}
	{
		k, c, err := pool.getClient(0)
		require.NoError(t, err)
		require.Equal(t, 1, k)
		keys = append(keys, k)
		clients = append(clients, c)
		require.Equal(t, 1, len(pool.fullClients))
		require.Equal(t, 1, len(pool.freeClients))
		require.Equal(t, 1, len(pool.fullClients[0]))
		require.Equal(t, 1, len(pool.freeClients[0]))
	}
	{
		k, c, err := pool.getClient(0)
		require.NoError(t, err)
		require.Equal(t, 1, k)
		keys = append(keys, k)
		clients = append(clients, c)
		require.Equal(t, 1, len(pool.fullClients))
		require.Equal(t, 1, len(pool.freeClients))
		require.Equal(t, 2, len(pool.fullClients[0]))
		require.Equal(t, 0, len(pool.freeClients[0]))
	}
	_, _, err := pool.getClient(0)
	require.Error(t, err)
	{
		pool.releaseClient(0, keys[0], clients[0])
		require.Equal(t, 1, len(pool.fullClients))
		require.Equal(t, 1, len(pool.freeClients))
		require.Equal(t, 1, len(pool.fullClients[0]))
		require.Equal(t, 1, len(pool.freeClients[0]))
	}
	{
		pool.releaseClient(0, keys[1], clients[1])
		require.Equal(t, 1, len(pool.fullClients))
		require.Equal(t, 1, len(pool.freeClients))
		require.Equal(t, 1, len(pool.fullClients[0]))
		require.Equal(t, 1, len(pool.freeClients[0]))
	}
	{
		pool.releaseClient(0, keys[2], clients[2])
		require.Equal(t, 1, len(pool.fullClients))
		require.Equal(t, 1, len(pool.freeClients))
		require.Equal(t, 0, len(pool.fullClients[0]))
		require.Equal(t, 2, len(pool.freeClients[0]))
	}
	{
		pool.releaseClient(0, keys[3], clients[3])
		require.Equal(t, 1, len(pool.fullClients))
		require.Equal(t, 1, len(pool.freeClients))
		require.Equal(t, 0, len(pool.fullClients[0]))
		require.Equal(t, 2, len(pool.freeClients[0]))
	}
}

type clientPoolState struct {
	shardReplicaMax int
	pool            *clientPool

	shardReplicaInflight map[int]int
	shardReplicaClients  map[int]map[int]*cachedClient
}

func (s *clientPoolState) GetClient(t *rapid.T) {
	shardReplica := rapid.IntRange(0, s.shardReplicaMax-1).Draw(t, "shard_replica")
	k, client, err := s.pool.getClient(uint32(shardReplica))
	t.Log("client id:", k)
	if inFlight := s.shardReplicaInflight[shardReplica]; inFlight == (s.pool.maxPacketInflightPerShardReplica * s.pool.config.MaxClientsPerShardReplica) {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
		s.shardReplicaInflight[shardReplica]++
		s.shardReplicaClients[shardReplica][k] = client
	}
}

func (s *clientPoolState) ReleaseClient(t *rapid.T) {
	shardReplica := rapid.IntRange(0, s.shardReplicaMax-1).Draw(t, "shard_replica")
	if inFlight := s.shardReplicaInflight[shardReplica]; inFlight == 0 {
		t.SkipNow()
	}
	clientIx := rapid.IntMax(s.pool.config.MaxClientsPerShardReplica).Draw(t, "client")
	if c, ok := s.shardReplicaClients[shardReplica][clientIx]; !ok || c.packetInflight == 0 {
		t.SkipNow()
	} else {
		s.pool.releaseClient(uint32(shardReplica), clientIx, c)
		s.shardReplicaInflight[shardReplica]--
	}
}

func (s *clientPoolState) Check(t *rapid.T) {
	for sr, inflight := range s.shardReplicaInflight {
		inFlightActual := 0
		for _, c := range s.pool.freeClients[sr] {
			inFlightActual += c.packetInflight
		}
		for _, c := range s.pool.fullClients[sr] {
			inFlightActual += c.packetInflight
		}
		require.Equal(t, inflight, inFlightActual)
	}
	for sr, clients := range s.shardReplicaClients {
		for k, c := range clients {
			if c.packetInflight == s.pool.maxPacketInflightPerShardReplica {
				require.Contains(t, s.pool.fullClients[sr], k)
				require.NotContains(t, s.pool.freeClients[sr], k)
			} else {
				require.NotContains(t, s.pool.fullClients[sr], k)
				require.Contains(t, s.pool.freeClients[sr], k)
			}
		}
	}
}

func (s *clientPoolState) init() {
	maxClientsPerShardReplica := 2
	maxPacketInflightPerShardReplica := 4
	shardReplicaMax := 4
	pool := newClientPool("", make([]string, shardReplicaMax), maxPacketInflightPerShardReplica, ConfigIngressProxy{
		MaxClientsPerShardReplica: maxClientsPerShardReplica,
	})
	s.pool = pool
	s.shardReplicaMax = shardReplicaMax
	s.shardReplicaClients = map[int]map[int]*cachedClient{}
	s.shardReplicaInflight = map[int]int{}
	for i := 0; i < shardReplicaMax; i++ {
		s.shardReplicaClients[i] = map[int]*cachedClient{}
	}
}

func Test_clientPool_getClient_stress(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := clientPoolState{}
		s.init()
		t.Repeat(rapid.StateMachineActions(&s))
	})
}

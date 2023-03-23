// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/dchest/siphash"
	"github.com/dgryski/go-maglev"
	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/vkgo/mem"
)

// TODO: weighted balancing

const (
	// random SipHash keys
	k0 = 0x127ec5f5ca3a335a
	k1 = 0x607e42e927413d01
)

var (
	errNoBackends = errors.New("rpc: no backends in cluster")
)

type TargetError struct {
	Address NetAddr
	Err     error
}

func (err TargetError) Error() string {
	return fmt.Sprintf("%v (from %v)", err.Err, err.Address)
}

func (err TargetError) Unwrap() error {
	return err.Err
}

type ClusterClient struct {
	client               *Client
	bigCluster           bool // set if expected cluster size is greater than 500 shards
	shardSelectKeyModulo bool // override default Maglev sharding with simple modulo
	onDo                 func(addr NetAddr, req *Request)

	mu     sync.RWMutex
	shards []ClusterShard
	table  *maglev.Table
}

type ClusterClientOptions struct {
	BigCluster           bool
	ShardSelectKeyModulo bool
	OnDo                 func(addr NetAddr, req *Request)
}

type ClusterClientOptionsFunc func(*ClusterClientOptions)

func ClusterClientWithBigCluster(v bool) ClusterClientOptionsFunc {
	return func(o *ClusterClientOptions) {
		o.BigCluster = v
	}
}

func ClusterClientWithShardSelectKeyModulo(v bool) ClusterClientOptionsFunc {
	return func(o *ClusterClientOptions) {
		o.ShardSelectKeyModulo = v
	}
}

func ClusterClientWithOnDo(v func(addr NetAddr, req *Request)) ClusterClientOptionsFunc {
	return func(o *ClusterClientOptions) {
		o.OnDo = v
	}
}

func NewClusterClient(client *Client, opts ...ClusterClientOptionsFunc) *ClusterClient {
	options := &ClusterClientOptions{OnDo: func(addr NetAddr, req *Request) {}}
	for _, opt := range opts {
		opt(options)
	}

	return &ClusterClient{
		client:               client,
		bigCluster:           options.BigCluster,
		shardSelectKeyModulo: options.ShardSelectKeyModulo,
		onDo:                 options.OnDo,
	}
}

func (cc *ClusterClient) RPCClient() *Client {
	return cc.client
}

type ClusterShard struct {
	Name       string
	ReadNodes  []NetAddr
	WriteNodes []NetAddr
}

func (cc *ClusterClient) UpdateCluster(shards []ClusterShard) error {
	m := maglev.SmallM
	if cc.bigCluster {
		m = maglev.BigM
	}
	if len(shards) > m/100 {
		return fmt.Errorf("cluster size %v is too big for Maglev table of size %v", len(shards), m)
	}

	var shardsCopy []ClusterShard
	seen := map[string]bool{}
	for _, s := range shards {
		if s.Name == "" {
			return fmt.Errorf("empty shard name")
		}
		if seen[s.Name] {
			return fmt.Errorf("duplicate shard name %q", s.Name)
		}
		seen[s.Name] = true
		if len(s.ReadNodes)+len(s.WriteNodes) == 0 {
			return fmt.Errorf("no nodes in shard %q", s.Name)
		}
		for _, addr := range s.ReadNodes {
			if addr.Network != "tcp4" && addr.Network != "unix" {
				return fmt.Errorf("unsupported network type %q in read node %v", addr.Network, addr)
			}
		}
		for _, addr := range s.WriteNodes {
			if addr.Network != "tcp4" && addr.Network != "unix" {
				return fmt.Errorf("unsupported network type %q in write node %v", addr.Network, addr)
			}
		}
		shardsCopy = append(shardsCopy, ClusterShard{
			Name:       s.Name,
			ReadNodes:  append([]NetAddr(nil), s.ReadNodes...),
			WriteNodes: append([]NetAddr(nil), s.WriteNodes...),
		})
	}

	var table *maglev.Table
	if !cc.shardSelectKeyModulo {
		shardNames := make([]string, 0, len(shardsCopy))
		for _, s := range shardsCopy {
			shardNames = append(shardNames, s.Name)
		}
		if len(shardNames) > 0 {
			table = maglev.New(shardNames, uint64(m))
		}
	}

	cc.mu.Lock()
	defer cc.mu.Unlock()

	cc.shards = shardsCopy
	cc.table = table

	return nil
}

func (cc *ClusterClient) DoKey(ctx context.Context, write bool, req *Request, key uint64) (*Response, error) {
	netAddr := cc.SelectKey(write, key)
	if netAddr.Address == "" {
		cc.client.putRequest(req)
		return nil, errNoBackends
	}

	cc.onDo(netAddr, req)
	resp, err := cc.client.Do(ctx, netAddr.Network, netAddr.Address, req)
	if err != nil {
		return nil, TargetError{Address: netAddr, Err: err}
	}

	return resp, nil
}

func (cc *ClusterClient) DoAny(ctx context.Context, write bool, req *Request) (*Response, error) {
	netAddr := cc.SelectAny(write)
	if netAddr.Address == "" {
		cc.client.putRequest(req)
		return nil, errNoBackends
	}

	cc.onDo(netAddr, req)
	resp, err := cc.client.Do(ctx, netAddr.Network, netAddr.Address, req)
	if err != nil {
		return nil, TargetError{Address: netAddr, Err: err}
	}

	return resp, nil
}

func (cc *ClusterClient) DoAll(
	ctx context.Context,
	write bool,
	prepareRequest func(addr NetAddr, req *Request) error,
	processResponse func(addr NetAddr, resp *Response, err error) error,
) error {
	netAddrs := cc.SelectAll(write)
	if len(netAddrs) == 0 {
		return errNoBackends
	}

	return cc.client.DoMulti(ctx, netAddrs, prepareRequest, processResponse)
}

func (cc *ClusterClient) pick2(n int) (int, int) {
	i := rand.Intn(n)
	j := i
	for j == i {
		j = rand.Intn(n)
	}

	return i, j
}

func (cc *ClusterClient) selectInShard(write bool, shard ClusterShard) NetAddr {
	nodes := shard.ReadNodes
	if write {
		nodes = shard.WriteNodes
	}

	switch n := len(nodes); n {
	case 0:
		return NetAddr{}
	case 1:
		return nodes[0]
	default:
		i, j := cc.pick2(n)
		li := cc.client.getLoad(nodes[i])
		lj := cc.client.getLoad(nodes[j])
		if li <= lj {
			return nodes[i]
		}
		return nodes[j]
	}
}

// SelectKey returns empty address when no backend is found
func (cc *ClusterClient) SelectKey(write bool, key uint64) NetAddr {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	if len(cc.shards) == 0 {
		return NetAddr{}
	}

	var i int
	if cc.shardSelectKeyModulo {
		i = int(key % uint64(len(cc.shards)))
	} else {
		h := hashInt(key) // guarantee perfect key distribution
		i = cc.table.Lookup(h)
	}

	return cc.selectInShard(write, cc.shards[i])
}

// SelectAny returns empty address when no backend is found
func (cc *ClusterClient) SelectAny(write bool) NetAddr {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	switch n := len(cc.shards); n {
	case 0:
		return NetAddr{}
	case 1:
		return cc.selectInShard(write, cc.shards[0])
	default:
		i, j := cc.pick2(n)
		ai := cc.selectInShard(write, cc.shards[i])
		aj := cc.selectInShard(write, cc.shards[j])
		switch {
		case ai.Address == "" && aj.Address == "":
			return NetAddr{}
		case ai.Address == "":
			return aj
		case aj.Address == "":
			return ai
		default:
			li := cc.client.getLoad(ai)
			lj := cc.client.getLoad(aj)
			if li <= lj {
				return ai
			}
			return aj
		}
	}
}

func (cc *ClusterClient) SelectAll(write bool) []NetAddr {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	addrs := make([]NetAddr, 0, len(cc.shards))
	for _, s := range cc.shards {
		addr := cc.selectInShard(write, s)
		if addr.Address != "" {
			addrs = append(addrs, addr)
		}
	}

	return addrs
}

func HashSlice(key []byte) uint64 {
	return siphash.Hash(k0, k1, key)
}

func HashString(key string) uint64 {
	return mem.SipHash24(k0, k1, key)
}

func hashInt(key uint64) uint64 {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], key)
	return HashSlice(b[:])
}

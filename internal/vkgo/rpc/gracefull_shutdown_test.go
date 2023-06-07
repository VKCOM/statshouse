// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func TestRPCGraceful(t *testing.T) {
	t.Parallel()

	// this is not really a property-based test, since it is not deterministic
	// however, biased integer generators from rapid are very convenient
	rapid.Check(t, testRPCGraceful)
}

func testRPCGraceful(t *rapid.T) {
	if debugPrint {
		fmt.Printf("---- testRPCGraceful\n")
	}
	var lc net.ListenConfig
	lc.Control = controlSetTCPReuseAddrPort

	ln, err := lc.Listen(context.Background(), "tcp4", "127.0.0.1:41577")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // otherwise, we must listen to it or lose SYNs
	var servers []*Server
	serverNum := 0
	for i := 0; i != 10; i++ {
		servers = append(servers, genServer(t))
	}
	clients := []*Client{genClient(t)} // rapid.SliceOf(rapid.Custom(genClient)).Draw(t, "clients")
	numRequests := 10                  // rapid.IntRange(1, 10).Draw(t, "numRequests")

	var smu sync.Mutex
	var server *Server

	var wg sync.WaitGroup
	var sg sync.WaitGroup
	replaceServer := func() {
		sg.Add(1)
		smu.Lock()
		if serverNum >= len(servers) {
			smu.Unlock()
			return
		}
		s := servers[serverNum]
		serverNum++
		if server != nil {
			if debugPrint {
				fmt.Printf("%v shutdown of %p\n", time.Now(), server)
			}
			server.Shutdown()
		}
		server = s
		smu.Unlock()

		ln2, err := lc.Listen(context.Background(), "tcp4", addr)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			defer sg.Done()
			if debugPrint {
				fmt.Printf("%v serve of %p\n", time.Now(), s)
			}
			if err := s.Serve(ln2); err != nil && err != ErrServerClosed {
				t.Fatal(err)
			}
			if debugPrint {
				fmt.Printf("%v quit of %p\n", time.Now(), s)
			}
		}()
	}

	replaceServer()

	var reqCounter atomic.Int64

	for _, c := range clients {
		wg.Add(1)
		go func(c *Client) {
			defer wg.Done()

			for j := 0; j < numRequests*2; j++ {
				reqID := reqCounter.Add(1)
				if reqID%int64(numRequests) == 0 {
					replaceServer()
				}
				dorequest(t, c, addr)
			}
			if debugPrint {
				fmt.Printf("%v before client close\n", time.Now())
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			if debugPrint {
				fmt.Printf("%v after client close\n", time.Now())
			}
		}(c)
	}
	wg.Wait() // All clients finished
	if debugPrint {
		fmt.Printf("%v all clients finished\n", time.Now())
	}

	smu.Lock()
	if server != nil {
		if debugPrint {
			fmt.Printf("%v shutdown (last) of %p\n", time.Now(), server)
		}
		server.Shutdown()
	}
	smu.Unlock()
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()
	// _ = s.CloseWait(ctx)

	sg.Wait()
	if debugPrint {
		fmt.Printf("%v all servers finished\n", time.Now())
	}

	for _, s := range servers {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

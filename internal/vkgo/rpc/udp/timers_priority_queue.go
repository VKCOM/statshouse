// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import "container/heap"

type __connectionsArray struct {
	connections []*Connection
}

func (a *__connectionsArray) Len() int { return len(a.connections) }

func (a *__connectionsArray) Less(i, j int) bool {
	return a.connections[i].resendTimeMs < a.connections[j].resendTimeMs
}

func (a *__connectionsArray) Swap(i, j int) {
	a.connections[i], a.connections[j] = a.connections[j], a.connections[i]
}

func (a *__connectionsArray) Push(x any) {
	if conn, ok := x.(*Connection); ok {
		a.connections = append(a.connections, conn)
	} else {
		panic("TimersPriorityQueue::Push(..) accepts only *Connection")
	}
}

func (a *__connectionsArray) Pop() any {
	n := len(a.connections)
	x := a.connections[n-1]
	a.connections[n-1] = nil
	a.connections = a.connections[0 : n-1]
	return x
}

type TimersPriorityQueue struct {
	array __connectionsArray
}

func (q *TimersPriorityQueue) Len() int {
	return q.array.Len()
}

func (q *TimersPriorityQueue) Empty() bool {
	return q.array.Len() == 0
}

func (q *TimersPriorityQueue) Min() *Connection {
	return q.array.connections[0]
}

func (q *TimersPriorityQueue) ExtractMin() *Connection {
	conn, ok := heap.Pop(&q.array).(*Connection)
	if !ok {
		panic("Pop(&t.resendTimers) must return *Connection")
	}
	return conn
}

func (q *TimersPriorityQueue) Add(conn *Connection) {
	heap.Push(&q.array, conn)
}

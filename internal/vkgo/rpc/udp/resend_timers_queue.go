// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

type ResendTimersQueue struct {
	connections []*Connection
}

func (q *ResendTimersQueue) Len() int { return len(q.connections) }

func (q *ResendTimersQueue) Less(i, j int) bool {
	return q.connections[i].resendTimeMs < q.connections[j].resendTimeMs
}

func (q *ResendTimersQueue) Swap(i, j int) {
	q.connections[i], q.connections[j] = q.connections[j], q.connections[i]
}

func (q *ResendTimersQueue) Push(x any) {
	if conn, ok := x.(*Connection); ok {
		q.connections = append(q.connections, conn)
	} else {
		panic("ResendTimersQueue::Push(..) accepts only *Connection")
	}
}

func (q *ResendTimersQueue) Pop() any {
	n := len(q.connections)
	x := q.connections[n-1]
	q.connections[n-1] = nil
	q.connections = q.connections[0 : n-1]
	return x
}

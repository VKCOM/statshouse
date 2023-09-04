package util

import (
	"container/list"
	"context"
	"sync"
)

type Qry1 struct {
	token string
	ch    chan struct{}
}

type user struct {
	qry   *list.List
	order int64
}

type Q1 struct {
	mx             sync.Mutex
	activeQuery    int
	maxActiveQuery int
	waitingUsers   map[string]*user
	globalOrder    int64
}

func NewQ1(n int) *Q1 {
	return &Q1{
		maxActiveQuery: n,
		waitingUsers:   map[string]*user{},
	}
}

func (q *Q1) incOrder() int64 {
	order := q.globalOrder
	q.globalOrder++
	return order

}

func (q *Q1) addUserQueryLocked(token string) (*list.Element, *Qry1) {
	qry := &Qry1{
		token: token,
		ch:    make(chan struct{}),
	}
	var e *list.Element
	if u, ok := q.waitingUsers[token]; ok {
		e = u.qry.PushBack(qry)
	} else {
		qrys := list.New()
		e = qrys.PushBack(qry)
		u = &user{
			qry:   qrys,
			order: q.incOrder(),
		}
		q.waitingUsers[token] = u
	}
	return e, qry
}

func (q *Q1) removeQueryLocked(token string, e *list.Element) {
	u, ok := q.waitingUsers[token]
	if !ok {
		return
	}
	u.qry.Remove(e)
	if u.qry.Len() == 0 {
		delete(q.waitingUsers, token)
	}
}

func (q *Q1) Acquire(ctx context.Context, token string) (Query, error) {
	q.mx.Lock()
	e, qry := q.addUserQueryLocked(token)
	q.nextQueryLocked()
	q.mx.Unlock()
	select {
	case <-ctx.Done():
		err := ctx.Err()
		q.mx.Lock()
		defer q.mx.Unlock()
		q.removeQueryLocked(token, e)
		return nil, err
	case <-qry.ch:
	}
	return qry, nil

}

func (q *Q1) nextQueryLocked() {
	if q.activeQuery == q.maxActiveQuery {
		return
	}
	var nextUser *user
	var token string
	for t, u := range q.waitingUsers {
		if nextUser == nil || u.order < nextUser.order {
			token = t
			nextUser = u
		}
	}
	if nextUser != nil {
		if nextUser.qry.Len() == 1 {
			delete(q.waitingUsers, token)
		}
		nextUser.order = q.incOrder()
		v := nextUser.qry.Remove(nextUser.qry.Front())
		ch := v.(*Qry1).ch
		q.activeQuery++
		close(ch)
	}
}

func (q *Q1) Release(_ Query) {
	q.mx.Lock()
	defer q.mx.Unlock()
	q.activeQuery--
	q.nextQueryLocked()
}

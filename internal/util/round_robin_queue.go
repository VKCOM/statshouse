package util

import (
	"container/list"
	"context"
	"sync"
)

type Qry struct {
	token string
	ch    chan struct{}
}

type Q struct {
	mx                sync.Mutex
	activeQuery       int
	maxActiveQuery    int
	maxPerUser        int
	waiters           *list.List
	waitOrActiveUsers map[string]int
	waitOrActiveQuery map[*Qry]*list.Element

	usersRequestQueue map[string]*list.List
}

func NewQ(maxActiveQuery, maxPerUser int) *Q {
	if maxActiveQuery <= 0 {
		maxActiveQuery = 1
	}
	if maxPerUser <= 0 {
		maxPerUser = 1
	}
	return &Q{
		maxActiveQuery:    maxActiveQuery,
		maxPerUser:        maxPerUser,
		waiters:           list.New(),
		waitOrActiveUsers: map[string]int{},
		waitOrActiveQuery: map[*Qry]*list.Element{},
		usersRequestQueue: map[string]*list.List{},
	}
}

func (q *Q) addQueryLocked(qry *Qry) *list.Element {
	count, ok := q.waitOrActiveUsers[qry.token]
	if ok && count == q.maxPerUser {
		l, ok := q.usersRequestQueue[qry.token]
		if !ok {
			l = list.New()
			q.usersRequestQueue[qry.token] = l
		}
		return l.PushBack(qry)
	}
	e := q.waiters.PushBack(qry)
	q.waitOrActiveUsers[qry.token]++
	q.waitOrActiveQuery[qry] = e
	return nil
}

func (q *Q) Acquire(ctx context.Context, token string) (Query, error) {
	q.mx.Lock()
	qry := &Qry{
		token: token,
	}
	qry.ch = make(chan struct{})
	element := q.addQueryLocked(qry)
	q.pushQueryLocked()
	q.mx.Unlock()
	select {
	case <-qry.ch:
	case <-ctx.Done():
		err := ctx.Err()
		q.mx.Lock()
		defer q.mx.Unlock()
		if e, ok := q.waitOrActiveQuery[qry]; ok {
			delete(q.waitOrActiveQuery, qry)
			delete(q.waitOrActiveUsers, qry.token)
			v := q.waiters.Remove(e)
			ch := v.(*Qry).ch
			select {
			case <-ch:
				q.activeQuery--
			default:
				close(ch)
			}
			q.updateUserQueueLocked(qry.token)
		} else if userQueue, ok := q.usersRequestQueue[qry.token]; ok && element != nil {
			userQueue.Remove(element)
			close(qry.ch)
		}
		return qry, err
	}

	return qry, nil
}

func (q *Q) pushQueryLocked() {
	if q.activeQuery < q.maxActiveQuery && q.waiters.Len() > 0 {
		f := q.waiters.Front()
		value := q.waiters.Remove(f)
		q.activeQuery++
		qry := value.(*Qry)
		close(qry.ch)
	}
}

func (q *Q) Release(qryI Query) {
	q.mx.Lock()
	defer q.mx.Unlock()
	qry := qryI.(*Qry)
	q.activeQuery--
	delete(q.waitOrActiveQuery, qry)
	q.waitOrActiveUsers[qry.token]--
	q.updateUserQueueLocked(qry.token)
	if count, _ := q.waitOrActiveUsers[qry.token]; count == 0 {
		delete(q.waitOrActiveUsers, qry.token)
	}
	q.pushQueryLocked()
}

func (q *Q) updateUserQueueLocked(token string) {
	qrs, ok := q.usersRequestQueue[token]
	if ok {
		if qrs.Len() == 0 {
			delete(q.usersRequestQueue, token)
		} else {
			f := qrs.Remove(qrs.Front())
			qry := f.(*Qry)
			e := q.waiters.PushBack(qry)
			q.waitOrActiveUsers[qry.token]++
			q.waitOrActiveQuery[qry] = e
		}
	}
}

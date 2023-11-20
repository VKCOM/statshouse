package queue

import (
	"container/list"
	"context"
	"sync"

	"github.com/petar/GoLLRB/llrb"
)

type query struct {
	token string
	ch    chan struct{}
}

type user struct {
	token string
	qry   *list.List
	order int64
}

func (u *user) Less(than llrb.Item) bool {
	r := than.(*user)
	return u.order < r.order
}

type Queue struct {
	mx                     sync.Mutex
	activeQuery            int64
	MaxActiveQuery         int64
	waitingUsersByName     map[string]*user
	waitingUsersByPriority *llrb.LLRB // priority -> *user
	globalOrder            int64
}

func NewQueue(n int64) *Queue {
	return &Queue{
		MaxActiveQuery:         n,
		waitingUsersByName:     map[string]*user{},
		waitingUsersByPriority: llrb.New(),
	}
}

func (q *Queue) incOrder() int64 {
	order := q.globalOrder
	q.globalOrder++
	return order

}

func (q *Queue) addUserQueryLocked(token string) (e *list.Element, qry *query, fastpath bool) {
	if u, ok := q.waitingUsersByName[token]; ok {
		qry = &query{
			token: token,
			ch:    make(chan struct{}),
		}
		e = u.qry.PushBack(qry)
	} else {
		order := q.incOrder()
		if q.activeQuery < q.MaxActiveQuery {
			q.activeQuery++
			return nil, nil, true
		}
		qry = &query{
			token: token,
			ch:    make(chan struct{}),
		}
		qrys := list.New()
		e = qrys.PushBack(qry)
		u = &user{
			qry:   qrys,
			order: order,
			token: token,
		}
		q.waitingUsersByName[token] = u
		q.waitingUsersByPriority.ReplaceOrInsert(u)
	}
	return e, qry, false
}

func (q *Queue) removeQueryLocked(token string, e *list.Element) {
	u, ok := q.waitingUsersByName[token]
	if !ok {
		return
	}
	u.qry.Remove(e)
	if u.qry.Len() == 0 {
		q.waitingUsersByPriority.Delete(u)
		delete(q.waitingUsersByName, token)
	}
}

func (q *Queue) Acquire(ctx context.Context, token string) error {
	q.mx.Lock()
	e, qry, fastpath := q.addUserQueryLocked(token)
	if fastpath {
		q.mx.Unlock()
		return nil
	}
	q.nextQueryLocked()
	q.mx.Unlock()
	select {
	case <-ctx.Done():
		err := ctx.Err()
		q.mx.Lock()
		defer q.mx.Unlock()
		if isClosed(qry.ch) {
			return nil
		}
		q.removeQueryLocked(token, e) // remove or nop
		return err
	case <-qry.ch:
	}
	return nil
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (q *Queue) nextQueryLocked() {
	if q.activeQuery == q.MaxActiveQuery {
		return
	}
	nextUserI := q.waitingUsersByPriority.DeleteMin()
	if nextUserI == nil {
		return
	}
	nextUser := nextUserI.(*user)
	if nextUser != nil {
		nextUser.order = q.incOrder()
		v := nextUser.qry.Remove(nextUser.qry.Front())
		ch := v.(*query).ch
		if nextUser.qry.Len() == 0 {
			delete(q.waitingUsersByName, nextUser.token)
		} else {
			q.waitingUsersByPriority.ReplaceOrInsert(nextUser)
		}
		q.activeQuery++
		close(ch)
	}
}

func (q *Queue) Release() {
	q.mx.Lock()
	defer q.mx.Unlock()
	q.activeQuery--
	q.nextQueryLocked()
}

func (q *Queue) Observe() (int64, error) {
	q.mx.Lock()
	defer q.mx.Unlock()
	return q.activeQuery, nil
}

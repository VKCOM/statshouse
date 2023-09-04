package util

import "context"

type Query interface {
}
type Queue interface {
	Acquire(ctx context.Context, token string) (Query, error)
	Release(qry Query)
}

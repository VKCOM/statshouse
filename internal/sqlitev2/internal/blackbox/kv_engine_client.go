package blackbox

import (
	"context"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlkv_engine"
)

type KVEngineClient interface {
	Get(key int64) (tlkv_engine.GetResponse, error)
	Put(key int64, value int64) (tlkv_engine.ChangeResponse, error)
	Incr(key int64, value int64) (tlkv_engine.ChangeResponse, error)
	Backup(prefix string) (tlkv_engine.BackupResponse, error)
	Check(kv map[int64]int64) (bool, error)
}

type kvEngine struct {
	client *tlkv_engine.Client
}

func (k kvEngine) Get(key int64) (resp tlkv_engine.GetResponse, _ error) {
	err := k.client.Get(context.Background(), tlkv_engine.Get{Key: key}, nil, &resp)
	return resp, err
}

func (k kvEngine) Put(key int64, value int64) (resp tlkv_engine.ChangeResponse, _ error) {
	err := k.client.Put(context.Background(), tlkv_engine.Put{Key: key, Value: value}, nil, &resp)
	return resp, err
}

func (k kvEngine) Incr(key int64, value int64) (resp tlkv_engine.ChangeResponse, _ error) {
	err := k.client.Inc(context.Background(), tlkv_engine.Inc{Key: key, Incr: value}, nil, &resp)
	return resp, err
}

func (k kvEngine) Backup(prefix string) (resp tlkv_engine.BackupResponse, _ error) {
	err := k.client.Backup(context.Background(), tlkv_engine.Backup{Prefix: prefix}, nil, &resp)
	return resp, err
}

func (k kvEngine) Check(kv map[int64]int64) (resp bool, _ error) {
	req := tlkv_engine.Check{}
	for k, v := range kv {
		req.Kv = append(req.Kv, tlkv_engine.Kv{
			Key:   k,
			Value: v,
		})
	}
	err := k.client.Check(context.Background(), req, nil, &resp)
	return resp, err
}

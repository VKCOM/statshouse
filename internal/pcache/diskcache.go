// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package pcache

import "time"

// DiskCache defines the interface for a persistent cache on disk.
type DiskCache interface {
	DiskSizeBytes() (int64, error)
	Close() error
	Get(ns string, key string) ([]byte, time.Time, time.Duration, error, bool)
	Set(ns string, key string, val []byte, update time.Time, ttl time.Duration) error
	List(ns string) ([]ListResult, error)
	ListKeys(ns string, limit, offset int) ([]ListKeysResult, error)
	VerySlowCountDoNotUse(ns string) (int, error)
	Erase(ns string, key string) error
	EraseNamespace(ns string) error
}

type ListResult struct {
	Value  []byte        `db:"value"`
	Update time.Time     `db:"update_time"`
	TTL    time.Duration `db:"ttl"`
}

type ListKeysResult struct {
	Key    string    `db:"key"`
	Update time.Time `db:"update_time"`
}

type ReadResult struct {
	Value  []byte        `db:"value"`
	Update time.Time     `db:"update_time"`
	TTL    time.Duration `db:"ttl"`
	Err    error
	Found  bool
}

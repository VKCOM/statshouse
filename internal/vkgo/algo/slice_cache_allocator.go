// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package algo

type SliceCacheAllocator[T any] struct {
	cache []*T
}

func (s *SliceCacheAllocator[T]) allocate() *T {
	if n := len(s.cache); n > 0 {
		t := s.cache[n-1]
		s.cache = s.cache[:n-1]
		return t
	}
	return new(T)
}

func (s *SliceCacheAllocator[T]) deallocate(t *T) {
	var empty T
	*t = empty
	s.cache = append(s.cache, t)
}

func NewSliceCacheAllocator[T any]() SliceCacheAllocator[T] {
	return SliceCacheAllocator[T]{
		cache: []*T{},
	}
}

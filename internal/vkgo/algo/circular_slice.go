// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package algo

type CircularSlice[T any] struct {
	elements  []T
	read_pos  int // 0..impl.size-1
	write_pos int // read_pos..read_pos + impl.size
}

func (s *CircularSlice[T]) Len() int {
	return s.write_pos - s.read_pos
}

func (s *CircularSlice[T]) Cap() int {
	return len(s.elements)
}

// Two parts of circular slice
func (s *CircularSlice[T]) Slices() ([]T, []T) {
	capacity := len(s.elements)
	if s.write_pos <= capacity {
		return s.elements[s.read_pos:s.write_pos], nil
	}
	return s.elements[s.read_pos:capacity], s.elements[0 : s.write_pos-capacity]
}

// Will also reduce capacity
func (s *CircularSlice[T]) Reserve(newCapacity int) {
	if newCapacity <= len(s.elements) { // fits perfectly, do nothing
		return
	}
	s1, s2 := s.Slices()
	elements := make([]T, newCapacity) // size will forever be equal to capacity
	off := copy(elements, s1)
	off += copy(elements[off:], s2)
	if off != len(s1)+len(s2) {
		panic("circular slice invariant violated in Reserve")
	}
	s.read_pos = 0
	s.write_pos = off
	s.elements = elements
}

func (s *CircularSlice[T]) PushBack(element T) {
	capacity := len(s.elements)
	if s.write_pos-s.read_pos > capacity { // cheap to test
		panic("circular slice invariant violated in PushBack")
	}
	if s.write_pos-s.read_pos == capacity {
		if capacity < 4 {
			capacity = 4
		}
		s.Reserve(capacity * 2)
		capacity = len(s.elements)
	}
	if s.write_pos < capacity {
		s.elements[s.write_pos] = element
	} else {
		s.elements[s.write_pos-capacity] = element
	}
	s.write_pos++
}

func (s *CircularSlice[T]) Front() T {
	if s.write_pos == s.read_pos {
		panic("empty circular slice")
	}
	return s.elements[s.read_pos]
}

func (s *CircularSlice[T]) Index(pos int) T {
	return *s.IndexRef(pos)
}

func (s *CircularSlice[T]) IndexRef(pos int) *T {
	if pos < 0 {
		panic("circular slice index < 0")
	}
	capacity := len(s.elements)
	offset := s.read_pos + pos
	if offset < capacity {
		return &s.elements[offset]
	}
	if offset >= s.write_pos {
		panic("circular slice index out of range")
	}
	return &s.elements[offset-capacity]
}

func (s *CircularSlice[T]) PopFront() T {
	if s.write_pos == s.read_pos {
		panic("empty circular slice")
	}
	element := s.elements[s.read_pos]
	var empty T
	s.elements[s.read_pos] = empty // do not prevent garbage collection from invisible parts of slice
	s.read_pos++
	capacity := len(s.elements)
	if s.read_pos >= capacity {
		s.read_pos -= capacity
		s.write_pos -= capacity
	}
	if s.read_pos == s.write_pos { // Maximize probability of single continuous slice
		s.read_pos = 0
		s.write_pos = 0
	}
	return element
}

func (s *CircularSlice[T]) Clear() {
	var empty T
	s1, s2 := s.Slices()
	for i := range s1 {
		s1[i] = empty
	}
	for i := range s2 {
		s2[i] = empty
	}
	s.read_pos = 0
	s.write_pos = 0
}

func (s *CircularSlice[T]) DeepAssign(other CircularSlice[T]) {
	*s = CircularSlice[T]{
		elements:  append([]T(nil), other.elements...),
		read_pos:  other.read_pos,
		write_pos: other.write_pos,
	}
}

func (s *CircularSlice[T]) Swap(other *CircularSlice[T]) {
	s.elements, other.elements = other.elements, s.elements
	s.write_pos, other.write_pos = other.write_pos, s.write_pos
	s.read_pos, other.read_pos = other.read_pos, s.read_pos
}

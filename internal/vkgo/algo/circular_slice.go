// Copyright 2022 V Kontakte LLC
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

// Two parts of circular slice
func (s *CircularSlice[T]) Slices() ([]T, []T) {
	size := len(s.elements)
	if s.write_pos <= size {
		return s.elements[s.read_pos:s.write_pos], nil
	}
	return s.elements[s.read_pos:size], s.elements[0 : s.write_pos-size]
}

func (s *CircularSlice[T]) Reserve(newSize int) {
	if newSize < s.Len() {
		newSize = s.Len()
	}
	s.growSlice(newSize)
}

func (s *CircularSlice[T]) growSlice(newSize int) {
	size := len(s.elements)
	if newSize < 4 {
		newSize = 4
	}
	s1, s2 := s.Slices()
	s.elements = make([]T, newSize)
	start := copy(s.elements, s1)
	copy(s.elements[start:], s2)
	s.read_pos = 0
	s.write_pos = size
}

func (s *CircularSlice[T]) PushBack(element T) {
	size := len(s.elements)
	if s.write_pos-s.read_pos == size {
		s.growSlice(size * 2)
		size = len(s.elements)
	}
	if s.write_pos < size {
		s.elements[s.write_pos] = element
	} else {
		s.elements[s.write_pos-size] = element
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
	if pos < 0 {
		panic("circular slice index < 0")
	}
	size := len(s.elements)
	offset := s.read_pos + pos
	if offset < size {
		return s.elements[offset]
	}
	if offset >= s.write_pos {
		panic("circular slice index out of range")
	}
	return s.elements[offset-size]
}

func (s *CircularSlice[T]) PopFront() T {
	if s.write_pos == s.read_pos {
		panic("empty circular slice")
	}
	element := s.elements[s.read_pos]
	var empty T
	s.elements[s.read_pos] = empty // do not prevent garbage collection from invisible parts of slice
	s.read_pos++
	size := len(s.elements)
	if s.read_pos >= size {
		s.read_pos -= size
		s.write_pos -= size
	}
	if s.read_pos == s.write_pos { // Maximize probability of single continuous slice
		s.read_pos = 0
		s.write_pos = 0
	}
	return element
}

func (s *CircularSlice[T]) Clear() {
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

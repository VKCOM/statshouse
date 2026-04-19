// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"sync"
)

type smootherStat struct {
	sum   float64
	count float64
}

type SmootherKey struct {
	A string
	B string
}

const smootherWindow = 120 // actually, we never want to change it
const smootherReportInterval = 30

// Smoother SignalSmoother stores short rolling statistics and returns a smoothed value
// using two overlapping windows (base + half-shifted).
//
// It is intended for control signals where we want both:
// 1. damping of per-tick noise
// 2. reasonable reactivity to load changes
type Smoother struct {
	mu           sync.Mutex
	lastReportTs uint32

	main map[uint32]map[SmootherKey]*smootherStat
	half map[uint32]map[SmootherKey]*smootherStat
}

func (s *Smoother) Init() {
	s.main = map[uint32]map[SmootherKey]*smootherStat{}
	s.half = map[uint32]map[SmootherKey]*smootherStat{}
}

func updateSmootherStat(m map[SmootherKey]*smootherStat, key SmootherKey, value float64) {
	st, ok := m[key]
	if !ok {
		st = &smootherStat{}
		m[key] = st
	}
	st.sum += value
	st.count++
}

func (s *Smoother) createWindowsLocked(ts uint32) (map[SmootherKey]*smootherStat, map[SmootherKey]*smootherStat) {
	tp := ts / smootherWindow
	m, ok := s.main[tp]
	if !ok {
		m = map[SmootherKey]*smootherStat{}
		s.main[tp] = m
	}
	htp := (ts + smootherWindow/2) / smootherWindow
	h, ok := s.half[htp]
	if !ok {
		h = map[SmootherKey]*smootherStat{}
		s.half[htp] = h
	}
	return m, h
}

func (s *Smoother) AddAndSmoothKey(key SmootherKey, ts uint32, value float64) float64 {
	value = max(value, 1)

	s.mu.Lock()
	defer s.mu.Unlock()

	mainWindow, halfWindow := s.createWindowsLocked(ts)
	updateSmootherStat(mainWindow, key, value)
	updateSmootherStat(halfWindow, key, value)

	a := mainWindow[key]
	b := halfWindow[key]

	ma := a.sum / max(a.count, 1)
	mb := b.sum / max(b.count, 1)

	// Estimators overlap, we take linear combination to smooth value
	weight := 2 * float64(ts%smootherWindow) / float64(smootherWindow)
	if weight > 1 {
		weight = 2 - weight
	}
	smoothed := ma*weight + mb*(1-weight)
	if smoothed < 1 {
		return 1
	}
	return smoothed
}

func (s *Smoother) GarbageCollect(oldestTs uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tp := oldestTs / smootherWindow
	htp := (oldestTs + smootherWindow/2) / smootherWindow

	for k := range s.main {
		if k < tp {
			delete(s.main, k)
		}
	}
	for k := range s.half {
		if k < htp {
			delete(s.half, k)
		}
	}
}

func (s *Smoother) ShouldReport(ts uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ts < s.lastReportTs+smootherReportInterval {
		return false
	}
	s.lastReportTs = ts
	return true
}

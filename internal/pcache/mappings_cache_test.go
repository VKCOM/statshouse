// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package pcache

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"pgregory.net/rand"
)

var btGlobal int

func BenchmarkDivision(b *testing.B) {
	c := math.MaxInt
	for i := 0; i < b.N; i++ {
		btGlobal = i / c
		c--
	}
}

func BenchmarkRand(b *testing.B) {
	rnd := rand.New()
	for i := 0; i < b.N; i++ {
		btGlobal += int(rnd.Int31())
	}
}

func newTestMappingsCache(fp *[]byte) *MappingsCache {
	c := LoadMappingsCacheSlice(fp, 170)
	c.deterministic = true
	return c
}

func TestAddValues(t *testing.T) {
	now := uint32(time.Now().Unix())
	var fp []byte
	cache := newTestMappingsCache(&fp)
	cache.AddValues(now, []MappingPair{{Str: "a", Value: 1}, {Str: "b", Value: 2}, {Str: "c", Value: 3}})
	cache.debugPrint(now, os.Stdout)
	a1 := now + 1
	a2 := a1 + 1
	v, ok := cache.GetValue(a1, "a")
	if v != 1 || !ok {
		t.Fail()
	}
	cache.debugPrint(now, os.Stdout)
	v, ok = cache.GetValue(a1, "a")
	if v != 1 || !ok {
		t.Fail()
	}
	cache.debugPrint(now, os.Stdout)
	v, ok = cache.GetValue(a1, "d")
	if v != 0 || ok {
		t.Fail()
	}
	cache.debugPrint(now, os.Stdout)
	cache.AddValues(now, []MappingPair{{Str: "d", Value: 4}, {Str: "e", Value: 5}, {Str: "f", Value: 6}})
	cache.debugPrint(now, os.Stdout)
	v, ok = cache.GetValue(a2, "a")
	if v != 1 || !ok {
		t.Fail()
	}
	cache.debugPrint(now, os.Stdout)
	v, ok = cache.GetValue(a2, "d")
	if v != 4 || !ok {
		t.Fail()
	}
	cache.debugPrint(now, os.Stdout)
	if err := cache.Save(); err != nil {
		t.Error(err)
	}
	fp2 := []byte(string(fp))
	cache = newTestMappingsCache(&fp2)
	cache.debugPrint(now, os.Stdout)
	if err := cache.Save(); err != nil {
		t.Error(err)
	}
	if string(fp) != string(fp2) {
		t.Fail()
	}
}

func generateRandomElements(count int, elements []MappingPair) []MappingPair {
	elements = elements[:0]
	for j := 0; j < count; j++ {
		v := rand.Int31()
		elements = append(elements, MappingPair{
			Str:   strconv.FormatInt(int64(v), 10),
			Value: v,
		})
	}
	return elements
}

func fillTestMappingsCache(now uint32, c *MappingsCache) []MappingPair {
	c.maxSize.Store(20000000)
	const testSize = 1024
	var elements []MappingPair
	for i := 0; i < 1000; i++ {
		elements = generateRandomElements(testSize, elements)
		t0 := now - uint32(rand.Int31n(3600))
		c.AddValues(t0, elements)
	}
	return elements
}

// Intel@2GHz: 50ns get from 450k values with sumSize 20MB
//
//	35ns without hits/misses statistics
func BenchmarkGetValues(b *testing.B) {
	now := uint32(time.Now().Unix())
	var fp []byte
	cache := newTestMappingsCache(&fp)
	elements := fillTestMappingsCache(now, cache)
	cache.debugPrint(now, os.Stdout)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		el := elements[i%len(elements)]
		v, ok := cache.GetValue(now, el.Str)
		if ok {
			btGlobal++
			if v != el.Value {
				fmt.Printf("hren\n")
			}
		}
	}
	cache.debugPrint(now, os.Stdout)
	b.ReportAllocs()
}

// Intel@2GHz: <1ms adding 1024 values to 450k values with sumSize 20MB
func BenchmarkAddValues(b *testing.B) {
	now := uint32(time.Now().Unix())
	var fp []byte
	cache := newTestMappingsCache(&fp)
	elements := fillTestMappingsCache(now, cache)
	cache.debugPrint(now, os.Stdout)
	var els [][]MappingPair
	for i := 0; i < b.N; i++ {
		e := generateRandomElements(len(elements), nil)
		els = append(els, e)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.AddValues(now, els[i])
	}
	cache.debugPrint(now, os.Stdout)
	b.ReportAllocs()
}

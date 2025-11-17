// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package algo

import (
	"log"
	"testing"

	"pgregory.net/rand"
)

type keyT = int32
type valT = int32

type testComp struct {
}

func (testComp) Cmp(k1 keyT, k2 keyT) bool {
	return k1 < k2
}

const iterations = 1000

func genKey() int32 {
	const M = int32(iterations)
	return rand.Int31n(2*M) - iterations
}

func genKeyValue() (int32, int32) {
	key := genKey()
	value := int32(rand.Int())
	return key, value
}

type testAlloc struct {
	inner       SliceCacheAllocator[TreeNode[Entry[keyT, valT]]]
	allocated   int
	deallocated int
}

func (a *testAlloc) allocate() *TreeNode[Entry[keyT, valT]] {
	a.allocated++
	return a.inner.allocate()
}

func (a *testAlloc) deallocate(t *TreeNode[Entry[keyT, valT]]) {
	a.deallocated++
	a.inner.deallocate(t)
}

func TestTreeMap(t *testing.T) {
	alloc := testAlloc{
		inner:       NewSliceCacheAllocator[TreeNode[Entry[keyT, valT]]](),
		allocated:   0,
		deallocated: 0,
	}
	// TODO kostil to let staticcheck know, that we use SliceCacheAllocator::allocate
	n := alloc.allocate()
	treeMap := NewTreeMap[keyT, valT, testComp](&alloc)
	stdMap := map[keyT]valT{}

	for i := 0; i < iterations; i++ {
		key, value := genKeyValue()
		treeMap.Set(key, value)
		stdMap[key] = value
		validateMaps(stdMap, &treeMap, &alloc)
	}

	for i := 0; i < iterations; i++ {
		key := genKey()
		val1, exists1 := stdMap[key]
		val2, exists2 := treeMap.Get(key)
		if exists1 != exists2 {
			log.Panicf("std map exists(%t) for key %+v != TreeMap exists(%t)", exists1, key, exists2)
		}
		if exists1 && val1 != val2 {
			log.Panicf("std map value(%+v) for key %+v != TreeMap value(%+v)", val1, key, val2)
		}
		validateMaps(stdMap, &treeMap, &alloc)
	}

	for i := 0; i < iterations; i++ {
		key := genKey()

		exists := treeMap.GetPtr(key) != nil
		wasDeallocated := alloc.deallocated
		treeMap.Delete(key)
		if exists {
			if alloc.deallocated != wasDeallocated+1 {
				log.Printf("treeMap.Delete(%d) of existing key didn't called deallocate()", key)
			}
		}

		delete(stdMap, key)
		validateMaps(stdMap, &treeMap, &alloc)
	}

	for key := range stdMap {
		treeMap.Delete(key)
	}

	// TODO kostil to let staticcheck know, that we use SliceCacheAllocator::deallocate
	alloc.deallocate(n)

	if alloc.allocated != alloc.deallocated {
		log.Panicf("allocated nodes (%d) != deallocated nodes (%d)", alloc.allocated, alloc.deallocated)
	}
}

func validateMaps(stdMap map[keyT]valT, treeMap *TreeMap[keyT, valT, testComp], alloc *testAlloc) {
	treeMap.validate()

	for k, v1 := range stdMap {
		v2, exists := treeMap.Get(k)
		if !exists {
			log.Panicf("key %+v exists in std map, but is missing in TreeMap", k)
		}
		if v1 != v2 {
			log.Panicf("maps has different values by key %+v: std map has %+v, but TreeMap has %+v", k, v1, v2)
		}
	}

	// TODO -1 is kostil
	if alloc.allocated-1-alloc.deallocated != len(stdMap) {
		log.Panicf("allocated nodes (%d) - deallocated nodes (%d) != std map len (%d)", alloc.allocated, alloc.deallocated, len(stdMap))
	}

	// TODO iterate over TreeMap
}

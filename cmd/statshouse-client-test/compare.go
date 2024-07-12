package main

import (
	"fmt"
	"reflect"
	"slices"
)

type diff struct {
	tags      [2]tags
	timestamp [2]uint32
	counter   [2]float64
	values    [2][]float64
	uniques   [2][]int64
}

func compareSeries(a, b series) (res diff) {
	var swap bool
	defer func() {
		if swap {
			res.swap()
		}
	}()
	if len(a) < len(b) {
		a, b = b, a
		swap = true
	}
	for tagsA, secondsA := range a {
		if secondsB, ok := b[tagsA]; ok {
			res = compareSeconds(secondsA, secondsB)
			if !res.empty() {
				return res
			}
		} else {
			return diff{tags: [2]tags{tagsA, {}}}
		}
	}
	// self check
	if !reflect.DeepEqual(a, b) {
		panic(fmt.Errorf("broken diff engine"))
	}
	return diff{}
}

func compareSeconds(a, b map[uint32]*value) (res diff) {
	var swap bool
	defer func() {
		if swap {
			res.swap()
		}
	}()
	if len(a) < len(b) {
		a, b = b, a
		swap = true
	}
	for timestampA, valueA := range a {
		if valueB, ok := b[timestampA]; ok {
			res = compareValues(valueA, valueB)
			if !res.empty() {
				return res
			}
		} else {
			return diff{timestamp: [2]uint32{timestampA, 0}}
		}
	}
	return diff{}
}

func compareValues(a, b *value) diff {
	if a.counter != b.counter {
		return diff{counter: [2]float64{a.counter, b.counter}}
	}
	if !slices.Equal(a.values, b.values) {
		return diff{values: [2][]float64{a.values, b.values}}
	}
	if !slices.Equal(a.uniques, b.uniques) {
		return diff{uniques: [2][]int64{a.uniques, b.uniques}}
	}
	return diff{}
}

func (d *diff) swap() {
	d.tags[0], d.tags[1] = d.tags[1], d.tags[0]
	d.counter[0], d.counter[1] = d.counter[1], d.counter[0]
	d.values[0], d.values[1] = d.values[1], d.values[0]
}

func (d *diff) empty() bool {
	if d.tags[0] != d.tags[1] {
		return false
	}
	if d.timestamp[0] != d.timestamp[1] {
		return false
	}
	if d.counter[0] != d.counter[1] {
		return false
	}
	if !slices.Equal(d.values[0], d.values[1]) {
		return false
	}
	if !slices.Equal(d.uniques[0], d.uniques[1]) {
		return false
	}
	return true
}

func (d *diff) String() string {
	if d.tags[0] != d.tags[1] {
		return "tags mistmatch"
	}
	if d.timestamp[0] != d.timestamp[1] {
		return "timestamp mistmatch"
	}
	if d.counter[0] != d.counter[1] {
		return "counter mistmatch"
	}
	if !slices.Equal(d.values[0], d.values[1]) {
		return "values mistmatch"
	}
	if !slices.Equal(d.values[0], d.values[1]) {
		return "values mistmatch"
	}
	if !slices.Equal(d.uniques[0], d.uniques[1]) {
		return "uniques mistmatch"
	}
	return ""
}

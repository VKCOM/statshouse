package data_model

import (
	"testing"

	"pgregory.net/rand"
)

// Stupid tests that should allow refactoring of primitives in data_model

func testEqual(t testing.TB, a ItemValue, b ItemValue) {
	if a != b {
		t.Fatalf("%+v != %+v", a, b)
	}
}

func testEqualNB(t testing.TB, a ItemValue, b ItemValue) {
	a.MaxCounterHostTagId = b.MaxCounterHostTagId // NB
	testEqual(t, a, b)
}

func TestItemValue(t *testing.T) {
	rng := rand.New()
	// create
	initialSc := ItemValue{
		ItemCounter: ItemCounter{
			counter:             2,
			MaxCounterHostTagId: 7,
		},
	}
	sc := SimpleItemCounter(2, 7)
	testEqual(t, sc, initialSc)
	// add to empty
	sc2 := ItemValue{}
	sc2.AddCounterHost(rng, 2, 7)
	testEqual(t, sc2, initialSc)
	// merge with empty
	sc2 = sc
	sc2.Merge(rng, &ItemValue{})
	testEqual(t, sc2, initialSc)
	// merge empty with full
	sc2 = ItemValue{}
	sc2.Merge(rng, &sc)
	testEqual(t, sc2, initialSc)
	// add
	sc2 = sc
	sc2.AddCounterHost(rng, 3, 9)
	testEqualNB(t, sc2, ItemValue{
		ItemCounter: ItemCounter{
			counter:             5,
			MaxCounterHostTagId: 9,
		},
	})

	// create
	initialIV := ItemValue{
		ItemCounter: ItemCounter{
			counter:             2,
			MaxCounterHostTagId: 7,
		},
		ValueMin:       5,
		ValueMax:       5,
		ValueSum:       10,
		ValueSumSquare: 50,
		MinHostTagId:   7,
		MaxHostTagId:   7,
		ValueSet:       true,
	}
	iv := SimpleItemValue(5, 2, 7)
	testEqual(t, iv, initialIV)
	// add to empty
	iv2 := ItemValue{}
	iv2.AddValueCounterHost(rng, 5, 2, 7)
	testEqual(t, iv2, initialIV)
	// merge with empty
	iv2 = iv
	iv2.Merge(rng, &ItemValue{})
	testEqual(t, iv2, initialIV)
	// merge empty with full
	iv2 = ItemValue{}
	iv2.Merge(rng, &iv)
	testEqual(t, iv2, initialIV)
	// add
	iv2 = iv
	iv2.AddValueCounter(3, 3)
	testEqual(t, iv2, ItemValue{
		ItemCounter: ItemCounter{
			counter:             5,
			MaxCounterHostTagId: 7,
		},
		ValueMin:       3,
		ValueMax:       5,
		ValueSum:       19,
		ValueSumSquare: 77,
		MinHostTagId:   0,
		MaxHostTagId:   7,
		ValueSet:       true,
	})
	// add
	iv2 = iv
	iv2.AddValueCounterHost(rng, 3, 3, 6)
	addIV := ItemValue{
		ItemCounter: ItemCounter{
			counter:             5,
			MaxCounterHostTagId: 6,
		},
		ValueMin:       3,
		ValueMax:       5,
		ValueSum:       19,
		ValueSumSquare: 77,
		MinHostTagId:   6,
		MaxHostTagId:   7,
		ValueSet:       true,
	}
	testEqualNB(t, iv2, addIV)
	// add 1 element array
	iv2 = iv
	iv2.AddValueArrayHost(rng, []float64{3}, 3, 6)
	testEqualNB(t, iv2, addIV)
	// add array
	iv2 = iv
	iv2.AddValueArrayHost(rng, []float64{1, 4}, 0.5, 6)
	addIVArray := ItemValue{
		ItemCounter: ItemCounter{
			counter:             3,
			MaxCounterHostTagId: 6,
		},
		ValueMin:       1,
		ValueMax:       5,
		ValueSum:       12.5,
		ValueSumSquare: 58.5,
		MinHostTagId:   6,
		MaxHostTagId:   7,
		ValueSet:       true,
	}
	testEqualNB(t, iv2, addIVArray)
	// merge value with counter
	iv2 = iv
	iv2.Merge(rng, &sc)
	testEqualNB(t, iv2, ItemValue{
		ItemCounter: ItemCounter{
			counter:             4,
			MaxCounterHostTagId: 6,
		},
		ValueMin:       5,
		ValueMax:       5,
		ValueSum:       10,
		ValueSumSquare: 50,
		MinHostTagId:   7,
		MaxHostTagId:   7,
		ValueSet:       true,
	})
	// merge counter with value
	iv2 = sc
	iv2.Merge(rng, &iv)
	testEqualNB(t, iv2, ItemValue{
		ItemCounter: ItemCounter{
			counter:             4,
			MaxCounterHostTagId: 6,
		},
		ValueMin:       5,
		ValueMax:       5,
		ValueSum:       10,
		ValueSumSquare: 50,
		MinHostTagId:   7,
		MaxHostTagId:   7,
		ValueSet:       true,
	})

	mv := MultiValue{}
	mv.AddValueCounterHost(rng, 5, 2, 7)
	testEqual(t, mv.Value, initialIV)

	// add value via array
	mv2 := mv
	mv2.ApplyValues(rng, nil, []float64{3}, 3, 1, 6, 0, false)
	testEqualNB(t, mv2.Value, addIV)
	// add value via histogram
	mv2 = mv
	mv2.ApplyValues(rng, [][2]float64{{3, 1}}, nil, 3, 1, 6, 0, false)
	testEqualNB(t, mv2.Value, addIV)
	// add array
	mv2 = mv
	mv2.ApplyValues(rng, nil, []float64{1, 4}, 1, 2, 6, 0, false)
	testEqualNB(t, mv2.Value, addIVArray)
	// add histogram
	mv2 = mv
	mv2.ApplyValues(rng, [][2]float64{{1, 1}, {4, 1}}, nil, 1, 2, 6, 0, false)
	testEqualNB(t, mv2.Value, addIVArray)
	// add array + histogram
	mv2 = mv
	mv2.ApplyValues(rng, [][2]float64{{1, 1}}, []float64{4}, 1, 2, 6, 0, false)
	testEqualNB(t, mv2.Value, addIVArray)

	// unique value via array
	mv2 = mv
	mv2.ApplyUnique(rng, []int64{3}, 3, 6)
	testEqualNB(t, mv2.Value, addIV)
	// unique
	mv2 = mv
	mv2.ApplyUnique(rng, []int64{1, 4}, 1, 6)
	testEqualNB(t, mv2.Value, addIVArray)
}

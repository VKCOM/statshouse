// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

type DigestWhat int
type DigestKind int

const (
	DigestUnspecified DigestWhat = iota
	DigestAvg
	DigestCount
	DigestCountSec
	DigestCountRaw
	DigestMax
	DigestMin
	DigestSum
	DigestSumSec
	DigestSumRaw
	DigestP0_1
	DigestP1
	DigestP5
	DigestP10
	DigestP25
	DigestP50
	DigestP75
	DigestP90
	DigestP95
	DigestP99
	DigestP999
	DigestStdDev
	DigestStdVar
	DigestCardinality
	DigestCardinalitySec
	DigestCardinalityRaw
	DigestUnique
	DigestUniqueSec
	DigestUniqueRaw
)

const (
	DigestKindUnspecified DigestKind = iota
	DigestKindCount
	DigestKindValue
	DigestKindPercentiles
	DigestKindPercentilesLow
	DigestKindUnique
)

func (d DigestWhat) Kind(maxhost bool) DigestKind {
	switch d {
	case DigestCount, DigestCountSec, DigestCountRaw,
		DigestCardinality, DigestCardinalitySec, DigestCardinalityRaw:
		if maxhost {
			return DigestKindValue
		}
		return DigestKindCount
	case DigestMin, DigestMax, DigestAvg,
		DigestSum, DigestSumSec, DigestSumRaw,
		DigestStdDev, DigestStdVar:
		return DigestKindValue
	case DigestP0_1, DigestP1, DigestP5, DigestP10:
		return DigestKindPercentilesLow
	case DigestP25, DigestP50, DigestP75, DigestP90, DigestP95, DigestP99, DigestP999:
		return DigestKindPercentiles
	case DigestUnique, DigestUniqueSec, DigestUniqueRaw:
		return DigestKindUnique
	default:
		return DigestKindUnspecified
	}
}

func (k DigestKind) String() string {
	var res string
	switch k {
	case DigestKindCount:
		res = "count"
	case DigestKindValue:
		res = "value"
	case DigestKindPercentiles:
		res = "percentiles"
	case DigestKindPercentilesLow:
		res = "percentiles_low"
	case DigestKindUnique:
		res = "unique"
	}
	return res
}

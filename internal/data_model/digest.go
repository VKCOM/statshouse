// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import "fmt"

type DigestSelector struct {
	What     DigestWhat
	Argument float64
}

type DigestWhat int

const (
	DigestUnspecified DigestWhat = iota
	DigestAvg
	DigestCount
	DigestMax
	DigestMin
	DigestSum
	DigestPercentile
	DigestStdDev
	DigestCardinality
	DigestUnique
	DigestLast
)

func (k DigestSelector) String() string {
	switch k.What {
	case DigestAvg:
		return "avg"
	case DigestCount:
		return "count"
	case DigestMax:
		return "max"
	case DigestMin:
		return "min"
	case DigestSum:
		return "sum"
	case DigestPercentile:
		return fmt.Sprintf("p%.3f", k.Argument)
	case DigestCardinality:
		return "cardinality"
	case DigestUnique:
		return "unique"
	default:
		return ""
	}
}

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"math"
)

func generateConstCounter(lod lodInfo) ([][]tsSelectRow, error) {
	const (
		constValue = 1000
	)
	fromSec := lod.fromSec
	rows := make([][]tsSelectRow, lod.getIndexForTimestamp(lod.toSec, 0))
	for i := range rows {
		rows[i] = []tsSelectRow{
			{
				time:     fromSec,
				stepSec:  lod.stepSec,
				tsValues: tsValues{countNorm: constValue * float64(lod.stepSec)},
			},
		}
		rows[i][0].val[0] = float64(lod.stepSec)
		fromSec += lod.stepSec
	}
	return rows, nil
}

func generateSinCounter(lod lodInfo) ([][]tsSelectRow, error) {
	const (
		sinPeriod    = 24 * 3600
		sinShift     = 15 * 3600 // adjust peaks/lows to Moscow TZ
		sinAmplitude = 1000.0
	)
	fromSec := lod.fromSec
	rows := make([][]tsSelectRow, lod.getIndexForTimestamp(lod.toSec, 0))
	for i := range rows {
		sum := 0.0
		toSec := fromSec + lod.stepSec
		for j := fromSec; j < toSec; j++ {
			sum += math.Sin(2 * math.Pi * (float64(j) + 0.5 + sinShift) / sinPeriod)
		}
		countNorm := sinAmplitude*float64(lod.stepSec) + sinAmplitude*sum
		rows[i] = []tsSelectRow{
			{
				time:     fromSec,
				stepSec:  lod.stepSec,
				tsValues: tsValues{countNorm: countNorm},
			},
		}
		rows[i][0].val[0] = float64(lod.stepSec)
		fromSec += lod.stepSec
	}
	return rows, nil
}

func generateGapsCounter(lod lodInfo) ([][]tsSelectRow, error) {
	const (
		gapShift     = 21 * 3600 // adjust peaks/lows to Moscow TZ
		sinAmplitude = 1000.0
	)
	fromSec := lod.fromSec
	rows := make([][]tsSelectRow, lod.getIndexForTimestamp(lod.toSec, 0))
	for i := range rows {
		sum := 0.0
		toSec := fromSec + lod.stepSec
		for j := fromSec; j < toSec; j++ {
			hour := (j + gapShift) / 3600
			switch hour % 4 {
			case 0, 1:
				if (j+gapShift)%3600 == 0 {
					sum += sinAmplitude
				}
			case 2:
				if (j+gapShift)%60 == 0 {
					sum += sinAmplitude
				}
			default:
				if (j+gapShift)%5 == 0 {
					sum += sinAmplitude + 4*(sinAmplitude/50)
				} else {
					sum += sinAmplitude - 1*(sinAmplitude/50)
				}
			}
		}
		if sum == 0 {
			sum = math.NaN()
		}
		rows[i] = []tsSelectRow{
			{
				time:     fromSec,
				stepSec:  lod.stepSec,
				tsValues: tsValues{countNorm: sum},
			},
		}
		rows[i][0].val[0] = float64(lod.stepSec)
		fromSec += lod.stepSec
	}
	return rows, nil
}

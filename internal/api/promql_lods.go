// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"time"
)

type lodSlimInfo struct {
	len  int64
	step int64
}

func calcSlimLODs(start, end, step, now int64) []lodSlimInfo {
	var (
		from = start
		lods []lodSlimInfo
	)
	for _, lodSwitch := range lodLevels[Version2] {
		cut := now - lodSwitch.relSwitch
		if cut < from {
			continue
		}
		to := end
		if cut < end {
			to = cut
		}
		lodStep := lodSwitch.levels[0]
		for _, l := range lodSwitch.levels {
			if l < step {
				break
			}
			lodStep = l
		}
		lodLen := lodStep * ((to-from)/lodStep + 1)
		lods = append(lods, lodSlimInfo{len: lodLen, step: lodStep})
		from += lodLen
		if from >= end {
			break
		}
	}
	mergeAdjacentEqualStepLODs(&lods)
	return lods
}

func calcLODs(s []lodSlimInfo, from, preKeyFrom int64, l *time.Location) ([]lodInfo, int) {
	// calculate LODs from slim LODs, split by preKey
	var (
		ret = make([]lodInfo, 0, len(s)+1)
		len int // resulting vector length
	)
	for _, lod := range s {
		len += int(lod.len / lod.step)
		to := from + lod.len
		table := lodTables[Version2][lod.step]
		if from < preKeyFrom && preKeyFrom < to {
			// split LOD by preKey
			split := lod.step * ((preKeyFrom-from-1)/lod.step + 1)
			ret = append(ret,
				lodInfo{
					fromSec:   from,
					toSec:     split,
					stepSec:   lod.step,
					table:     table,
					hasPreKey: false,
					location:  l,
				},
				lodInfo{
					fromSec:   split,
					toSec:     to,
					stepSec:   lod.step,
					table:     table,
					hasPreKey: true,
					location:  l,
				})
		} else {
			ret = append(ret, lodInfo{
				fromSec:   from,
				toSec:     to,
				stepSec:   lod.step,
				table:     table,
				hasPreKey: preKeyFrom <= from,
				location:  l,
			})
		}
		from = to
	}
	return ret, len
}

func alignQueryLODs(a []*promQuery, b []*promQuery) {
	if len(a) == 0 || len(b) == 0 {
		return
	}
	aa := a[0]
	bb := b[0]
	for i := 0; i < len(aa.lods) && i < len(bb.lods); i++ {
		if aa.lods[i].len < bb.lods[i].len {
			splitQueryLOD(b, i, aa.lods[i].len)
		} else if bb.lods[i].len < aa.lods[i].len {
			splitQueryLOD(a, i, bb.lods[i].len)
		}
		if aa.lods[i].step < bb.lods[i].step {
			setQueryStep(a, i, bb.lods[i].step)
		} else if bb.lods[i].step < aa.lods[i].step {
			setQueryStep(b, i, aa.lods[i].step)
		}
	}
	for _, qry := range a {
		mergeAdjacentEqualStepLODs(&qry.lods)
	}
	for _, qry := range b {
		mergeAdjacentEqualStepLODs(&qry.lods)
	}
}

func splitQueryLOD(s []*promQuery, i int, len int64) {
	for _, vq := range s {
		vq.lods = append(vq.lods[:i+1], vq.lods[i:]...)
		vq.lods[i].len = len
		vq.lods[i+1].len -= len
	}
}

func setQueryStep(q []*promQuery, i int, step int64) {
	for _, vq := range q {
		vq.lods[i].step = step
	}
}

func mergeAdjacentEqualStepLODs(s *[]lodSlimInfo) {
	ss := *s
	for i := 1; i < len(ss); i++ {
		if ss[i-1].step == ss[i].step {
			ss[i-1].len += ss[i].len
			ss = append(ss[:i], ss[i+1:]...)
		}
	}
	*s = ss
}

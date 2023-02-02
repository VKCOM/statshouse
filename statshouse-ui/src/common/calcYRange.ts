// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import uPlot from 'uplot';
import { minMax, range0 } from '../view/utils';

export function calcYRange(u: uPlot, onlyVisible: boolean): [number, number] {
  let dataMin = null;
  let dataMax = null;
  for (let i = 1; i < u.series.length; i++) {
    const s = u.series[i];
    if ((onlyVisible && s.show !== true) || s.idxs === undefined) {
      continue;
    }
    const sData = u.data[i] as (number | null)[];
    if (sData === undefined) {
      continue; // we have e.g. initial dummy Y series from options, but no data yet
    }
    // uPlot excludes points not on the graph itself; because of this:
    // - leftmost line segment is tied to point that may be "offscreen" after selection,
    //   in that case for the calculated range to be valid we have to explicitly include it:
    const from = s.idxs[0] > 0 ? s.idxs[0] - 1 : s.idxs[0];
    // - rightmost point suddenly is included in calculations when s.idxs[1] === undefined,
    //   so try to include it prematurely for range calculations to be more stable:
    const to = s.idxs[1] < sData.length - 1 ? s.idxs[1] + 1 : s.idxs[1];
    const [yMin, yMax] = range0(minMax([sData], from, to));
    if (yMin !== null && (dataMin === null || yMin < dataMin)) {
      dataMin = yMin;
    }
    if (yMax !== null && (dataMax === null || yMax > dataMax)) {
      dataMax = yMax;
    }
  }

  if (dataMin === null || dataMax === null) {
    return [0, 100];
  }

  return uPlot.rangeNum(dataMin, dataMax, 0.1, true) as [number, number];
}

export function calcYRange2(series: uPlot.Series[], data: uPlot.AlignedData, onlyVisible: boolean): [number, number] {
  let dataMin = null;
  let dataMax = null;
  for (let i = 0; i < series.length; i++) {
    const s = series[i];
    if (onlyVisible && s.show !== true) {
      continue;
    }
    const sData = data[i + 1] as (number | null)[];
    if (sData === undefined) {
      continue; // we have e.g. initial dummy Y series from options, but no data yet
    }
    const [yMin, yMax] = range0(minMax([sData]));
    if (yMin !== null && (dataMin === null || yMin < dataMin)) {
      dataMin = yMin;
    }
    if (yMax !== null && (dataMax === null || yMax > dataMax)) {
      dataMax = yMax;
    }
  }
  if (dataMin === null || dataMax === null) {
    return [0, 100];
  }

  return uPlot.rangeNum(dataMin, dataMax, 0.1, true) as [number, number];
}

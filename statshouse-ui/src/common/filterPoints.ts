// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import uPlot from 'uplot';

/**
 *
 * @see https://leeoniya.github.io/uPlot/demos/points.html
 */
export function filterPoints(u: uPlot, seriesIdx: number, show: boolean, _gaps?: null | number[][]): number[] | null {
  const filtered = [];
  const series = u.series[seriesIdx];
  const [firstIdx, lastIdx] = series.idxs!;
  const yData = u.data[seriesIdx];

  if (!show) {
    for (let ix = firstIdx; ix <= lastIdx; ix++) {
      if (
        yData[ix] !== null &&
        ((ix > firstIdx && yData[ix - 1] === null) || (ix < lastIdx && yData[ix + 1] === null))
      ) {
        filtered.push(ix);
      }
    }
  }

  return filtered.length ? filtered : null;
}

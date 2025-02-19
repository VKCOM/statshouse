// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import uPlot from 'uplot';

export type SeriesValues = { idx: number | null; seriesIdx: number; dataValue: number | null };
export const seriesValues: uPlot.Series.Values = (u, seriesIdx, idx): SeriesValues => ({
  idx,
  seriesIdx,
  dataValue: idx != null ? (u.data[seriesIdx]?.[idx] ?? null) : null,
});

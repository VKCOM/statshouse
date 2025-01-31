// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import uPlot from 'uplot';

import { fmtInputDateTime } from '@/view/utils2';

export function dateRangeFormat(
  self: uPlot,
  rawValue: number,
  _seriesIdx: number,
  idx: number | null
): string | number {
  if (idx === null) {
    return rawValue;
  }
  const xValues = self.data[0];
  const nextValue = xValues[idx + 1];
  const suffix = nextValue === undefined || nextValue - rawValue === 1 ? '' : '  Δ' + (nextValue - rawValue) + 's';
  return fmtInputDateTime(new Date(rawValue * 1000)) + suffix;
}

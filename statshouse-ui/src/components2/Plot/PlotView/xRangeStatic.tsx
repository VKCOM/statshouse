// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import uPlot from 'uplot';

import { now } from '@/view/utils2';

export function xRangeStatic(_u: uPlot, dataMin: number | null, dataMax: number | null): [number, number] {
  if (dataMin === null || dataMax === null) {
    const t = now();
    return [t - 3600, t];
  }
  return [dataMin, dataMax];
}

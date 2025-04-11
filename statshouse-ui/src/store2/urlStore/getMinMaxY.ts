// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { calcYRange2 } from '@/common/calcYRange';
import { PlotData } from '@/store2/plotDataStore';

export function getMinMaxY(plotData?: PlotData): {
  min: number;
  max: number;
} {
  if (plotData) {
    const [min, max] = calcYRange2(plotData.series, plotData.data, true);
    return { min, max };
  }
  return { min: 0, max: 0 };
}

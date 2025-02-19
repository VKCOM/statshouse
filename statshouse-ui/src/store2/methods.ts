// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useStatsHouse } from '@/store2/statsHouseStore';

export const {
  setParams,
  setPlot,
  removePlot,
  setPlotYLock,
  setTimeRange,
  resetZoom,
  timeRangePanLeft,
  timeRangePanRight,
  timeRangeZoomIn,
  timeRangeZoomOut,
  setPlotType,
  removePlotHeals,
} = useStatsHouse.getState();

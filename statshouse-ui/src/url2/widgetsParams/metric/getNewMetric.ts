// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { LayoutInfo, PlotParams } from '@/url2';
import { METRIC_VALUE_BACKEND_VERSION, PLOT_TYPE, PlotType, QUERY_WHAT } from '@/api/enum';
import { LAYOUT_COLUMNS, LAYOUT_WIDGET_SIZE } from '@/components2/Dashboard/DashboardGridLayout/constant';

export function getNewMetric(): PlotParams {
  return {
    id: '',
    metricName: '',
    customName: '',
    customDescription: '',
    promQL: '',
    metricUnit: undefined,
    what: [QUERY_WHAT.countNorm],
    customAgg: 0,
    groupBy: [],
    filterIn: {},
    filterNotIn: {},
    numSeries: 5,
    backendVersion: METRIC_VALUE_BACKEND_VERSION.v2,
    yLock: {
      min: 0,
      max: 0,
    },
    maxHost: false,
    type: PLOT_TYPE.Metric,
    events: [],
    eventsBy: [],
    eventsHide: [],
    totalLine: false,
    filledGraph: true,
    logScale: false,
    timeShifts: [],
    prometheusCompat: false,
  };
}

export const defaultMetric = Object.freeze(getNewMetric());

export function getNewMetricLayout(
  type: PlotType = PLOT_TYPE.Metric,
  size: string = '2',
  offset?: { x: number; y: number }
): LayoutInfo {
  let col = offset?.x ?? 0;
  let row = offset?.y ?? 0;
  const { minH, minW } = LAYOUT_WIDGET_SIZE[type] ?? { minH: 1, minW: 1 };
  let w: number = minW;
  switch (size) {
    case '1':
      w = LAYOUT_COLUMNS;
      break;
    case '2':
    case 'l':
      w = LAYOUT_COLUMNS / 2;
      break;
    case '3':
    case 'm':
      w = LAYOUT_COLUMNS / 3;
      break;
    case '4':
    case 's':
      w = LAYOUT_COLUMNS / 4;
      break;
    case '6':
      w = LAYOUT_COLUMNS / 6;
      break;
  }
  const h = Math.max(w, minH);

  if (col + w > LAYOUT_COLUMNS) {
    col = 0;
    row += h;
  }
  return {
    x: col,
    y: row,
    w: w,
    h: h,
  };
}

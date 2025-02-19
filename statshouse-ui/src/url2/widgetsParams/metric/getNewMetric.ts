// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { PlotParams } from '@/url2';
import { METRIC_VALUE_BACKEND_VERSION, PLOT_TYPE, QUERY_WHAT } from '@/api/enum';

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

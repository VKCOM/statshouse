// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { METRIC_TYPE } from 'api/enum';
import { type PlotData } from './plotsDataStore';
import { autoAgg } from '../constants';

export function getEmptyPlotData(): PlotData {
  return {
    data: [],
    dataView: [],
    bands: [],
    series: [],
    seriesShow: [],
    // scales: {},
    promQL: '',
    metricName: '',
    metricWhat: '',
    whats: [],
    plotAgg: autoAgg,
    showMetricName: '',
    metricUnit: METRIC_TYPE.none,
    lastHeals: true,
    error: '',
    error403: '',
    errorSkipCount: 0,
    seriesTimeShift: [],
    // lastPlotParams?: PlotParams;
    // lastTimeRange?: TimeRange;
    // lastTimeShifts?: number[];
    // lastQuerySeriesMeta?: querySeriesMeta[];
    receiveErrors: 0,
    receiveWarnings: 0,
    samplingFactorSrc: 0,
    samplingFactorAgg: 0,
    mappingFloodEvents: 0,
    legendValueWidth: 0,
    legendMaxDotSpaceWidth: 0,
    legendNameWidth: 0,
    legendPercentWidth: 0,
    legendMaxHostWidth: 0,
    legendMaxHostPercentWidth: 0,
    // topInfo?: TopInfo;
    maxHostLists: [],
    promqltestfailed: false,
    promqlExpand: false,
  };
}

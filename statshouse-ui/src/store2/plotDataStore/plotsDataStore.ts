// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import uPlot from 'uplot';
import type { MetricType, QueryWhat } from '@/api/enum';
import type { PlotKey, PlotParams, TimeRange } from '@/url2';
import type { QuerySeriesMeta } from '@/api/query';
import { createStore } from '@/store2/createStore';
import { ProduceUpdate } from '@/store2/helpers';
import { produce } from 'immer';
import { getEmptyPlotData } from '@/store2/plotDataStore/getEmptyPlotData';
import { updateTitle } from '@/store2/helpers/updateTitle';
import { useStatsHouse } from '@/store2';

export type PlotValues = {
  rawValue: number | null;
  value: string;
  seriesIdx: number;
  idx: number | null;
};

export type TopInfo = {
  top: string;
  total: string;
  info: string;
};

export type PlotDataSeries = uPlot.Series & {
  label: string | undefined;
};

export type PlotData = {
  data: uPlot.AlignedData;
  dataView: uPlot.AlignedData;
  bands?: uPlot.Band[];
  series: PlotDataSeries[];
  seriesShow: boolean[];
  promQL: string;
  metricName: string;
  metricWhat: string;
  whats: QueryWhat[];
  plotAgg: string;
  showMetricName: string;
  metricUnit: MetricType;
  lastHeals: boolean;
  error: string;
  error403?: string;
  errorSkipCount: number;
  seriesTimeShift: number[];
  lastPlotParams?: PlotParams;
  lastTimeRange?: TimeRange;
  lastTimeShifts?: number[];
  lastQuerySeriesMeta?: QuerySeriesMeta[];
  loadBadges?: boolean;
  receiveErrors: number;
  receiveWarnings: number;
  samplingFactorSrc: number;
  samplingFactorAgg: number;
  mappingFloodEvents: number;
  legendValueWidth: number;
  legendMaxDotSpaceWidth: number;
  legendNameWidth: number;
  legendPercentWidth: number;
  legendMaxHostWidth: number;
  legendMaxHostPercentWidth: number;
  topInfo?: TopInfo;
  promqltestfailed?: boolean;
  promqlExpand: boolean;
};

export type PlotsDataStore = {
  plotsData: Partial<Record<PlotKey, PlotData>>;
};

export const usePlotsDataStore = createStore<PlotsDataStore>(() => ({
  plotsData: {},
}));

export function setPlotData(plotKey: PlotKey, next: ProduceUpdate<PlotData>) {
  usePlotsDataStore.setState((s) => {
    s.plotsData[plotKey] = produce(s.plotsData[plotKey] ?? getEmptyPlotData(), next);
  });
}

export function clearPlotData(plotKey: PlotKey) {
  usePlotsDataStore.setState((s) => {
    delete s.plotsData[plotKey];
  });
}

usePlotsDataStore.subscribe((state, prevState) => {
  const {
    params: { tabNum, dashboardName, plots },
  } = useStatsHouse.getState();
  if (state.plotsData[tabNum] !== prevState.plotsData[tabNum]) {
    updateTitle(tabNum, dashboardName, plots[tabNum]);
  }
});

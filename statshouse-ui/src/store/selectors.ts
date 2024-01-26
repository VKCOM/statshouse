// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type Store } from './statshouse';
import { TimeRange } from '../common/TimeRange';
import { EventData } from './statshouse';
import { MetricMetaValue } from '../api/metric';

export const selectorParams = (s: Store) => s.params;
export const selectorParamsPlots = (s: Store) => s.params.plots;
export const selectorParamsPlotsByIndex = (index: number, s: Store) => s.params.plots[index];

export const selectorParamsTimeShifts = (s: Store) => s.params.timeShifts;
export const selectorParamsTabNum = (s: Store) => s.params.tabNum;
export const selectorTimeRange = (s: Store) => s.timeRange;
export const selectorSetTimeRange = (s: Store) => s.setTimeRange;
export const selectorSetParams = (s: Store) => s.setParams;

export const selectorDisabledLive = (s: Store) => !s.params.plots.every(({ useV2 }) => useV2);

export const selectorGlobalNumQueriesPlot = (s: Store) => s.globalNumQueriesPlot;

export const selectorNumQueriesPlotByIndex = (index: number, s: Store) => s.numQueriesPlot[index] ?? 0;

export const selectorBaseRange = (s: Store) => s.baseRange;
export const selectorSetBaseRange = (s: Store) => s.setBaseRange;

export const selectorUPlotsWidthByIndex = (index: number, s: Store) => s.uPlotsWidth[index];

export const selectorPlotsData = (s: Store) => s.plotsData;
export const selectorPlotsDataByIndex = (index: number, s: Store) => s.plotsData[index] ?? {};

export const selectorMetricsMeta = (s: Store) => s.metricsMeta;
export const selectorMetricsMetaByName = (name: string, s: Store): MetricMetaValue | undefined => s.metricsMeta[name];

export const selectorParamsTagSync = (s: Store) => s.params.tagSync;

export const selectorSaveServerParams = (s: Store) => s.saveServerParams;
export const selectorRemoveServerParams = (s: Store) => s.removeServerParams;
export const selectorIsServer = (s: Store) => s.params.dashboard?.dashboard_id !== undefined;
export const selectorDashboardId = (s: Store) => s.params.dashboard?.dashboard_id;

export const selectorDashboardPlotList = (s: Store) =>
  s.params.plots.map((plot, indexPlot) => ({
    plot,
    group:
      s.params.dashboard?.groupInfo?.flatMap((g, indexGroup) => new Array(g.count).fill(indexGroup))[indexPlot] ??
      Math.max(0, (s.params.dashboard?.groupInfo?.length ?? 0) - 1),
    indexPlot,
  }));
export const selectorMoveAndResortPlot = (s: Store) => s.moveAndResortPlot;
export const selectorDashboardLayoutEdit = (s: Store) => s.dashboardLayoutEdit;
export const selectorSetDashboardLayoutEdit = (s: Store) => s.setDashboardLayoutEdit;

export const selectorSetGroupName = (s: Store) => s.setGroupName;
export const selectorSetGroupShow = (s: Store) => s.setGroupShow;
export const selectorSetGroupSize = (s: Store) => s.setGroupSize;

export const selectorPlotList = (s: Store) =>
  selectorDashboardPlotList(s).filter((item) => s.params.dashboard?.groupInfo?.[item.group]?.show !== false);

export const selectorListMetricsGroup = (s: Store) => s.listMetricsGroup;
export const selectorSelectMetricsGroup = (s: Store) => s.selectMetricsGroup;
export const selectorLoadListMetricsGroup = (s: Store) => s.loadListMetricsGroup;
export const selectorSaveMetricsGroup = (s: Store) => s.saveMetricsGroup;
export const selectorRemoveMetricsGroup = (s: Store) => s.removeMetricsGroup;
export const selectorLoadMetricsGroup = (s: Store) => s.loadMetricsGroup;
export const selectorSetSelectMetricsGroup = (s: Store) => s.setSelectMetricsGroup;

export const selectorPromConfig = (s: Store) => s.promConfig;
export const selectorLoadPromConfig = (s: Store) => s.loadPromConfig;
export const selectorSavePromConfig = (s: Store) => s.savePromConfig;

export const selectorPromqltestfailed = (s: Store) => s.plotsData.map((d) => d.promqltestfailed).some(Boolean);

export const selectorEventsByIndex = (index: number, s: Store): EventData =>
  s.events[index] ?? { chunks: [], rows: [], what: [], range: new TimeRange(s.params.timeRange) };
export const selectorLoadEvents = (s: Store) => s.loadEvents;
export const selectorClearEvents = (s: Store) => s.clearEvents;

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type Store } from './store';
import * as api from '../view/api';
import { metricTagValueInfo } from '../view/api';
import { promQLMetric } from '../view/utils';
import { TimeRange } from '../common/TimeRange';
import { EventData } from './statshouse';
import { MetricMetaValue } from '../api/metric';

export const selectorParams = (s: Store) => s.params;
export const selectorDefaultParams = (s: Store) => s.defaultParams;
export const selectorParamsPlots = (s: Store) => s.params.plots;
export const selectorParamsPlotsByIndex = (index: number, s: Store) => s.params.plots[index];
export const selectorSetParamsPlots = (s: Store) => s.setPlotParams;
export const selectorRemovePlot = (s: Store) => s.removePlot;

export const selectorParamsTimeShifts = (s: Store) => s.params.timeShifts;
export const selectorParamsTabNum = (s: Store) => s.params.tabNum;
export const selectorTimeRange = (s: Store) => s.timeRange;
export const selectorSetTimeRange = (s: Store) => s.setTimeRange;
export const selectorSetParams = (s: Store) => s.setParams;

export const selectorLiveMode = (s: Store) => s.liveMode;
export const selectorSetLiveMode = (s: Store) => s.setLiveMode;

export const selectorDisabledLive = (s: Store) => !s.params.plots.every(({ useV2 }) => useV2);

export const selectorGlobalNumQueriesPlot = (s: Store) => s.globalNumQueriesPlot;

export const selectorNumQueriesPlotByIndex = (index: number, s: Store) => s.numQueriesPlot[index] ?? 0;

export const selectorPreviews = (s: Store) => s.previews;
export const selectorPreviewsByIndex = (index: number, s: Store) => s.previews[index];

export const selectorBaseRange = (s: Store) => s.baseRange;
export const selectorSetBaseRange = (s: Store) => s.setBaseRange;

export const selectorUPlotsWidthByIndex = (index: number, s: Store) => s.uPlotsWidth[index];

export const selectorPlotsData = (s: Store) => s.plotsData;
export const selectorPlotsDataByIndex = (index: number, s: Store) => s.plotsData[index] ?? {};

export const selectorMetricsMeta = (s: Store) => s.metricsMeta;
export const selectorMetricsMetaByName = (name: string, s: Store): MetricMetaValue | undefined => s.metricsMeta[name];

export const selectorParamsTagSync = (s: Store) => s.params.tagSync;

export const selectorTagsListByPlotAndTag = (indexPlot: number, indexTag: number, s: Store) =>
  s.tagsList[indexPlot]?.[indexTag] ?? [];
export const selectorTagsListByPlotAndTagAllSync = (
  indexPlot: number,
  indexTag: number,
  s: Store
): metricTagValueInfo[] => {
  const group = s.params.tagSync.find((group) => group[indexPlot] === indexTag) ?? [];
  const tagsListObj: Record<string, number> = {};
  group.forEach((tag, plot) => {
    if (tag !== null) {
      const nextList = s.tagsList[plot]?.[tag] ?? [];
      nextList.forEach(({ value, count }) => {
        tagsListObj[value] = (tagsListObj[value] ?? 0) + count;
      });
    }
  });
  return Object.entries(tagsListObj).map(([value, count]) => ({ value, count }));
};

export const selectorTagsListSKeyByPlot = (indexPlot: number, s: Store) => s.tagsListSKey[indexPlot] ?? [];
export const selectorTagsListAbortControllerByPlotAndTag = (indexPlot: number, indexTag: number, s: Store) =>
  s.tagsListLoading[indexPlot]?.[indexTag] ?? null;
export const selectorTagsSKeyListAbortControllerByPlot = (indexPlot: number, s: Store) =>
  s.tagsListSKeyLoading[indexPlot] ?? null;
export const selectorTagsListMoreByPlotAndTag = (indexPlot: number, indexTag: number, s: Store) =>
  s.tagsListMore[indexPlot]?.[indexTag] ?? false;
export const selectorTagsSKeyListMoreByPlot = (indexPlot: number, s: Store) => s.tagsListSKeyMore[indexPlot] ?? false;

export const selectorTitle = (s: Store) => {
  switch (s.params.tabNum) {
    case -1:
      if (s.params.dashboard?.dashboard_id !== undefined) {
        return `${s.params.dashboard?.name} — StatsHouse`;
      }
      return 'Dashboard — StatsHouse';
    case -2:
      return 'Dashboard setting — StatsHouse';
  }
  if (s.params.plots[s.params.tabNum] && s.params.plots[s.params.tabNum].metricName === promQLMetric) {
    if (!s.plotsData[s.params.tabNum].nameMetric) {
      return 'StatsHouse';
    }
    return (
      s.plotsData[s.params.tabNum].nameMetric +
      (s.plotsData[s.params.tabNum].whats.length
        ? ': ' + s.plotsData[s.params.tabNum].whats.map((qw) => api.whatToWhatDesc(qw)).join(',')
        : '') +
      ' — StatsHouse'
    );
  }
  return (
    (s.params.plots[s.params.tabNum] &&
      s.params.plots[s.params.tabNum].metricName !== '' &&
      `${s.params.plots[s.params.tabNum].metricName}: ${s.params.plots[s.params.tabNum].what
        .map((qw) => api.whatToWhatDesc(qw))
        .join(',')} — StatsHouse`) ||
    ''
  );
};

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

export const selectorThemeDark = (s: Store) => s.theme.dark;
export const selectorThemeName = (s: Store) => s.theme.theme;
export const selectorSetTheme = (s: Store) => s.theme.setTheme;

export const selectorPromqltestfailed = (s: Store) => s.plotsData.map((d) => d.promqltestfailed).some(Boolean);

export const selectorEventsByIndex = (index: number, s: Store): EventData =>
  s.events[index] ?? { chunks: [], rows: [], what: [], range: new TimeRange(s.params.timeRange) };
export const selectorLoadEvents = (s: Store) => s.loadEvents;
export const selectorClearEvents = (s: Store) => s.clearEvents;

// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type Store } from './statshouse';
import * as api from '../view/api';
import { metricTagValueInfo } from '../view/api';

export const selectorAll = (s: Store) => s;

export const selectorParams = (s: Store) => s.params;
export const selectorParamsPlots = (s: Store) => s.params.plots;
export const selectorParamsPlotsByIndex = (index: number, s: Store) => s.params.plots[index];
export const selectorSetParamsPlots = (s: Store) => s.setPlotParams;
export const selectorRemovePlot = (s: Store) => s.removePlot;
export const selectorParamsTimeRange = (s: Store) => s.params.timeRange;
export const selectorParamsTimeShifts = (s: Store) => s.params.timeShifts;
export const selectorParamsTabNum = (s: Store) => s.params.tabNum;
export const selectorTimeRange = (s: Store) => s.timeRange;
export const selectorSetTimeRange = (s: Store) => s.setTimeRange;
export const selectorUpdateParamsByUrl = (s: Store) => s.updateParamsByUrl;
export const selectorInitSetSearchParams = (s: Store) => s.initSetSearchParams;
export const selectorSetParams = (s: Store) => s.setParams;
export const selectorPlotActive = (s: Store) => s.params.plots[Math.max(0, s.params.tabNum)];

export const selectorLiveMode = (s: Store) => s.liveMode;
export const selectorSetLiveMode = (s: Store) => s.setLiveMode;

export const selectorDisabledLive = (s: Store) => !s.params.plots.every(({ useV2 }) => useV2);

export const selectorGlobalNumQueriesPlot = (s: Store) => s.globalNumQueriesPlot;
export const selectorSetGlobalNumQueriesPlot = (s: Store) => s.setGlobalNumQueriesPlot;

export const selectorNumQueriesPlot = (s: Store) => s.numQueriesPlot;
export const selectorNumQueriesPlotByIndex = (index: number, s: Store) => s.numQueriesPlot[index] ?? 0;
export const selectorSetNumQueriesPlot = (s: Store) => s.setNumQueriesPlot;

export const selectorPreviews = (s: Store) => s.previews;
export const selectorPreviewsByIndex = (index: number, s: Store) => s.previews[index];
export const selectorSetPreviews = (s: Store) => s.setPreviews;

export const selectorBaseRange = (s: Store) => s.baseRange;
export const selectorSetBaseRange = (s: Store) => s.setBaseRange;

export const selectorLastError = (s: Store) => s.lastError;
export const selectorSetLastError = (s: Store) => s.setLastError;

export const selectorCompact = (s: Store) => s.compact;
export const selectorSetCompact = (s: Store) => s.setCompact;

export const selectorUPlotsWidth = (s: Store) => s.uPlotsWidth;
export const selectorUPlotsWidthByIndex = (index: number, s: Store) => s.uPlotsWidth[index];
export const selectorSetUPlotWidth = (s: Store) => s.setUPlotWidth;

export const selectorMetricsList = (s: Store) => s.metricsList;
export const selectorLoadMetricsList = (s: Store) => s.loadMetricsList;

export const selectorPlotsData = (s: Store) => s.plotsData;
export const selectorSetPlotShow = (s: Store) => s.setPlotShow;
export const selectorPlotsDataByIndex = (index: number, s: Store) => s.plotsData[index] ?? {};
export const selectorPlotLastError = (s: Store) => s.setPlotLastError;
export const selectorSetYLockChange = (s: Store) => s.setYLockChange;

export const selectorMetricsMeta = (s: Store) => s.metricsMeta;
export const selectorMetricsMetaByName = (name: string, s: Store) => s.metricsMeta[name] ?? s.metricsMeta[''];
export const selectorLoadMetricsMeta = (s: Store) => s.loadMetricsMeta;
export const selectorClearMetricsMeta = (s: Store) => s.clearMetricsMeta;

export const selectorParamsTagSync = (s: Store) => s.params.tagSync;
export const selectorSetTagSync = (s: Store) => s.setTagSync;
export const selectorSetPlotParamsTag = (s: Store) => s.setPlotParamsTag;
export const selectorSetPlotParamsTagGroupBy = (s: Store) => s.setPlotParamsTagGroupBy;

export const selectorTagsList = (s: Store) => s.tagsList;
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

export const selectorTagsListSKey = (s: Store) => s.tagsListSKey;
export const selectorTagsListSKeyByPlot = (indexPlot: number, s: Store) => s.tagsListSKey[indexPlot] ?? [];
export const selectorTagsListAbortControllerByPlotAndTag = (indexPlot: number, indexTag: number, s: Store) =>
  s.tagsListAbortController[indexPlot]?.[indexTag] ?? null;
export const selectorTagsSKeyListAbortControllerByPlot = (indexPlot: number, s: Store) =>
  s.tagsListSKeyAbortController[indexPlot] ?? null;
export const selectorTagsListMoreByPlotAndTag = (indexPlot: number, indexTag: number, s: Store) =>
  s.tagsListMore[indexPlot]?.[indexTag] ?? false;
export const selectorTagsSKeyListMoreByPlot = (indexPlot: number, s: Store) => s.tagsListSKeyMore[indexPlot] ?? false;
export const selectorTagsListAbortController = (s: Store) => s.tagsListAbortController;
export const selectorTagsListSKeyAbortController = (s: Store) => s.tagsListSKeyAbortController;
export const selectorSetTagsList = (s: Store) => s.setTagsList;
export const selectorLoadTagsList = (s: Store) => s.loadTagsList;
export const selectorPreSync = (s: Store) => s.preSync;

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

export const selectorListServerDashboard = (s: Store) => s.listServerDashboard;
export const selectorLoadListServerDashboard = (s: Store) => s.loadListServerDashboard;

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

export const selectorPlotList = (s: Store) => {
  if (selectorIsServer(s)) {
    return selectorDashboardPlotList(s).filter((item) => s.params.dashboard?.groupInfo?.[item.group]?.show !== false);
  }
  return selectorDashboardPlotList(s);
};

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

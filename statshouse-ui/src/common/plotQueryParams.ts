// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { dequal } from 'dequal/lite';
import {
  intListFromParams,
  intListToParams,
  join,
  numberFromParams,
  numberToParams,
  stringFromParams,
  stringListFromParams,
  stringListToParams,
  stringToParams,
} from './url';
import {
  filterInSep,
  filterNotInSep,
  flattenFilterIn,
  flattenFilterNotIn,
  queryDashboardID,
  queryParamAgg,
  queryParamBackendVersion,
  queryParamFilter,
  queryParamFilterSync,
  queryParamFromTime,
  queryParamGroupBy,
  queryParamLockMax,
  queryParamLockMin,
  queryParamMetric,
  queryParamNumResults,
  queryParamTabNum,
  queryParamTimeShifts,
  queryParamToTime,
  queryParamWhat,
  queryValueBackendVersion2,
  queryWhat,
  tabPrefix,
  v2Value,
} from '../view/api';
import { defaultTimeRange, KeysTo } from './TimeRange';
import { timeRangeToFromParams, timeRangeToToParams } from '../hooks';

export type PlotParams = {
  metricName: string;
  what: queryWhat[];
  customAgg: number;
  groupBy: string[];
  filterIn: Record<string, string[]>;
  filterNotIn: Record<string, string[]>;
  numSeries: number;
  timeShifts: number[];
  useV2: boolean;
  yLock: {
    min: number;
    max: number;
  };
};

export type GroupInfo = {
  name: string;
  show: boolean;
};

export type DashboardParams = {
  dashboard_id?: number;
  name: string;
  description: string;
  version?: number;
  groups?: (number | null)[]; // [page_plot]
  groupInfo?: GroupInfo[];
};

export type QueryParams = {
  ['@type']?: 'QueryParams'; // ld-json
  dashboard?: DashboardParams;
  timeRange: { to: number | KeysTo; from: number };
  tabNum: number;
  tagSync: (number | null)[][]; // [group][page_plot][tag_index]
  plots: PlotParams[];
};

export const defaultParams: Readonly<QueryParams> = {
  timeRange: { ...defaultTimeRange },
  tabNum: 0,
  tagSync: [],
  plots: [
    {
      metricName: '',
      what: ['count_norm'],
      customAgg: 0,
      groupBy: [],
      filterIn: {},
      filterNotIn: {},
      numSeries: 5,
      timeShifts: [],
      useV2: true,
      yLock: {
        min: 0,
        max: 0,
      },
    },
  ],
};

export function parseFilter(filter: readonly string[]) {
  const filterIn: Record<string, string[]> = {};
  const filterNotIn: Record<string, string[]> = {};
  filter.forEach((s) => {
    const inIx = s.indexOf(filterInSep);
    const notInIx = s.indexOf(filterNotInSep);
    if (inIx === -1 && notInIx === -1) {
      // do nothing, invalid value
    } else if (inIx !== -1 && (notInIx === -1 || inIx < notInIx)) {
      const indexTag = s.substring(0, inIx).replace('skey', '_s').replace('key', '');
      const tagID = '_s' === indexTag ? 'skey' : `key${indexTag}`;
      const tagValue = s.substring(inIx + 1);
      filterIn[tagID] = [...(filterIn[tagID] || []), tagValue];
    } else {
      const indexTag = s.substring(0, notInIx).replace('skey', '_s').replace('key', '');
      const tagID = '_s' === indexTag ? 'skey' : `key${indexTag}`;
      const tagValue = s.substring(notInIx + 1);
      filterNotIn[tagID] = [...(filterNotIn[tagID] || []), tagValue];
    }
  });
  return { filterIn, filterNotIn };
}

export function readPlotParams(
  prevPlot: PlotParams,
  params: URLSearchParams,
  defaultParams: Readonly<PlotParams>,
  prefix: string = ''
): PlotParams {
  const defaultFilterIn = flattenFilterIn(defaultParams.filterIn);
  const defaultFilterNotIn = flattenFilterNotIn(defaultParams.filterNotIn);
  const filter = stringListFromParams(params, prefix, queryParamFilter, [...defaultFilterIn, ...defaultFilterNotIn]);
  const { filterIn, filterNotIn } = parseFilter(filter);

  const plot: PlotParams = {
    metricName: stringFromParams(params, prefix, queryParamMetric, defaultParams.metricName),
    what: stringListFromParams(params, prefix, queryParamWhat, defaultParams.what) as queryWhat[],
    customAgg: numberFromParams(params, prefix, queryParamAgg, defaultParams.customAgg, false),
    groupBy: stringListFromParams(params, prefix, queryParamGroupBy, defaultParams.groupBy) as string[],
    filterIn,
    filterNotIn,
    numSeries: numberFromParams(params, prefix, queryParamNumResults, defaultParams.numSeries, false),
    timeShifts: intListFromParams(params, prefix, queryParamTimeShifts, defaultParams.timeShifts) as number[],
    useV2:
      stringFromParams(params, prefix, queryParamBackendVersion, v2Value(defaultParams.useV2)) ===
      queryValueBackendVersion2,
    yLock: {
      min: numberFromParams(params, prefix, queryParamLockMin, defaultParams.yLock.min, false),
      max: numberFromParams(params, prefix, queryParamLockMax, defaultParams.yLock.max, false),
    },
  };
  if (!prevPlot) {
    return plot;
  }

  if (dequal(plot, prevPlot)) {
    return prevPlot;
  }

  if (dequal(plot.what, prevPlot.what)) {
    plot.what = prevPlot.what;
  }

  if (dequal(plot.groupBy, prevPlot.groupBy)) {
    plot.groupBy = prevPlot.groupBy;
  }

  if (dequal(plot.filterIn, prevPlot.filterIn)) {
    plot.filterIn = prevPlot.filterIn;
  }

  if (dequal(plot.filterNotIn, prevPlot.filterNotIn)) {
    plot.filterNotIn = prevPlot.filterNotIn;
  }

  if (dequal(plot.timeShifts, prevPlot.timeShifts)) {
    plot.timeShifts = prevPlot.timeShifts;
  }

  if (dequal(plot.yLock, prevPlot.yLock)) {
    plot.yLock = prevPlot.yLock;
  }
  return plot;
}

export function readParams(
  prevState: QueryParams,
  params: URLSearchParams,
  defaultParams: Readonly<QueryParams>
): QueryParams {
  const tabs: string[] = [];
  params.forEach((value, key) => {
    const [prefix, realKey] = key.split('.', 2);
    if (prefix === queryParamMetric && !realKey) {
      tabs.push('');
    } else if (realKey === queryParamMetric) {
      tabs.push(prefix);
    }
  });
  let plots = tabs.map((prefix, index) =>
    readPlotParams(prevState.plots[index], params, defaultParams.plots[0], prefix)
  );
  const timeRange = {
    from: numberFromParams(params, '', queryParamFromTime, defaultParams.timeRange.from, false),
    to: timeRangeToFromParams(params, '', queryParamToTime, defaultParams.timeRange.to),
  };
  const tagSync = stringListFromParams(params, '', queryParamFilterSync, []).map((s) =>
    s.split('-').reduce((res, t) => {
      const [plot, tagKey] = t.split('.').map((r) => parseInt(r));
      res[plot] = tagKey;
      return res;
    }, new Array(plots.length).fill(null) as (number | null)[])
  );
  if (dequal(plots, prevState.plots) && prevState.tagSync.length > tagSync.length) {
    for (let i = 0, maxI = prevState.tagSync.length - tagSync.length; i < maxI; i++) {
      tagSync.push([]);
    }
  }

  return {
    timeRange: dequal(timeRange, prevState.timeRange) ? prevState.timeRange : timeRange,
    tabNum: numberFromParams(params, '', queryParamTabNum, defaultParams.tabNum, false),
    tagSync: dequal(tagSync, prevState.tagSync) ? prevState.tagSync : tagSync,
    plots: dequal(plots, prevState.plots) ? prevState.plots : plots,
  };
}

export function writePlotParams(
  value: PlotParams,
  params: URLSearchParams,
  defaultParams: Readonly<PlotParams>,
  prefix: string = ''
): URLSearchParams {
  stringToParams(value.metricName, params, prefix, queryParamMetric, defaultParams.metricName);
  stringListToParams(value.what, params, prefix, queryParamWhat, defaultParams.what);
  numberToParams(value.customAgg, params, prefix, queryParamAgg, defaultParams.customAgg);
  stringListToParams(value.groupBy, params, prefix, queryParamGroupBy, defaultParams.groupBy);
  const filterIn = flattenFilterIn(value.filterIn);
  const filterNotIn = flattenFilterNotIn(value.filterNotIn);
  const defaultFilterIn = flattenFilterIn(defaultParams.filterIn);
  const defaultFilterNotIn = flattenFilterNotIn(defaultParams.filterNotIn);
  stringListToParams([...filterIn, ...filterNotIn], params, prefix, queryParamFilter, [
    ...defaultFilterIn,
    ...defaultFilterNotIn,
  ]);
  numberToParams(value.numSeries, params, prefix, queryParamNumResults, defaultParams.numSeries);
  intListToParams(value.timeShifts, params, prefix, queryParamTimeShifts, defaultParams.timeShifts);
  stringToParams(v2Value(value.useV2), params, prefix, queryParamBackendVersion, v2Value(defaultParams.useV2));

  numberToParams(value.yLock.min, params, prefix, queryParamLockMin, defaultParams.yLock.min);
  numberToParams(value.yLock.max, params, prefix, queryParamLockMax, defaultParams.yLock.max);

  return params;
}

export function removePlotParams(params: URLSearchParams, prefix: string = ''): URLSearchParams {
  params.delete(join(prefix, queryParamMetric));
  params.delete(join(prefix, queryParamWhat));
  params.delete(join(prefix, queryParamAgg));
  params.delete(join(prefix, queryParamGroupBy));
  params.delete(join(prefix, queryParamFilter));
  params.delete(join(prefix, queryParamNumResults));
  params.delete(join(prefix, queryParamTimeShifts));
  params.delete(join(prefix, queryParamBackendVersion));
  params.delete(join(prefix, queryParamLockMax));
  params.delete(join(prefix, queryParamLockMin));
  return params;
}

export function writeParams(
  value: QueryParams,
  params: URLSearchParams,
  defaultParams: Readonly<QueryParams>
): URLSearchParams {
  numberToParams(value.tabNum, params, '', queryParamTabNum, defaultParams.tabNum);
  const tabs: string[] = [];
  params.forEach((value, key) => {
    const [prefix, realKey] = key.split('.', 2);
    if (prefix === queryParamMetric && !realKey) {
      tabs.push('');
    } else if (realKey === queryParamMetric) {
      tabs.push(prefix);
    }
  });
  tabs.forEach((prefix) => {
    removePlotParams(params, prefix);
  });

  numberToParams(value.timeRange.from, params, '', queryParamFromTime, defaultParams.timeRange.from);
  timeRangeToToParams(value.timeRange.to, params, '', queryParamToTime, defaultParams.timeRange.to);
  stringListToParams(
    value.tagSync
      .map((t) =>
        t
          .map((key, index) => (Number.isInteger(key) ? `${index}.${key}` : undefined))
          .filter((s) => s)
          .join('-')
      )
      .filter(Boolean),
    params,
    '',
    queryParamFilterSync,
    [],
    false
  );
  for (let i = 0, m = value.plots.length; i < m; i++) {
    if (value.plots[i]) {
      writePlotParams(value.plots[i], params, defaultParams.plots[0], i !== 0 ? `${tabPrefix}${i}` : '');
    }
  }
  return params;
}

export function parseParamsFromUrl(url: string): QueryParams {
  const params = new URLSearchParams(url);
  return readParams(defaultParams, params, defaultParams);
}

export function getUrlSearch(
  value: QueryParams | ((prevState: QueryParams) => QueryParams),
  prev?: QueryParams,
  baseLink?: string
) {
  const params = new URLSearchParams(baseLink ?? document.location.search);
  const prevState = prev ?? readParams(defaultParams, params, defaultParams);
  const newState = typeof value === 'function' ? (value as (prevState: QueryParams) => QueryParams)(prevState) : value;
  const newSearchParams = writeParams(newState, params, defaultParams);
  return '?' + newSearchParams.toString();
}

export function sortEntity<T extends number | string>(arr: T[]): T[] {
  return [...arr].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
}

export function readDashboardID(params: URLSearchParams): number {
  return numberFromParams(params, '', queryDashboardID, 0, false);
}

export function writeDashboard(
  value: QueryParams,
  params: URLSearchParams,
  defaultParams: Readonly<QueryParams>
): URLSearchParams {
  if (value.dashboard?.dashboard_id) {
    numberToParams(value.dashboard.dashboard_id, params, '', queryDashboardID, 0);
    numberToParams(value.tabNum, params, '', queryParamTabNum, defaultParams.tabNum);
    numberToParams(value.timeRange.from, params, '', queryParamFromTime, defaultParams.timeRange.from);
    timeRangeToToParams(value.timeRange.to, params, '', queryParamToTime, defaultParams.timeRange.to);
  }
  return params;
}

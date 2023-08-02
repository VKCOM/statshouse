// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_PARAMS, METRIC_VALUE_BACKEND_VERSION, QUERY_WHAT, QueryWhat, TAG_KEY, TagKey } from '../api/enum';
import { KeysTo, stringToTime, TIME_RANGE_KEYS_TO } from '../common/TimeRange';
import { dequal } from 'dequal/lite';
import { deepClone, isNotNil, toNumber } from '../common/helpers';
import { globalSettings } from '../common/settings';

export const filterInSep = '-';
export const filterNotInSep = '~';

export const maxPrefixArray = 1000;
export const removeValueChar = String.fromCharCode(7);

export const toGroupInfoPrefix = (i: number) => `${GET_PARAMS.dashboardGroupInfoPrefix}${i}.`;
export const toPlotPrefix = (i: number) => (i ? `${GET_PARAMS.plotPrefix}${i}.` : '');
export const toVariablePrefix = (i: number) => `${GET_PARAMS.variablePrefix}${i}.`;

export const parseTagSync = (s?: string) => {
  if (s == null) {
    return null;
  }
  return [
    ...s.split('-').reduce((res, t) => {
      const [plot, tagKey] = t.split('.').map((r) => parseInt(r));
      res[plot] = tagKey;
      return res;
    }, [] as (number | null)[]),
  ].map((s) => s ?? null);
};

export function isNotNilVariableLink(link: (number | null)[]): link is [number, number] {
  return link[0] != null && link[1] != null;
}

export const QueryWhatValues: Set<string> = new Set(Object.values(QUERY_WHAT));
export function isQueryWhat(s: string): s is QueryWhat {
  return QueryWhatValues.has(s);
}

export const TagKeyValues: Set<string> = new Set(Object.values(TAG_KEY));
export function isTagKey(s: string): s is TagKey {
  return TagKeyValues.has(s);
}

export interface lockRange {
  readonly min: number;
  readonly max: number;
}

export const PLOT_TYPE = {
  Metric: 0,
  Event: 1,
} as const;
export type PlotType = (typeof PLOT_TYPE)[keyof typeof PLOT_TYPE];
export type PlotParams = {
  metricName: string;
  customName: string;
  what: QueryWhat[];
  customAgg: number;
  groupBy: string[];
  filterIn: Record<string, string[]>;
  filterNotIn: Record<string, string[]>;
  numSeries: number;
  useV2: boolean;
  yLock: {
    min: number;
    max: number;
  };
  maxHost: boolean;
  promQL: string;
  type: PlotType;
  events: number[];
  eventsBy: string[];
  eventsHide: string[];
};
export type GroupInfo = {
  name: string;
  show: boolean;
  count: number;
  size: number;
};
export type DashboardParams = {
  dashboard_id?: number;
  name?: string;
  description?: string;
  version?: number;
  /**
   * @deprecated not use
   */
  groups?: (number | null)[]; // [page_plot]
  groupInfo?: GroupInfo[];
};
export type VariableParams = {
  name: string;
  description: string;
  values: string[];
  link: [number | null, number | null][];
  args: {
    groupBy?: boolean;
    negative?: boolean;
  };
  // source: string;
};
export type QueryParams = {
  ['@type']?: 'QueryParams'; // ld-json
  dashboard?: DashboardParams;
  timeRange: { to: number | KeysTo; from: number };
  eventFrom: number;
  timeShifts: number[];
  tabNum: number;
  tagSync: (number | null)[][]; // [group][page_plot][tag_index]
  plots: PlotParams[];
  variables: VariableParams[];
};
export const PlotTypeValues: Set<number> = new Set(Object.values(PLOT_TYPE));
export function isPlotType(s: number): s is PlotType {
  return PlotTypeValues.has(s);
}

export function freeKeyPrefix(str: string): string {
  return str.replace('skey', '_s').replace('key', '');
}

export function toIndexTag(str: string): number | null {
  const shortKey = freeKeyPrefix(str);
  return shortKey === '_s' ? -1 : toNumber(shortKey);
}

export function toKeyTag(indexTag: number, full?: boolean): string {
  if (full) {
    return indexTag === -1 ? 'skey' : `key${indexTag}`;
  }
  return indexTag === -1 ? '_s' : indexTag.toString();
}

export function getNewPlot(): PlotParams {
  return {
    metricName: globalSettings.default_metric,
    customName: '',
    promQL: '',
    what: globalSettings.default_metric_what.slice(),
    customAgg: 0,
    groupBy: globalSettings.default_metric_group_by.slice(),
    filterIn: deepClone(globalSettings.default_metric_filter_in),
    filterNotIn: deepClone(globalSettings.default_metric_filter_not_in),
    numSeries: globalSettings.default_num_series,
    useV2: true,
    yLock: {
      min: 0,
      max: 0,
    },
    maxHost: false,
    type: PLOT_TYPE.Metric,
    events: [],
    eventsBy: [],
    eventsHide: [],
  };
}

export function toPlotType(s: unknown): PlotType | null {
  const t = toNumber(s);
  if (t && isPlotType(t)) {
    return t;
  }
  return null;
}

export function encodeParams(value: QueryParams, defaultParams?: QueryParams) {
  const search: string[][] = [];

  // tabNum
  if (value.tabNum !== (defaultParams?.tabNum ?? 0)) {
    search.push([GET_PARAMS.metricTabNum, value.tabNum.toString()]);
  }

  // dashboard
  if (value.dashboard?.dashboard_id) {
    search.push([GET_PARAMS.dashboardID, value.dashboard.dashboard_id.toString()]);
  }

  // groupInfo
  if (value.dashboard?.groupInfo?.length && !dequal(value.dashboard?.groupInfo, defaultParams?.dashboard?.groupInfo)) {
    value.dashboard.groupInfo.forEach(({ name, show, count, size }, indexGroup) => {
      const prefix = toGroupInfoPrefix(indexGroup);
      search.push([prefix + GET_PARAMS.dashboardGroupInfoName, name]);
      if (!show) {
        search.push([prefix + GET_PARAMS.dashboardGroupInfoShow, '0']);
      }
      if (count) {
        search.push([prefix + GET_PARAMS.dashboardGroupInfoCount, count.toString()]);
      }
      if (size !== 2) {
        search.push([prefix + GET_PARAMS.dashboardGroupInfoSize, size.toString()]);
      }
    });
  } else if (!value.dashboard?.groupInfo?.length && defaultParams?.dashboard?.groupInfo?.length) {
    search.push([toGroupInfoPrefix(0) + GET_PARAMS.dashboardGroupInfoName, removeValueChar]);
  }

  // timeRange to
  if (value.timeRange.to !== (defaultParams?.timeRange.to ?? TIME_RANGE_KEYS_TO.default)) {
    search.push([GET_PARAMS.toTime, value.timeRange.to.toString()]);
  }
  // timeRange from
  if (value.timeRange.from !== (defaultParams?.timeRange.from ?? 0)) {
    search.push([GET_PARAMS.fromTime, value.timeRange.from.toString()]);
  }

  // eventFrom
  if (value.eventFrom) {
    search.push([GET_PARAMS.metricEventFrom, value.eventFrom.toString()]);
  }

  // plots
  if (value.plots.length && !dequal(value.plots, defaultParams?.plots)) {
    value.plots.forEach((plot, indexPlot) => {
      const prefix = toPlotPrefix(indexPlot);

      search.push([prefix + GET_PARAMS.metricName, plot.metricName]);

      if (plot.customName) {
        search.push([prefix + GET_PARAMS.metricCustomName, plot.customName]);
      }

      if (!(plot.what.length === 1 && plot.what[0] === 'count_norm') && plot.what.length) {
        plot.what.forEach((w) => {
          search.push([prefix + GET_PARAMS.metricWhat, w]);
        });
      }

      if (plot.customAgg) {
        search.push([prefix + GET_PARAMS.metricAgg, plot.customAgg.toString()]);
      }

      if (plot.groupBy.length) {
        plot.groupBy.forEach((g) => {
          search.push([prefix + GET_PARAMS.metricGroupBy, freeKeyPrefix(g)]);
        });
      }

      const entriesFilterIn = Object.entries(plot.filterIn);
      if (entriesFilterIn.length) {
        entriesFilterIn.forEach(([keyTag, valuesTag]) => {
          valuesTag.forEach((valueTag) => {
            search.push([prefix + GET_PARAMS.metricFilter, freeKeyPrefix(keyTag) + filterInSep + valueTag]);
          });
        });
      }

      const entriesFilterNotIn = Object.entries(plot.filterNotIn);
      if (entriesFilterNotIn.length) {
        entriesFilterNotIn.forEach(([keyTag, valuesTag]) => {
          valuesTag.forEach((valueTag) => {
            search.push([prefix + GET_PARAMS.metricFilter, freeKeyPrefix(keyTag) + filterNotInSep + valueTag]);
          });
        });
      }

      if (plot.numSeries !== 5) {
        search.push([prefix + GET_PARAMS.numResults, plot.numSeries.toString()]);
      }

      if (!plot.useV2) {
        search.push([
          prefix + GET_PARAMS.version,
          plot.useV2 ? METRIC_VALUE_BACKEND_VERSION.v2 : METRIC_VALUE_BACKEND_VERSION.v1,
        ]);
      }

      if (plot.yLock.min) {
        search.push([prefix + GET_PARAMS.metricLockMin, plot.yLock.min.toString()]);
      }

      if (plot.yLock.max) {
        search.push([prefix + GET_PARAMS.metricLockMax, plot.yLock.max.toString()]);
      }

      if (plot.maxHost) {
        search.push([prefix + GET_PARAMS.metricMaxHost, '1']);
      }

      if (plot.promQL) {
        search.push([prefix + GET_PARAMS.metricPromQL, plot.promQL]);
      }

      if (plot.type !== PLOT_TYPE.Metric) {
        search.push([prefix + GET_PARAMS.metricType, plot.type.toString()]);
      }

      if (plot.events.length) {
        plot.events.forEach((event) => {
          search.push([prefix + GET_PARAMS.metricEvent, event.toString()]);
        });
      }

      if (plot.eventsBy.length) {
        plot.eventsBy.forEach((eventBy) => {
          search.push([prefix + GET_PARAMS.metricEventBy, eventBy]);
        });
      }

      if (plot.eventsHide.length) {
        plot.eventsHide.forEach((eventHide) => {
          search.push([prefix + GET_PARAMS.metricEventHide, eventHide]);
        });
      }
    });
  } else if (!value.plots.length && defaultParams?.plots.length) {
    search.push([toPlotPrefix(0) + GET_PARAMS.metricName, removeValueChar]);
  }

  // timeShifts
  if (value.timeShifts.length && !dequal(value.timeShifts, defaultParams?.timeShifts)) {
    value.timeShifts.forEach((timeShift) => {
      search.push([GET_PARAMS.metricTimeShifts, timeShift.toString()]);
    });
  } else if (!value.timeShifts.length && defaultParams?.timeShifts.length) {
    search.push([GET_PARAMS.metricTimeShifts, removeValueChar]);
  }

  // tagSync
  if (value.tagSync.length && !dequal(value.tagSync, defaultParams?.tagSync)) {
    value.tagSync.forEach((s) => {
      search.push([
        GET_PARAMS.metricFilterSync,
        s
          .map((key, index) => (Number.isInteger(key) ? `${index}.${key}` : null))
          .filter((s) => s)
          .join('-'),
      ]);
    });
  } else if (!value.tagSync.length && defaultParams?.tagSync.length) {
    search.push([GET_PARAMS.metricFilterSync, removeValueChar]);
  }

  // variables
  if (value.variables.length && !dequal(value.variables, defaultParams?.variables)) {
    value.variables.forEach((variable, indexVariable) => {
      const prefix = toVariablePrefix(indexVariable);
      const variableName = `${GET_PARAMS.variableValuePrefix}${variable.name}`;

      search.push([prefix + GET_PARAMS.variableName, variable.name]);

      if (variable.description) {
        search.push([prefix + GET_PARAMS.variableDescription, variable.description]);
      }

      if (variable.link.length) {
        search.push([
          prefix + GET_PARAMS.variableLinkPlot,
          variable.link
            .filter(isNotNilVariableLink)
            .map(([p, t]) => `${p}.${toKeyTag(t)}`)
            .join('-'),
        ]);
      }

      if (variable.values.length) {
        variable.values.forEach((value) => {
          search.push([variableName, value]);
        });
      }

      if (variable.args.groupBy) {
        search.push([`${variableName}.${GET_PARAMS.variableGroupBy}`, '1']);
      }

      if (variable.args.negative) {
        search.push([`${variableName}.${GET_PARAMS.variableNegative}`, '1']);
      }
    });
  } else if (!value.variables.length && defaultParams?.variables.length) {
    search.push([toVariablePrefix(0) + GET_PARAMS.variableName, removeValueChar]);
  }

  return new URLSearchParams(search);
}

export function decodeParams(urlSearchParams: URLSearchParams, defaultParams?: QueryParams): QueryParams {
  const urlParams = [...urlSearchParams.entries()].reduce((res, [key, value]) => {
    res[key] ??= [];
    res[key].push(value);
    return res;
  }, {} as Record<string, string[]>);
  const tabNum = toNumber(urlParams[GET_PARAMS.metricTabNum]?.[0]) ?? defaultParams?.tabNum ?? 0;

  let dashboard: DashboardParams | undefined = undefined;

  const dashboard_id =
    toNumber(urlParams[GET_PARAMS.dashboardID]?.[0]) ?? defaultParams?.dashboard?.dashboard_id ?? undefined;

  if (dashboard_id != null) {
    const groupInfo: GroupInfo[] = [];
    dashboard = {
      dashboard_id,
      name: defaultParams?.dashboard?.name ?? '',
      description: defaultParams?.dashboard?.description ?? '',
      groupInfo,
      version: defaultParams?.dashboard?.version,
    };
    for (let i = 0; i < maxPrefixArray; i++) {
      const prefix = toGroupInfoPrefix(i);

      const name = urlParams[prefix + GET_PARAMS.dashboardGroupInfoName]?.[0];

      if (name == null) {
        if (defaultParams?.dashboard?.groupInfo?.length && i === 0) {
          groupInfo.push(...defaultParams.dashboard.groupInfo.map((g) => ({ ...g })));
        }
        break;
      }
      if (name === removeValueChar) {
        break;
      }
      const show = urlParams[prefix + GET_PARAMS.dashboardGroupInfoShow]?.[0] !== '0';
      const count = toNumber(urlParams[prefix + GET_PARAMS.dashboardGroupInfoCount]?.[0]) ?? 0;
      const size = toNumber(urlParams[prefix + GET_PARAMS.dashboardGroupInfoSize]?.[0]) ?? 2;
      groupInfo.push({ name, show, count, size });
    }
  }

  const timeRangeTo =
    stringToTime(urlParams[GET_PARAMS.toTime]?.[0]) ?? defaultParams?.timeRange.to ?? TIME_RANGE_KEYS_TO.default;

  const timeRangeFrom = toNumber(urlParams[GET_PARAMS.fromTime]?.[0]) ?? defaultParams?.timeRange.from ?? 0;

  const eventFrom = toNumber(urlParams[GET_PARAMS.metricEventFrom]?.[0]) ?? defaultParams?.eventFrom ?? 0;

  const plots: PlotParams[] = [];

  for (let i = 0; i < maxPrefixArray; i++) {
    const prefix = toPlotPrefix(i);

    const metricName = urlParams[prefix + GET_PARAMS.metricName]?.[0];
    if (metricName == null) {
      if (defaultParams?.plots.length && i === 0) {
        plots.push(...deepClone(defaultParams.plots));
      }
      break;
    }
    if (metricName === removeValueChar) {
      break;
    }
    const customName = urlParams[prefix + GET_PARAMS.metricCustomName]?.[0] ?? '';
    const what: QueryWhat[] = urlParams[prefix + GET_PARAMS.metricWhat]?.filter(isQueryWhat) ?? ['count_norm'];
    const customAgg = toNumber(urlParams[prefix + GET_PARAMS.metricAgg]?.[0]) ?? 0;
    const groupBy: string[] =
      urlParams[prefix + GET_PARAMS.metricGroupBy].map((s) => {
        const indexTag = freeKeyPrefix(s);
        return '_s' === indexTag ? 'skey' : `key${indexTag}`;
      }) ?? [];

    const filterIn: Record<string, string[]> = {};
    const filterNotIn: Record<string, string[]> = {};
    urlParams[prefix + GET_PARAMS.metricFilter]?.forEach((s) => {
      const pos = s.indexOf(filterInSep);
      const pos2 = s.indexOf(filterNotInSep);
      if (pos2 === -1 || (pos2 > pos && pos > -1)) {
        const indexTag = freeKeyPrefix(s.substring(0, pos));
        const tagID = '_s' === indexTag ? 'skey' : `key${indexTag}`;
        const tagValue = s.substring(pos + 1);
        filterIn[tagID] ??= [];
        filterIn[tagID].push(tagValue);
      } else if (pos === -1 || (pos > pos2 && pos2 > -1)) {
        const indexTag = freeKeyPrefix(s.substring(0, pos2));
        const tagID = '_s' === indexTag ? 'skey' : `key${indexTag}`;
        const tagValue = s.substring(pos2 + 1);
        filterNotIn[tagID] ??= [];
        filterNotIn[tagID].push(tagValue);
      }
    });

    const numSeries = toNumber(urlParams[prefix + GET_PARAMS.numResults]?.[0]) ?? 5;

    const useV2 = urlParams[prefix + GET_PARAMS.version]?.[0] !== METRIC_VALUE_BACKEND_VERSION.v1;

    const yLockMin = toNumber(urlParams[prefix + GET_PARAMS.metricLockMin]?.[0]) ?? 0;
    const yLockMax = toNumber(urlParams[prefix + GET_PARAMS.metricLockMax]?.[0]) ?? 0;

    const maxHost = urlParams[prefix + GET_PARAMS.metricMaxHost]?.[0] === '1';

    const promQL = urlParams[prefix + GET_PARAMS.metricPromQL]?.[0] ?? '';

    const type = toPlotType(urlParams[prefix + GET_PARAMS.metricType]?.[0]) ?? PLOT_TYPE.Metric;

    const events = urlParams[prefix + GET_PARAMS.metricEvent]?.map(toNumber).filter(isNotNil) ?? [];

    const eventsBy = urlParams[prefix + GET_PARAMS.metricEventBy] ?? [];

    const eventsHide = urlParams[prefix + GET_PARAMS.metricEventHide] ?? [];

    plots.push({
      metricName,
      customName,
      what,
      customAgg,
      groupBy,
      filterIn,
      filterNotIn,
      numSeries,
      useV2,
      yLock: {
        min: yLockMin,
        max: yLockMax,
      },
      maxHost,
      promQL,
      type,
      events,
      eventsBy,
      eventsHide,
    });
  }

  const timeShifts =
    (urlParams[GET_PARAMS.metricTimeShifts]?.[0] === removeValueChar
      ? []
      : urlParams[GET_PARAMS.metricTimeShifts]?.map(toNumber).filter(isNotNil)) ??
    defaultParams?.timeShifts ??
    [];

  const tagSync: (number | null)[][] =
    (urlParams[GET_PARAMS.metricFilterSync]?.[0] === removeValueChar
      ? []
      : urlParams[GET_PARAMS.metricFilterSync]?.map(parseTagSync).filter(isNotNil)) ??
    defaultParams?.tagSync ??
    [];

  const variables: VariableParams[] = [];
  for (let i = 0; i < maxPrefixArray; i++) {
    const prefix = toVariablePrefix(i);

    const name = urlParams[prefix + GET_PARAMS.variableName]?.[0];
    if (name == null) {
      if (defaultParams?.variables.length && i === 0) {
        variables.push(
          ...defaultParams.variables.map((v) => ({
            ...v,
            link: v.link.map(([p, t]) => [p, t] as [number | null, number | null]),
            args: { ...v.args },
          }))
        );
      }
      break;
    }
    if (name === removeValueChar) {
      break;
    }

    const link =
      urlParams[prefix + GET_PARAMS.variableLinkPlot][0]
        ?.split('-')
        .map((s) => {
          const [p, t] = s.split('.', 2);
          return [toNumber(p), toIndexTag(t)];
        })
        .filter(isNotNilVariableLink) ?? [];

    const variableName = `${GET_PARAMS.variableValuePrefix}${name}`;

    const description = urlParams[prefix + GET_PARAMS.variableDescription]?.[0] ?? '';

    const values = (urlParams[variableName]?.[0] === removeValueChar ? [] : urlParams[variableName]) ?? [];

    const groupBy = urlParams[`${variableName}.${GET_PARAMS.variableGroupBy}`]?.[0] === '1';
    const negative = urlParams[`${variableName}.${GET_PARAMS.variableNegative}`]?.[0] === '1';

    variables.push({ name, description, link, values, args: { groupBy, negative } });
  }

  return {
    tabNum,
    dashboard,
    timeRange: {
      to: timeRangeTo,
      from: timeRangeFrom,
    },
    eventFrom,
    plots,
    timeShifts,
    tagSync,
    variables,
  };
}

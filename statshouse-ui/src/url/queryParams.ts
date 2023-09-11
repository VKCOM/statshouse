// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import {
  Enum,
  GET_PARAMS,
  isQueryWhat,
  isTagKey,
  METRIC_VALUE_BACKEND_VERSION,
  QueryWhat,
  TAG_KEY,
  TagKey,
} from '../api/enum';
import { KeysTo, stringToTime, TIME_RANGE_KEYS_TO } from '../common/TimeRange';
import { dequal } from 'dequal/lite';
import { deepClone, isNotNil, toNumber, toString } from '../common/helpers';
import { globalSettings } from '../common/settings';

export const filterInSep = '-';
export const filterNotInSep = '~';

export const maxPrefixArray = 1000;
export const removeValueChar = String.fromCharCode(7);

export const toGroupInfoPrefix = (i: number) => `${GET_PARAMS.dashboardGroupInfoPrefix}${i}.`;
export const toPlotPrefix = (i: number) => (i ? `${GET_PARAMS.plotPrefix}${i}.` : '');
export const toVariablePrefix = (i: number) => `${GET_PARAMS.variablePrefix}${i}.`;

export const toVariableConfig = ({ name, description, link }: VariableParams) => ({ name, description, link });
export const toVariableValue = ({ values, args }: VariableParams) => ({ values, args });

export const toGroupInfoConfig = ({ name, count, size }: GroupInfo) => ({ name, count, size });
export const toGroupInfoValue = ({ show }: GroupInfo) => ({ show });

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

export function isNotNilVariableLink(link: (string | null)[]): link is VariableParamsLink {
  return toPlotKey(link[0]) != null && toTagKey(link[1]) != null;
}

export function toTagKey(s: unknown): TagKey | null;
export function toTagKey(s: unknown, defaultTagKey: TagKey): TagKey;
export function toTagKey(s: unknown, defaultTagKey?: TagKey): TagKey | null {
  const str = freeKeyPrefix(toString(s));
  if (isTagKey(str)) {
    return str;
  }
  return defaultTagKey ?? null;
}

export function normalizeFilterKey(filter: Record<string, string[]>): Partial<Record<TagKey, string[]>> {
  return Object.fromEntries(
    Object.entries(filter)
      .map(([key, values]) => {
        const tagKey = toTagKey(key);
        if (tagKey) {
          return [tagKey, values];
        }
        return null;
      })
      .filter(isNotNil)
  );
}

export interface lockRange {
  readonly min: number;
  readonly max: number;
}

export const PLOT_TYPE = {
  Metric: 0,
  Event: 1,
} as const;

export type PlotType = Enum<typeof PLOT_TYPE>;

export type PlotKey = string;

export function isPlotKey(s: unknown): s is PlotKey {
  return (typeof s === 'string' || typeof s === 'number') && toNumber(s) != null;
}

export function toPlotKey(s: unknown): PlotKey | null;
export function toPlotKey(s: unknown, defaultPlotKey: PlotKey): PlotKey;
export function toPlotKey(s: unknown, defaultPlotKey?: PlotKey): PlotKey | null {
  if (isPlotKey(s)) {
    return toString(s);
  }
  return defaultPlotKey ?? null;
}

export type PlotParams = {
  metricName: string;
  customName: string;
  what: QueryWhat[];
  customAgg: number;
  groupBy: TagKey[];
  filterIn: Partial<Record<TagKey, string[]>>;
  filterNotIn: Partial<Record<TagKey, string[]>>;
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

export type VariableParamsLink = [PlotKey, TagKey];

export type VariableParams = {
  name: string;
  description: string;
  values: string[];
  link: VariableParamsLink[];
  args: {
    groupBy?: boolean;
    negative?: boolean;
  };
  // source: string;
};

export type QueryParams = {
  ['@type']?: 'QueryParams'; // ld-json
  live: boolean;
  theme?: string;
  dashboard?: DashboardParams;
  timeRange: { to: number | KeysTo; from: number };
  eventFrom: number;
  timeShifts: number[];
  tabNum: number;
  /** @deprecated */
  tagSync: (number | null)[][]; // [group][page_plot][tag_index]
  plots: PlotParams[];
  variables: VariableParams[];
};

export function getDefaultParams(): QueryParams {
  return {
    live: false,
    theme: undefined,
    dashboard: {
      dashboard_id: undefined,
      groupInfo: [],
      description: '',
      name: '',
      version: undefined,
    },
    timeRange: { to: TIME_RANGE_KEYS_TO.default, from: 0 },
    eventFrom: 0,
    tagSync: [],
    plots: [],
    timeShifts: [],
    tabNum: 0,
    variables: [],
  };
}

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
export function toKeyTag(indexTag: number): TagKey | null;
export function toKeyTag(indexTag: number, full: boolean): string;
export function toKeyTag(indexTag: number, full?: boolean): TagKey | string | null {
  if (full) {
    return indexTag === -1 ? 'skey' : `key${indexTag}`;
  }
  return indexTag === -1 ? TAG_KEY._s : toTagKey(indexTag);
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

export function fixMessageTrouble(search: string): string {
  if (search.replaceAll) {
    return search.replaceAll('+', '%20').replace(/\.$/gi, '%2E');
  }
  return search.replace(/\+/gi, '%20').replace(/\.$/gi, '%2E');
}

export function encodeVariableConfig(value: QueryParams, defaultParams?: QueryParams): [string, string][] {
  const search: [string, string][] = [];

  // variables config
  if (
    value.variables.length &&
    !dequal(value.variables.map(toVariableConfig), defaultParams?.variables.map(toVariableConfig))
  ) {
    value.variables.forEach((variable, indexVariable) => {
      const prefix = toVariablePrefix(indexVariable);
      search.push([prefix + GET_PARAMS.variableName, variable.name]);

      if (variable.description) {
        search.push([prefix + GET_PARAMS.variableDescription, variable.description]);
      }

      if (variable.link.length) {
        search.push([
          prefix + GET_PARAMS.variableLinkPlot,
          variable.link
            .filter(isNotNilVariableLink)
            .map(([p, t]) => `${p}.${t}`)
            .join('-'),
        ]);
      }
    });
  } else if (!value.variables.length && defaultParams?.variables.length) {
    search.push([toVariablePrefix(0) + GET_PARAMS.variableName, removeValueChar]);
  }

  return search;
}

export function encodeVariableValues(value: QueryParams, defaultParams?: QueryParams): [string, string][] {
  const search: [string, string][] = [];

  // variables values
  if (
    value.variables.length &&
    !dequal(value.variables.map(toVariableValue), defaultParams?.variables.map(toVariableValue))
  ) {
    value.variables.forEach((variable, indexVariable) => {
      const variableName = `${GET_PARAMS.variableValuePrefix}${variable.name}`;

      if (variable.values.length && !dequal(variable.values, defaultParams?.variables[indexVariable]?.values ?? [])) {
        variable.values.forEach((value) => {
          search.push([variableName, value]);
        });
      } else if (!variable.values.length && defaultParams?.variables[indexVariable]?.values.length) {
        search.push([variableName, removeValueChar]);
      }

      if (variable.args.groupBy !== (defaultParams?.variables[indexVariable]?.args.groupBy ?? false)) {
        search.push([`${variableName}.${GET_PARAMS.variableGroupBy}`, variable.args.groupBy ? '1' : '0']);
      }

      if (variable.args.negative !== (defaultParams?.variables[indexVariable]?.args.negative ?? false)) {
        search.push([`${variableName}.${GET_PARAMS.variableNegative}`, variable.args.negative ? '1' : '0']);
      }
    });
  }

  return search;
}

export function encodeParams(value: QueryParams, defaultParams?: QueryParams): [string, string][] {
  const search: [string, string][] = [];

  //live
  if (value.live) {
    search.push([GET_PARAMS.metricLive, '1']);
  }

  //theme
  if (value.theme) {
    search.push([GET_PARAMS.theme, value.theme]);
  }

  // tabNum
  if (value.tabNum !== (defaultParams?.tabNum ?? 0)) {
    search.push([GET_PARAMS.metricTabNum, value.tabNum.toString()]);
  }

  // dashboard
  if (value.dashboard?.dashboard_id) {
    search.push([GET_PARAMS.dashboardID, value.dashboard.dashboard_id.toString()]);
  }

  // groupInfo config
  if (
    value.dashboard?.groupInfo?.length &&
    !dequal(
      value.dashboard?.groupInfo?.map(toGroupInfoConfig),
      defaultParams?.dashboard?.groupInfo?.map(toGroupInfoConfig)
    )
  ) {
    if (
      !(
        value.dashboard?.groupInfo?.length === 1 &&
        value.dashboard?.groupInfo[0].name === '' &&
        value.dashboard?.groupInfo[0].size === 2 &&
        value.dashboard?.groupInfo[0].show === true &&
        !defaultParams?.dashboard?.groupInfo?.length
      )
    ) {
      value.dashboard.groupInfo.forEach(({ name, show, count, size }, indexGroup) => {
        const prefix = toGroupInfoPrefix(indexGroup);
        search.push([prefix + GET_PARAMS.dashboardGroupInfoName, name]);
        if (count) {
          search.push([prefix + GET_PARAMS.dashboardGroupInfoCount, count.toString()]);
        }
        if (size !== 2) {
          search.push([prefix + GET_PARAMS.dashboardGroupInfoSize, size.toString()]);
        }
      });
    }
  } else if (!value.dashboard?.groupInfo?.length && defaultParams?.dashboard?.groupInfo?.length) {
    search.push([toGroupInfoPrefix(0) + GET_PARAMS.dashboardGroupInfoName, removeValueChar]);
  }

  // groupInfo show value
  if (
    value.dashboard?.groupInfo?.length &&
    !dequal(
      value.dashboard?.groupInfo?.map(toGroupInfoValue),
      defaultParams?.dashboard?.groupInfo?.map(toGroupInfoValue)
    )
  ) {
    value.dashboard.groupInfo.forEach(({ show }, indexGroup) => {
      const prefix = toGroupInfoPrefix(indexGroup);
      if (show !== (defaultParams?.dashboard?.groupInfo?.[indexGroup]?.show ?? true)) {
        search.push([prefix + GET_PARAMS.dashboardGroupInfoShow, show ? '1' : '0']);
      }
    });
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

  search.push(...encodeVariableConfig(value, defaultParams));
  search.push(...encodeVariableValues(value, defaultParams));
  return search;
}

export function decodeParams(searchParams: [string, string][], defaultParams?: QueryParams): QueryParams {
  const urlParams = searchParams.reduce((res, [key, value]) => {
    res[key] ??= [];
    res[key]?.push(value);
    return res;
  }, {} as Partial<Record<string, string[]>>);

  const live = urlParams[GET_PARAMS.metricLive]?.[0] === '1';

  const theme = urlParams[GET_PARAMS.theme]?.[0];

  const tabNum = toNumber(urlParams[GET_PARAMS.metricTabNum]?.[0]) ?? defaultParams?.tabNum ?? 0;

  const groupInfo: GroupInfo[] = [];

  let dashboard: DashboardParams = {
    dashboard_id:
      toNumber(urlParams[GET_PARAMS.dashboardID]?.[0]) ?? defaultParams?.dashboard?.dashboard_id ?? undefined,
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
    const count = toNumber(urlParams[prefix + GET_PARAMS.dashboardGroupInfoCount]?.[0]) ?? 0;
    const size = toNumber(urlParams[prefix + GET_PARAMS.dashboardGroupInfoSize]?.[0]) ?? 2;
    groupInfo.push({ name, show: defaultParams?.dashboard?.groupInfo?.[i]?.show ?? true, count, size });
  }
  groupInfo.forEach((group, indexGroup) => {
    const prefix = toGroupInfoPrefix(indexGroup);
    if (urlParams[prefix + GET_PARAMS.dashboardGroupInfoShow]?.length) {
      group.show = urlParams[prefix + GET_PARAMS.dashboardGroupInfoShow]?.[0] !== '0';
    }
  });

  const timeRangeTo =
    stringToTime(urlParams[GET_PARAMS.toTime]?.[0] ?? '') ?? defaultParams?.timeRange.to ?? TIME_RANGE_KEYS_TO.default;

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
    const groupBy: TagKey[] =
      urlParams[prefix + GET_PARAMS.metricGroupBy]?.map((s) => toTagKey(s)).filter(isNotNil) ?? [];

    const filterIn: Partial<Record<TagKey, string[]>> = {};
    const filterNotIn: Partial<Record<TagKey, string[]>> = {};
    urlParams[prefix + GET_PARAMS.metricFilter]?.forEach((s) => {
      const pos = s.indexOf(filterInSep);
      const pos2 = s.indexOf(filterNotInSep);
      if (pos2 === -1 || (pos2 > pos && pos > -1)) {
        const tagKey = toTagKey(s.substring(0, pos));
        const tagValue = s.substring(pos + 1);
        if (tagKey && tagValue) {
          filterIn[tagKey] ??= [];
          filterIn[tagKey]?.push(tagValue);
        }
      } else if (pos === -1 || (pos > pos2 && pos2 > -1)) {
        const tagKey = toTagKey(s.substring(0, pos2));
        const tagValue = s.substring(pos2 + 1);
        if (tagKey && tagValue) {
          filterNotIn[tagKey] ??= [];
          filterNotIn[tagKey]?.push(tagValue);
        }
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
        variables.push(...deepClone(defaultParams.variables));
      }
      break;
    }
    if (name === removeValueChar) {
      break;
    }

    const link =
      urlParams[prefix + GET_PARAMS.variableLinkPlot]?.[0]
        ?.split('-')
        .map((s) => {
          const [p, t] = s.split('.', 2);
          return [toPlotKey(p), toTagKey(t)];
        })
        .filter(isNotNilVariableLink) ?? [];

    const description = urlParams[prefix + GET_PARAMS.variableDescription]?.[0] ?? '';

    variables.push({
      name,
      description,
      link,
      values: defaultParams?.variables[i]?.values ?? [],
      args: {
        groupBy: defaultParams?.variables[i]?.args.groupBy ?? false,
        negative: defaultParams?.variables[i]?.args.negative ?? false,
      },
    });
  }

  variables.forEach((variable) => {
    const variableName = `${GET_PARAMS.variableValuePrefix}${variable.name}`;
    if (urlParams[variableName]?.length) {
      variable.values = (urlParams[variableName]?.[0] === removeValueChar ? [] : urlParams[variableName]) ?? [];
    }
    if (urlParams[`${variableName}.${GET_PARAMS.variableGroupBy}`]?.length) {
      variable.args.groupBy = urlParams[`${variableName}.${GET_PARAMS.variableGroupBy}`]?.[0] === '1';
    }
    if (urlParams[`${variableName}.${GET_PARAMS.variableNegative}`]?.length) {
      variable.args.negative = urlParams[`${variableName}.${GET_PARAMS.variableNegative}`]?.[0] === '1';
    }
  });

  return {
    live,
    theme,
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

export function decodeDashboardIdParam(urlSearchParams: URLSearchParams) {
  return toNumber(urlSearchParams.get(GET_PARAMS.dashboardID));
}

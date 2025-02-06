// Copyright 2025 V Kontakte LLC
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
  MetricType,
  metricTypeToMetricTypeUrl,
  metricTypeUrlToMetricType,
  QueryWhat,
  TAG_KEY,
  TagKey,
  TIME_RANGE_KEYS_TO,
  TimeRangeKeysTo,
} from '@/api/enum';
import { dequal } from 'dequal/lite';
import {
  deepClone,
  isNotNil,
  numberAsStr,
  searchParamsObjectValueSymbol,
  searchParamsToObject,
  sortEntity,
  toNumber,
  toString,
  uniqueArray,
} from '@/common/helpers';
import { globalSettings } from '@/common/settings';
import { stringToTime } from '@/url2';

export const filterInSep = '-';
export const filterNotInSep = '~';

export const variableSourceSplitter = '-';

export const maxPrefixArray = 1000;
export const removeValueChar = String.fromCharCode(7);

export const toGroupInfoPrefix = (i: number) => `${GET_PARAMS.dashboardGroupInfoPrefix}${i}`;
export const toPlotPrefix = (i: number | string) => (i ? `${GET_PARAMS.plotPrefix}${i}` : '');
export const toVariablePrefix = (i: number) => `${GET_PARAMS.variablePrefix}${i}`;

export const toVariableConfig = ({ name, description, link, source }: VariableParams) => ({
  name,
  description,
  link,
  source,
});
export const toVariableValue = ({ values, args }: VariableParams) => ({ values, args });

export const toGroupInfoConfig = ({ name, count, size }: GroupInfo) => ({ name, count, size });
export const toGroupInfoValue = ({ show }: GroupInfo) => ({ show });

export const parseTagSync = (s?: string) => {
  if (s == null) {
    return null;
  }
  return [
    ...s.split('-').reduce(
      (res, t) => {
        const [plot, tagKey] = t.split('.').map((r) => parseInt(r));
        res[plot] = tagKey;
        return res;
      },
      [] as (number | null)[]
    ),
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

export function normalizeFilterKey(filter: Record<string, string[]>): FilterTag {
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

export type FilterTag = Partial<Record<TagKey, string[]>>;

export type PlotParams = {
  id: PlotKey;
  metricName: string;
  customName: string;
  metricType?: MetricType;
  customDescription: string;
  what: QueryWhat[];
  customAgg: number;
  groupBy: TagKey[];
  filterIn: FilterTag;
  filterNotIn: FilterTag;
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
  totalLine: boolean;
  filledGraph: boolean;
  logScale: boolean;
};

export type GroupInfo = {
  name: string;
  show: boolean;
  count: number;
  size: string;
  description: string;
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
export type VariableParamsSource = {
  metric: string;
  tag: TagKey;
  filterIn: FilterTag;
  filterNotIn: FilterTag;
};
export type VariableParams = {
  name: string;
  description: string;
  values: string[];
  link: VariableParamsLink[];
  args: {
    groupBy?: boolean;
    negative?: boolean;
  };
  source: VariableParamsSource[];
};

export type QueryParams = {
  ['@type']?: 'QueryParams'; // ld-json
  live: boolean;
  theme?: string;
  dashboard?: DashboardParams;
  timeRange: { to: number | TimeRangeKeysTo; from: number };
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
    id: '',
    metricName: globalSettings.default_metric,
    customName: '',
    customDescription: '',
    promQL: '',
    metricType: undefined,
    what: globalSettings.default_metric_what?.slice() ?? [],
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
    totalLine: false,
    filledGraph: true,
    logScale: false,
  };
}

export function toPlotType(s: unknown): PlotType | null {
  const t = toNumber(s);
  if (t && isPlotType(t)) {
    return t;
  }
  return null;
}

export function isGetParam(key: string, getPrefix: string) {
  return key.indexOf(getPrefix) === 0 && numberAsStr(key.slice(getPrefix.length));
}

export function encodeVariableConfig(value: QueryParams, defaultParams?: QueryParams): [string, string][] {
  const search: [string, string][] = [];

  // variables config
  if (
    value.variables.length &&
    !dequal(value.variables.map(toVariableConfig), defaultParams?.variables.map(toVariableConfig))
  ) {
    value.variables.forEach((variable, indexVariable) => {
      const prefix = toVariablePrefix(indexVariable) + '.';
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
      if (variable.source.length) {
        variable.source.forEach((source, indexSource) => {
          const sourcePrefix = prefix + GET_PARAMS.variableSourcePrefix + indexSource.toString() + '.';
          search.push([sourcePrefix + GET_PARAMS.variableSourceMetricName, source.metric]);
          search.push([sourcePrefix + GET_PARAMS.variableSourceTag, source.tag]);

          const entriesFilterIn = Object.entries(source.filterIn);
          if (entriesFilterIn.length) {
            entriesFilterIn.forEach(([keyTag, valuesTag]) => {
              valuesTag.forEach((valueTag) => {
                search.push([
                  sourcePrefix + GET_PARAMS.variableSourceFilter,
                  freeKeyPrefix(keyTag) + filterInSep + valueTag,
                ]);
              });
            });
          }

          const entriesFilterNotIn = Object.entries(source.filterNotIn);
          if (entriesFilterNotIn.length) {
            entriesFilterNotIn.forEach(([keyTag, valuesTag]) => {
              valuesTag.forEach((valueTag) => {
                search.push([
                  sourcePrefix + GET_PARAMS.metricFilter,
                  freeKeyPrefix(keyTag) + filterNotInSep + valueTag,
                ]);
              });
            });
          }
        });
      }
    });
  } else if (!value.variables.length && defaultParams?.variables.length) {
    search.push([toVariablePrefix(0) + '.' + GET_PARAMS.variableName, removeValueChar]);
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
      const variableName = `${GET_PARAMS.variableValuePrefix}.${variable.name}`;

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
        value.dashboard?.groupInfo[0].size === '2' &&
        value.dashboard?.groupInfo[0].show === true &&
        !defaultParams?.dashboard?.groupInfo?.length
      )
    ) {
      value.dashboard.groupInfo.forEach(({ name, count, size, description }, indexGroup) => {
        const prefix = toGroupInfoPrefix(indexGroup) + '.';
        search.push([prefix + GET_PARAMS.dashboardGroupInfoName, name]);
        if (count) {
          search.push([prefix + GET_PARAMS.dashboardGroupInfoCount, count.toString()]);
        }
        if (size !== '2') {
          search.push([prefix + GET_PARAMS.dashboardGroupInfoSize, size.toString()]);
        }
        if (description) {
          search.push([prefix + GET_PARAMS.dashboardGroupInfoDescription, description]);
        }
      });
    }
  } else if (!value.dashboard?.groupInfo?.length && defaultParams?.dashboard?.groupInfo?.length) {
    search.push([toGroupInfoPrefix(0) + '.' + GET_PARAMS.dashboardGroupInfoName, removeValueChar]);
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
      const prefix = toGroupInfoPrefix(indexGroup) + '.';
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
  const defaultPlot = getNewPlot();

  if (value.plots.length && !dequal(value.plots, defaultParams?.plots)) {
    value.plots.forEach((plot, indexPlot) => {
      const prefix = toPlotPrefix(indexPlot) + (indexPlot > 0 ? '.' : '');

      search.push([prefix + GET_PARAMS.metricName, plot.metricName]);

      if (plot.customName !== defaultPlot.customName) {
        search.push([prefix + GET_PARAMS.metricCustomName, plot.customName]);
      }

      if (plot.metricType != null && plot.metricType !== defaultPlot.metricType) {
        search.push([prefix + GET_PARAMS.metricMetricUnit, metricTypeToMetricTypeUrl(plot.metricType)]);
      }

      if (plot.customDescription !== defaultPlot.customDescription) {
        search.push([prefix + GET_PARAMS.metricCustomDescription, plot.customDescription]);
      }

      if (!dequal(plot.what, defaultPlot.what) && plot.what.length) {
        plot.what.forEach((w) => {
          search.push([prefix + GET_PARAMS.metricWhat, w]);
        });
      }

      if (plot.customAgg !== defaultPlot.customAgg) {
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
      if (plot.type === PLOT_TYPE.Event) {
        if (plot.numSeries !== 0) {
          search.push([prefix + GET_PARAMS.numResults, plot.numSeries.toString()]);
        }
      } else {
        if (plot.numSeries !== defaultPlot.numSeries) {
          search.push([prefix + GET_PARAMS.numResults, plot.numSeries.toString()]);
        }
      }

      if (!plot.useV2) {
        search.push([
          prefix + GET_PARAMS.version,
          plot.useV2 ? METRIC_VALUE_BACKEND_VERSION.v2 : METRIC_VALUE_BACKEND_VERSION.v1,
        ]);
      }

      if (plot.yLock.min !== defaultPlot.yLock.min) {
        search.push([prefix + GET_PARAMS.metricLockMin, plot.yLock.min.toString()]);
      }

      if (plot.yLock.max !== defaultPlot.yLock.max) {
        search.push([prefix + GET_PARAMS.metricLockMax, plot.yLock.max.toString()]);
      }

      if (plot.maxHost) {
        search.push([prefix + GET_PARAMS.metricMaxHost, '1']);
      }

      if (plot.promQL) {
        search.push([prefix + GET_PARAMS.metricPromQL, plot.promQL]);
      }

      if (plot.type !== defaultPlot.type) {
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

      // totalLine
      if (plot.totalLine) {
        search.push([prefix + GET_PARAMS.viewTotalLine, '1']);
      }

      // filledGraph
      if (!plot.filledGraph) {
        search.push([prefix + GET_PARAMS.viewFilledGraph, '0']);
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
  const urlParams = searchParamsToObject(searchParams);

  const live = urlParams[GET_PARAMS.metricLive]?.[searchParamsObjectValueSymbol]?.[0] === '1';

  const theme = urlParams[GET_PARAMS.theme]?.[searchParamsObjectValueSymbol]?.[0];

  const tabNum =
    toNumber(urlParams[GET_PARAMS.metricTabNum]?.[searchParamsObjectValueSymbol]?.[0]) ?? defaultParams?.tabNum ?? 0;

  const groupInfo: GroupInfo[] = [];

  const dashboard: DashboardParams = {
    dashboard_id:
      toNumber(urlParams[GET_PARAMS.dashboardID]?.[searchParamsObjectValueSymbol]?.[0]) ??
      defaultParams?.dashboard?.dashboard_id ??
      undefined,
    name: defaultParams?.dashboard?.name ?? '',
    description: defaultParams?.dashboard?.description ?? '',
    groupInfo,
    version: defaultParams?.dashboard?.version,
  };

  for (let i = 0; i < maxPrefixArray; i++) {
    const prefix = toGroupInfoPrefix(i);

    const name = urlParams[prefix]?.[GET_PARAMS.dashboardGroupInfoName]?.[searchParamsObjectValueSymbol]?.[0];

    if (name == null) {
      if (defaultParams?.dashboard?.groupInfo?.length && i === 0) {
        groupInfo.push(...defaultParams.dashboard.groupInfo.map((g) => ({ ...g })));
      }
      break;
    }
    if (name === removeValueChar) {
      break;
    }
    const description =
      urlParams[prefix]?.[GET_PARAMS.dashboardGroupInfoDescription]?.[searchParamsObjectValueSymbol]?.[0] ?? '';
    const count =
      toNumber(urlParams[prefix]?.[GET_PARAMS.dashboardGroupInfoCount]?.[searchParamsObjectValueSymbol]?.[0]) ?? 0;
    const size = urlParams[prefix]?.[GET_PARAMS.dashboardGroupInfoSize]?.[searchParamsObjectValueSymbol]?.[0] ?? '2';
    groupInfo.push({ name, show: defaultParams?.dashboard?.groupInfo?.[i]?.show ?? true, count, size, description });
  }
  groupInfo.forEach((group, indexGroup) => {
    const prefix = toGroupInfoPrefix(indexGroup);
    if (urlParams[prefix]?.[GET_PARAMS.dashboardGroupInfoShow]?.[searchParamsObjectValueSymbol]?.length) {
      group.show = urlParams[prefix]?.[GET_PARAMS.dashboardGroupInfoShow]?.[searchParamsObjectValueSymbol]?.[0] !== '0';
    }
  });

  const timeRangeTo =
    stringToTime(urlParams[GET_PARAMS.toTime]?.[searchParamsObjectValueSymbol]?.[0] ?? '') ??
    defaultParams?.timeRange.to ??
    TIME_RANGE_KEYS_TO.default;

  const timeRangeFrom =
    toNumber(urlParams[GET_PARAMS.fromTime]?.[searchParamsObjectValueSymbol]?.[0]) ??
    defaultParams?.timeRange.from ??
    0;

  const eventFrom =
    toNumber(urlParams[GET_PARAMS.metricEventFrom]?.[searchParamsObjectValueSymbol]?.[0]) ??
    defaultParams?.eventFrom ??
    0;

  const plots: PlotParams[] = [];
  const defaultPlot = getNewPlot();
  const plotIdList = Object.keys(urlParams)
    .filter((key) => key === 's' || isGetParam(key, GET_PARAMS.plotPrefix))
    .map((key) => (key === 's' ? '' : key.slice(1)));
  for (let i = 0; i < plotIdList.length; i++) {
    const prefix = toPlotPrefix(plotIdList[i]);
    const id = toPlotKey(plotIdList[i], '0');
    const plotParams = urlParams[prefix] ?? urlParams;
    const metricName = plotParams[GET_PARAMS.metricName]?.[searchParamsObjectValueSymbol]?.[0];
    if (metricName == null) {
      if (defaultParams?.plots.length && i === 0) {
        plots.push(...deepClone(defaultParams.plots));
      }
      break;
    }
    if (metricName === removeValueChar) {
      break;
    }
    const customName =
      plotParams[GET_PARAMS.metricCustomName]?.[searchParamsObjectValueSymbol]?.[0] ?? defaultPlot.customName;
    const customDescription =
      plotParams[GET_PARAMS.metricCustomDescription]?.[searchParamsObjectValueSymbol]?.[0] ??
      defaultPlot.customDescription;
    const metricType: MetricType | undefined =
      metricTypeUrlToMetricType(plotParams[GET_PARAMS.metricMetricUnit]?.[searchParamsObjectValueSymbol]?.[0]) ??
      defaultPlot.metricType;
    const what: QueryWhat[] =
      plotParams[GET_PARAMS.metricWhat]?.[searchParamsObjectValueSymbol]?.filter(isQueryWhat) ??
      defaultPlot.what.slice();
    const customAgg =
      toNumber(plotParams[GET_PARAMS.metricAgg]?.[searchParamsObjectValueSymbol]?.[0]) ?? defaultPlot.customAgg;
    const groupBy: TagKey[] =
      plotParams[GET_PARAMS.metricGroupBy]?.[searchParamsObjectValueSymbol]?.map((s) => toTagKey(s)).filter(isNotNil) ??
      [];

    const filterIn: FilterTag = {};
    const filterNotIn: FilterTag = {};
    plotParams[GET_PARAMS.metricFilter]?.[searchParamsObjectValueSymbol]?.forEach((s) => {
      const pos = s.indexOf(filterInSep);
      const pos2 = s.indexOf(filterNotInSep);
      if (pos2 === -1 || (pos2 > pos && pos > -1)) {
        const tagKey = toTagKey(s.substring(0, pos));
        const tagValue = s.substring(pos + 1);
        if (tagKey && tagValue) {
          filterIn[tagKey] = sortEntity(uniqueArray([...(filterIn[tagKey] ?? []), tagValue]));
        }
      } else if (pos === -1 || (pos > pos2 && pos2 > -1)) {
        const tagKey = toTagKey(s.substring(0, pos2));
        const tagValue = s.substring(pos2 + 1);
        if (tagKey && tagValue) {
          filterNotIn[tagKey] = sortEntity(uniqueArray([...(filterNotIn[tagKey] ?? []), tagValue]));
        }
      }
    });

    const useV2 =
      plotParams[GET_PARAMS.version]?.[searchParamsObjectValueSymbol]?.[0] !== METRIC_VALUE_BACKEND_VERSION.v1;

    const yLockMin =
      toNumber(plotParams[GET_PARAMS.metricLockMin]?.[searchParamsObjectValueSymbol]?.[0]) ?? defaultPlot.yLock.min;
    const yLockMax =
      toNumber(plotParams[GET_PARAMS.metricLockMax]?.[searchParamsObjectValueSymbol]?.[0]) ?? defaultPlot.yLock.max;

    const maxHost = plotParams[GET_PARAMS.metricMaxHost]?.[searchParamsObjectValueSymbol]?.[0] === '1';

    const promQL = plotParams[GET_PARAMS.metricPromQL]?.[searchParamsObjectValueSymbol]?.[0] ?? '';

    const type =
      toPlotType(plotParams[GET_PARAMS.metricType]?.[searchParamsObjectValueSymbol]?.[0]) ?? defaultPlot.type;

    const numSeries =
      toNumber(plotParams[GET_PARAMS.numResults]?.[searchParamsObjectValueSymbol]?.[0]) ??
      (type === PLOT_TYPE.Event ? 0 : defaultPlot.numSeries);

    const events =
      plotParams[GET_PARAMS.metricEvent]?.[searchParamsObjectValueSymbol]?.map(toNumber).filter(isNotNil) ?? [];

    const eventsBy = plotParams[GET_PARAMS.metricEventBy]?.[searchParamsObjectValueSymbol] ?? [];

    const eventsHide = plotParams[GET_PARAMS.metricEventHide]?.[searchParamsObjectValueSymbol] ?? [];

    const totalLine = plotParams[GET_PARAMS.viewTotalLine]?.[searchParamsObjectValueSymbol]?.[0] === '1';
    const filledGraph = plotParams[GET_PARAMS.viewFilledGraph]?.[searchParamsObjectValueSymbol]?.[0] !== '0';
    const logScale = plotParams[GET_PARAMS.viewLogScale]?.[searchParamsObjectValueSymbol]?.[0] === '1';

    plots.push({
      id,
      metricName,
      customName,
      customDescription,
      metricType,
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
      totalLine,
      filledGraph,
      logScale,
    });
  }

  if (!plotIdList.length && defaultParams?.plots.length) {
    plots.push(...deepClone(defaultParams.plots));
  }

  const timeShifts =
    (urlParams[GET_PARAMS.metricTimeShifts]?.[searchParamsObjectValueSymbol]?.[0] === removeValueChar
      ? []
      : urlParams[GET_PARAMS.metricTimeShifts]?.[searchParamsObjectValueSymbol]?.map(toNumber).filter(isNotNil)) ??
    defaultParams?.timeShifts ??
    [];

  const tagSync: (number | null)[][] =
    (urlParams[GET_PARAMS.metricFilterSync]?.[searchParamsObjectValueSymbol]?.[0] === removeValueChar
      ? []
      : urlParams[GET_PARAMS.metricFilterSync]?.[searchParamsObjectValueSymbol]?.map(parseTagSync).filter(isNotNil)) ??
    defaultParams?.tagSync ??
    [];

  const variables: VariableParams[] = [];
  for (let i = 0; i < maxPrefixArray; i++) {
    const prefix = toVariablePrefix(i);

    const name = urlParams[prefix]?.[GET_PARAMS.variableName]?.[searchParamsObjectValueSymbol]?.[0];
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
      urlParams[prefix]?.[GET_PARAMS.variableLinkPlot]?.[searchParamsObjectValueSymbol]?.[0]
        ?.split('-')
        .map((s) => {
          const [p, t] = s.split('.', 2);
          return [toPlotKey(p), toTagKey(t)];
        })
        .filter(isNotNilVariableLink) ?? [];

    const description = urlParams[prefix]?.[GET_PARAMS.variableDescription]?.[searchParamsObjectValueSymbol]?.[0] ?? '';

    const source: VariableParamsSource[] = Object.entries(urlParams[prefix] ?? {}).reduce((res, [key, value]) => {
      if (isGetParam(key, GET_PARAMS.variableSourcePrefix)) {
        const filterIn: FilterTag = {};
        const filterNotIn: FilterTag = {};
        value?.[GET_PARAMS.variableSourceFilter]?.[searchParamsObjectValueSymbol]?.forEach((s) => {
          const pos = s.indexOf(filterInSep);
          const pos2 = s.indexOf(filterNotInSep);
          if (pos2 === -1 || (pos2 > pos && pos > -1)) {
            const tagKey = toTagKey(s.substring(0, pos));
            const tagValue = s.substring(pos + 1);
            if (tagKey && tagValue) {
              filterIn[tagKey] = sortEntity(uniqueArray([...(filterIn[tagKey] ?? []), tagValue]));
            }
          } else if (pos === -1 || (pos > pos2 && pos2 > -1)) {
            const tagKey = toTagKey(s.substring(0, pos2));
            const tagValue = s.substring(pos2 + 1);
            if (tagKey && tagValue) {
              filterNotIn[tagKey] = sortEntity(uniqueArray([...(filterNotIn[tagKey] ?? []), tagValue]));
            }
          }
        });

        res.push({
          metric: value?.[GET_PARAMS.variableSourceMetricName]?.[searchParamsObjectValueSymbol]?.[0] ?? '',
          tag: toTagKey(value?.[GET_PARAMS.variableSourceTag]?.[searchParamsObjectValueSymbol]?.[0]) ?? TAG_KEY._0,
          filterIn,
          filterNotIn,
        });
      }
      return res;
    }, [] as VariableParamsSource[]);

    variables.push({
      name,
      description,
      link,
      values: defaultParams?.variables[i]?.values ?? [],
      args: {
        groupBy: defaultParams?.variables[i]?.args.groupBy ?? false,
        negative: defaultParams?.variables[i]?.args.negative ?? false,
      },
      source,
    });
  }

  variables.forEach((variable) => {
    const variableParamsValue = urlParams[GET_PARAMS.variableValuePrefix] ?? {};
    const variableName = variable.name;
    if (variableParamsValue[variableName]?.[searchParamsObjectValueSymbol]?.length) {
      variable.values =
        (variableParamsValue[variableName]?.[searchParamsObjectValueSymbol]?.[0] === removeValueChar
          ? []
          : variableParamsValue[variableName]?.[searchParamsObjectValueSymbol]) ?? [];
    }
    if (variableParamsValue[variableName]?.[GET_PARAMS.variableGroupBy]?.[searchParamsObjectValueSymbol]?.length) {
      variable.args.groupBy =
        variableParamsValue[variableName]?.[GET_PARAMS.variableGroupBy]?.[searchParamsObjectValueSymbol]?.[0] === '1';
    }
    if (variableParamsValue[variableName]?.[GET_PARAMS.variableNegative]?.[searchParamsObjectValueSymbol]?.length) {
      variable.args.negative =
        variableParamsValue[variableName]?.[GET_PARAMS.variableNegative]?.[searchParamsObjectValueSymbol]?.[0] === '1';
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

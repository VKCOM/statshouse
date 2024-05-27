import {
  GroupInfo,
  GroupKey,
  PlotKey,
  PlotParams,
  QueryParams,
  TimeRange,
  VariableParams,
  VariableParamsLink,
  VariableParamsSource,
} from './queryParams';
import { deepClone, isNotNil, toNumber, toString } from '../../common/helpers';
import {
  GET_PARAMS,
  METRIC_TYPE,
  PLOT_TYPE,
  QUERY_WHAT,
  TAG_KEY,
  TagKey,
  TIME_RANGE_KEYS_TO,
  toTagKey,
  toTimeRangeKeysTo,
} from '../../api/enum';
import { urlEncode, urlDecode } from '../urlStore';
import { produce } from 'immer';

export const filterInSep = '-';
export const filterNotInSep = '~';
export const orderPlotSplitter = '.';
export const orderGroupSplitter = '.';
export const orderVariableSplitter = '.';
export const removeValueChar = String.fromCharCode(7);
export const promQLMetric = '~';

export function arrToObj(arr: [string, string][]) {
  return arr.reduce(
    (res, [key, value]) => {
      (res[key] ??= []).push(value);
      return res;
    },
    {} as Partial<Record<string, string[]>>
  );
}

export const treeParamsObjectValueSymbol = Symbol('value');
export type TreeParamsObject = Partial<{
  [key: string]: TreeParamsObject;
  [treeParamsObjectValueSymbol]: string[];
}>;
export function toTreeObj(obj: Partial<Record<string, string[]>>): TreeParamsObject {
  const res = {};
  for (let key in obj) {
    const keys = key.split('.');
    let target: TreeParamsObject = res;
    keys.forEach((keyName) => {
      target = target[keyName] ??= {};
    });
    target[treeParamsObjectValueSymbol] = obj[key];
  }
  return res;
}

export function toTimeStamp(time: number) {
  return Math.floor(time / 1000);
}

export function toDateTime(now?: number) {
  return now != null ? new Date(now * 1000) : new Date();
}

export function getNow(now?: number): number {
  if (typeof now === 'number') {
    return now;
  }
  return toTimeStamp(Date.now());
}

export function getEndDay(now?: number) {
  let time = toDateTime(now);
  time.setHours(23, 59, 59, 0);
  return toTimeStamp(+time);
}

export function getEndWeek(now?: number) {
  let time = toDateTime(now);
  time.setHours(23, 59, 59, 0);
  time.setDate(time.getDate() - (time.getDay() || 7) + 7);
  return toTimeStamp(+time);
}

export function readTimeRange(from: unknown, to: unknown): TimeRange {
  const timeNow = getNow();
  let urlTo = toNumber(to) || toTimeRangeKeysTo(to, TIME_RANGE_KEYS_TO.default); //?
  let timeTo;
  let timeAbsolute = (typeof urlTo === 'number' && urlTo > 0) || urlTo === TIME_RANGE_KEYS_TO.default;
  switch (urlTo) {
    case TIME_RANGE_KEYS_TO.EndDay:
      timeTo = getEndDay(timeNow);
      break;
    case TIME_RANGE_KEYS_TO.EndWeek:
      timeTo = getEndWeek(timeNow);
      break;
    case TIME_RANGE_KEYS_TO.Now:
      timeTo = timeNow;
      break;
    case TIME_RANGE_KEYS_TO.default:
      urlTo = timeTo = timeNow;
      break;
    default:
      timeTo = urlTo;
  }
  if (typeof urlTo === 'number' && urlTo <= 0) {
    timeTo = urlTo + timeNow;
  }

  let timeFrom = toNumber(from, 0);
  if (timeFrom > 0) {
    timeFrom = timeFrom - timeTo;
  }
  return { urlTo, to: timeTo, from: timeFrom, now: timeNow, absolute: timeAbsolute };
}

export function freeKeyPrefix(str: string): string {
  return str.replace('skey', '_s').replace('key', '');
}

export function isNotNilVariableLink(link: (string | null)[]): link is VariableParamsLink {
  return toPlotKey(link[0]) != null && toTagKey(link[1]) != null;
}

export function isKeyId(s: unknown): s is string {
  return (typeof s === 'string' || typeof s === 'number') && toNumber(s) != null;
}

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

export function sortUniqueKeys<T extends string | number>(arr: T[]): T[] {
  return Object.values(
    arr.reduce(
      (res, v) => {
        res[v] = v;
        return res;
      },
      {} as Partial<Record<T, T>>
    )
  );
}

export function getDefaultParams(): QueryParams {
  return {
    orderPlot: [],
    plots: {},
    dashboardId: undefined,
    eventFrom: 0,
    timeShifts: [],
    tabNum: '0',
    theme: undefined,
    live: false,
    orderVariables: [],
    orderGroup: ['0'],
    groups: { '0': { ...getNewGroup(), id: '0' } },
    timeRange: {
      from: 0,
      urlTo: TIME_RANGE_KEYS_TO.default,
      absolute: false,
      now: 0,
      to: 0,
    },
    variables: {},
    dashboardDescription: '',
    dashboardName: '',
    dashboardVersion: undefined,
  };
}

export function getNewPlot(): PlotParams {
  return {
    id: '',
    metricName: '',
    customName: '',
    customDescription: '',
    promQL: '',
    metricUnit: METRIC_TYPE.none,
    what: [QUERY_WHAT.countNorm],
    customAgg: 0,
    groupBy: [],
    filterIn: {},
    filterNotIn: {},
    numSeries: 5,
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
    timeShifts: [],
  };
}

export function getNewGroup(): GroupInfo {
  return {
    id: '',
    name: '',
    description: '',
    count: 0,
    size: '2',
    show: true,
  };
}

export function getNewVariable(): VariableParams {
  return {
    id: '',
    name: '',
    description: '',
    link: [],
    source: {},
    sourceOrder: [],
    values: [],
    negative: false,
    groupBy: false,
  };
}

export function getNewVariableSource(): VariableParamsSource {
  return {
    id: '',
    metric: '',
    tag: TAG_KEY._0,
    filterIn: {},
    filterNotIn: {},
  };
}

export const toGroupInfoPrefix = (i: number | string) => `${GET_PARAMS.dashboardGroupInfoPrefix}${i}.`;
export const toPlotPrefix = (i: number | string) => (i && i !== '0' ? `${GET_PARAMS.plotPrefix}${i}.` : '');
export const toVariablePrefix = (i: number | string) => `${GET_PARAMS.variablePrefix}${i}.`;
export const toVariableValuePrefix = (name: string) => `${GET_PARAMS.variableValuePrefix}.${name}`;

export function getNewPlotIndex(params: QueryParams): PlotKey {
  let n = toNumber(Object.keys(params.plots).slice(-1)[0], -1) + 1;
  while (params.plots[n]) {
    n++;
  }
  return n.toString();
}

export function getPlotLink(plotKey: PlotKey, params: QueryParams, saveParams?: QueryParams): string {
  return (
    '?' +
    new URLSearchParams(
      urlEncode(
        produce(params, (p) => {
          p.tabNum = plotKey;
        }),
        saveParams
      )
    ).toString()
  );
}

export function getAddPlotLink(params: QueryParams, saveParams?: QueryParams): string {
  const tabNum = params.plots[params.tabNum] ? params.tabNum : params.orderPlot.slice(-1)[0];
  const plot = deepClone(params.plots[tabNum]) ?? getNewPlot();
  const nextParams = addPlot(plot, params);
  return '?' + new URLSearchParams(urlEncode(nextParams, saveParams)).toString();
}

export type GroupPlotsMap = {
  groupPlots: Partial<Record<GroupKey, PlotKey[]>>;
  orderGroup: GroupKey[];
  viewOrderPlot: PlotKey[];
  plotToGroupMap: Partial<Record<PlotKey, GroupKey>>;
};

export function getGroupPlotsMap(params: QueryParams): GroupPlotsMap {
  const orderPlots = [...params.orderPlot];
  const plotToGroupMap: Partial<Record<PlotKey, GroupKey>> = {};
  const groupPlots = params.orderGroup.reduce(
    (res, groupKey) => {
      const group = params.groups[groupKey];
      if (group) {
        const plots = orderPlots.splice(0, group.count);
        res[groupKey] = plots;
        Object.assign(plotToGroupMap, Object.fromEntries(plots.map((p) => [p, groupKey])));
      }
      return res;
    },
    {} as Partial<Record<GroupKey, PlotKey[]>>
  );
  //add no group plots
  if (orderPlots.length) {
    const groupKey = params.orderGroup.slice(-1)[0] ?? '0';
    groupPlots[groupKey] = [...(groupPlots[groupKey] ?? []), ...orderPlots];
  }
  return {
    groupPlots,
    orderGroup: [...params.orderGroup],
    viewOrderPlot: params.orderGroup
      .filter((g) => params.groups[g]?.show ?? true)
      .flatMap((g) => groupPlots[g])
      .filter(isNotNil),
    plotToGroupMap,
  };
}

export function updateGroupsPlot(groupPlotsMap: GroupPlotsMap, params: QueryParams): QueryParams {
  return produce(params, (p) => {
    p.orderPlot = groupPlotsMap.orderGroup
      .flatMap((g) => {
        const group = p.groups[g];
        if (group) {
          group.count = groupPlotsMap.groupPlots[g]?.length ?? 0;
        }
        return groupPlotsMap.groupPlots[g];
      })
      .filter(isNotNil);
    p.orderGroup = [...groupPlotsMap.orderGroup];
  });
}

export function addPlotByUrl(url: string, params: QueryParams) {
  let nextParams = params;
  getPlotByUrl(url).forEach((plot) => {
    nextParams = addPlot(plot, nextParams);
  });
  return nextParams;
}
export function addPlot(
  plot: PlotParams,
  params: QueryParams,
  group?: GroupKey,
  activeInsert: boolean = true
): QueryParams {
  return produce(params, (p) => {
    const tabNum = p.plots[p.tabNum] ? p.tabNum : p.orderPlot.slice(-1)[0];
    const groupPlotMap = getGroupPlotsMap(p);
    const activeGroup = group ?? groupPlotMap.plotToGroupMap[tabNum] ?? p.orderGroup.slice(-1)[0];
    const newTabNum = getNewPlotIndex(p);
    p.plots[newTabNum] = { ...plot, id: newTabNum };
    const { orderPlot, groups, orderGroup } = updateGroupsPlot(
      produce(groupPlotMap, (gpm) => {
        gpm.groupPlots[activeGroup]?.push(newTabNum);
      }),
      p
    );
    p.orderGroup = orderGroup;
    p.orderPlot = orderPlot;
    p.groups = groups;
    if (activeInsert) {
      p.tabNum = newTabNum;
    }
  });
}

export function getPlotByUrl(url: string): PlotParams[] {
  try {
    const getUrl = new URL(url, window.document.location.origin);
    const tree = toTreeObj(arrToObj([...getUrl.searchParams.entries()]));
    const params = urlDecode(tree);
    return Object.values(params.plots).filter(isNotNil);
  } catch (e) {
    return [];
  }
}

export function filterHasTagID(params: PlotParams, tagKey: TagKey): boolean {
  return (
    (params.filterIn[tagKey] !== undefined && params.filterIn[tagKey]?.length !== 0) ||
    (params.filterNotIn[tagKey] !== undefined && params.filterNotIn[tagKey]?.length !== 0) ||
    params.groupBy.indexOf(tagKey) >= 0
  );
}

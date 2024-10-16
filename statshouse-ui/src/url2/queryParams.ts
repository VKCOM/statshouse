import {
  type MetricType,
  MetricValueBackendVersion,
  type PlotType,
  type QueryWhat,
  type TagKey,
  type TimeRangeKeysTo,
} from 'api/enum';

export type PlotKey = string;

export type GroupKey = string;

export type GroupInfo = {
  id: GroupKey;
  name: string;
  show: boolean;
  count: number;
  size: string;
  description: string;
};

export type VariableParamsLink = [PlotKey, TagKey];

export type VariableKey = string;
export type VariableSourceKey = string;
export type VariableParamsSource = {
  id: VariableSourceKey;
  metric: string;
  tag: TagKey;
  filterIn: FilterTag;
  filterNotIn: FilterTag;
};
export type VariableParams = {
  id: VariableKey;
  name: string;
  description: string;
  values: string[];
  link: VariableParamsLink[];
  groupBy: boolean;
  negative: boolean;
  source: Partial<Record<VariableSourceKey, VariableParamsSource>>;
  sourceOrder: VariableSourceKey[];
};
export type FilterTag = Partial<Record<TagKey, string[]>>;
export type PlotParams = {
  id: PlotKey;
  metricName: string;
  promQL: string;
  metricUnit?: MetricType;
  type: PlotType;
  what: QueryWhat[];
  customAgg: number;
  customName: string;
  customDescription: string;
  groupBy: TagKey[];
  filterIn: FilterTag;
  filterNotIn: FilterTag;
  numSeries: number;
  backendVersion: MetricValueBackendVersion;
  yLock: {
    min: number;
    max: number;
  };
  maxHost: boolean;
  events: PlotKey[];
  eventsBy: TagKey[];
  eventsHide: TagKey[];
  totalLine: boolean;
  filledGraph: boolean;
  timeShifts: number[];
  prometheusCompat: boolean;
};
export type TimeRange = {
  to: number;
  urlTo: number | TimeRangeKeysTo;
  from: number;
  absolute: boolean;
  now: number;
};

export type QueryParams = {
  live: boolean;
  theme?: string;
  dashboardId?: string;
  dashboardName: string;
  dashboardDescription: string;
  dashboardVersion?: number;
  timeRange: TimeRange;
  eventFrom: number;
  timeShifts: number[];
  tabNum: string;
  plots: Partial<Record<PlotKey, PlotParams>>;
  orderPlot: PlotKey[];
  variables: Partial<Record<VariableKey, VariableParams>>;
  orderVariables: VariableKey[];
  groups: Partial<Record<GroupKey, GroupInfo>>;
  orderGroup: GroupKey[];
};

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import {
  type MetricType,
  MetricValueBackendVersion,
  type PlotType,
  type QueryWhat,
  type TagKey,
  type TimeRangeKeysTo,
} from '@/api/enum';

export type PlotKey = string;

export type GroupKey = string;

export type GroupAutoSize = 'l' | 'm' | 's';
export type GroupSize = '1' | '2' | '3' | '4' | '6' | GroupAutoSize;

export type GroupInfo = {
  id: GroupKey;
  name: string;
  show: boolean;
  /**
   * @deprecated
   */
  count?: number;
  /**
   * @deprecated
   */
  size?: string;
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

export type LayoutInfo = {
  x: number;
  y: number;
  h: number;
  w: number;
};

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
  logScale: boolean;
  timeShifts: number[];
  prometheusCompat: boolean;
  group?: GroupKey;
  layout?: LayoutInfo;
};

export type TimeRange = {
  to: number;
  urlTo: number | TimeRangeKeysTo;
  from: number;
  absolute: boolean;
  now: number;
};

export type QueryParams = {
  version?: string;
  live: boolean;
  theme?: string;
  dashboardId?: string;
  dashboardName: string;
  dashboardDescription: string;
  dashboardVersion?: number;
  dashboardCurrentVersion?: number;
  timeRange: TimeRange;
  eventFrom: number;
  timeShifts: number[];
  tabNum: string;
  plots: Partial<Record<PlotKey, PlotParams>>;
  /** @deprecated */
  orderPlot?: PlotKey[];
  variables: Partial<Record<VariableKey, VariableParams>>;
  orderVariables: VariableKey[];
  groups: Partial<Record<GroupKey, GroupInfo>>;
  orderGroup: GroupKey[];
};

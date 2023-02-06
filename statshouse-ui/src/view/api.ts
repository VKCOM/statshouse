// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import * as utils from './utils';
import { convert } from './utils';
import { TimeRange } from '../common/TimeRange';

export interface lockRange {
  readonly min: number;
  readonly max: number;
}

export interface querySelector {
  readonly metricName: string;
  readonly what: queryWhat[];
  readonly customAgg: number;
  readonly groupBy: readonly string[];
  readonly filterIn: Readonly<Record<string, readonly string[]>>;
  readonly filterNotIn: Readonly<Record<string, readonly string[]>>;
  readonly numSeries: number;
  readonly timeShifts: readonly number[];
  readonly useV2: boolean;
  readonly yLock: lockRange;
}

export interface queryResult {
  readonly series: querySeries;
  readonly receive_errors: number;
  readonly receive_errors_legacy: number;
  readonly sampling_factor_src: number;
  readonly sampling_factor_agg: number;
  readonly mapping_errors: number;
  readonly mapping_flood_events_legacy: number;
}

export interface querySeries {
  readonly time: readonly number[];
  readonly series_meta: readonly querySeriesMeta[];
  readonly series_data: readonly ReadonlyArray<number | null>[];
}

export interface querySeriesMeta {
  readonly time_shift: number;
  readonly tags: Readonly<Record<string, querySeriesMetaTag>>;
  readonly max_hosts: null | string[];
  readonly what: queryWhat;
  readonly total: number;
}

export interface querySeriesMetaTag {
  readonly value: string;
  readonly comment?: string;
  readonly raw?: boolean;
  readonly raw_kind?: RawValueKind;
}

export type dashboardShortInfo = {
  id: number;
  name: string;
  description: string;
};

export type GetDashboardListResp = {
  dashboards: dashboardShortInfo[] | null;
};

export interface DashboardInfo {
  dashboard: DashboardMeta;
  delete_mark?: boolean;
}

export interface DashboardMeta {
  dashboard_id?: number;
  name: string;
  description: string;
  version?: number;
  update_time?: number;
  deleted_time?: number;
  data: { [key: string]: unknown };
}

export interface MetricsGroupInfo {
  group: MetricsGroup;
  metrics: string[] | null;
  delete_mark?: boolean;
}

export interface MetricsGroup {
  group_id?: number;
  name: string;
  version?: number;
  update_time?: number;
  weight: number;
}

export interface MetricsGroupInfoList {
  groups: MetricsGroupShort[];
}

export interface MetricsGroupShort {
  id: number;
  name: string;
  weight?: number;
}

export interface PromConfigInfo {
  config: string;
  version: number;
}

// XXX: keep in sync with Go
export function formatTagValue(value: string, comment?: string, raw?: boolean, kind?: RawValueKind): string {
  if (comment) {
    return comment;
  }

  if (value.length < 1 || value[0] !== ' ') {
    return value;
  }
  if (raw && kind) {
    return `⚡ ${convert(kind, parseInt(value))}`;
  }
  const i = parseInt(value.substring(1));
  if (i === 0 && !raw) {
    return '⚡ empty';
  }
  if (i === -1 && !raw) {
    return '⚡ mapping flood';
  }
  return `⚡ ${i}`;
}

export function metaToBaseLabel(meta: querySeriesMeta, uniqueWhatLength: number): string {
  let desc =
    Object.entries(meta.tags)
      .sort(([a], [b]) => (a > b ? 1 : a < b ? -1 : 0))
      .map(([, metaTag]) => formatTagValue(metaTag.value, metaTag.comment, metaTag.raw, metaTag.raw_kind))
      .join(', ') || 'Value';
  if (uniqueWhatLength > 1) {
    desc = `${desc}: ${whatToWhatDesc(meta.what)}`;
  }
  return desc;
}

// XXX: keep in sync with Go
export function metaToLabel(meta: querySeriesMeta, uniqueWhatLength: number): string {
  const desc = metaToBaseLabel(meta, uniqueWhatLength);
  const tsd = utils.timeShiftDesc(meta.time_shift);
  return tsd !== '' ? `${tsd} ${desc}` : desc;
}

export type queryWhat =
  | '-'
  | 'count'
  | 'count_norm'
  | 'cu_count'
  | 'cardinality'
  | 'cardinality_norm'
  | 'cu_cardinality'
  | 'min'
  | 'max'
  | 'avg'
  | 'cu_avg'
  | 'sum'
  | 'sum_norm'
  | 'cu_sum'
  | 'stddev'
  | 'max_host'
  | 'max_count_host'
  | 'p25'
  | 'p50'
  | 'p75'
  | 'p90'
  | 'p95'
  | 'p99'
  | 'p999'
  | 'unique'
  | 'unique_norm'
  | 'dv_count'
  | 'dv_count_norm'
  | 'dv_sum'
  | 'dv_sum_norm'
  | 'dv_avg'
  | 'dv_min'
  | 'dv_max'
  | 'dv_unique'
  | 'dv_unique_norm';

export function metricKindToWhat(kind: metricKind): queryWhat[] {
  switch (kind) {
    case 'counter':
      return [
        'count_norm',
        'count',
        'cu_count',
        'max_count_host',
        'dv_count',
        'dv_count_norm',
        '-',
        'cardinality_norm',
        'cardinality',
        'cu_cardinality',
      ];
    case 'value':
      return [
        'avg',
        'min',
        'max',
        'sum_norm',
        'sum',
        'stddev',
        'count_norm',
        'count',
        '-',
        'cu_avg',
        'cu_sum',
        'cu_count',
        '-',
        'max_host',
        '-',
        'dv_count',
        'dv_count_norm',
        'dv_sum',
        'dv_sum_norm',
        'dv_avg',
        'dv_min',
        'dv_max',
        '-',
        'cardinality_norm',
        'cardinality',
        'cu_cardinality',
      ];
    case 'value_p':
      return [
        'avg',
        'min',
        'max',
        'sum_norm',
        'sum',
        'stddev',
        'count_norm',
        'count',
        '-',
        'cu_avg',
        'cu_sum',
        'cu_count',
        '-',
        'p25',
        'p50',
        'p75',
        'p90',
        'p95',
        'p99',
        'p999',
        '-',
        'max_host',
        '-',
        'dv_count',
        'dv_count_norm',
        'dv_sum',
        'dv_sum_norm',
        'dv_avg',
        'dv_min',
        'dv_max',
        '-',
        'cardinality_norm',
        'cardinality',
        'cu_cardinality',
      ];
    case 'unique':
      return [
        'unique_norm',
        'unique',
        'count_norm',
        'count',
        '-',
        'cu_count',
        '-',
        'avg',
        'min',
        'max',
        'stddev',
        '-',
        'max_count_host',
        '-',
        'dv_count',
        'dv_count_norm',
        'dv_avg',
        'dv_min',
        'dv_max',
        'dv_unique',
        'dv_unique_norm',
        '-',
        'cardinality_norm',
        'cardinality',
        'cu_cardinality',
      ];
    case 'mixed':
      return [
        'count_norm',
        'count',
        'avg',
        'min',
        'max',
        'sum_norm',
        'sum',
        'stddev',
        'unique_norm',
        'unique',
        '-',
        'cu_count',
        'cu_avg',
        'cu_sum',
        '-',
        'max_host',
        'max_count_host',
        '-',
        'dv_count',
        'dv_count_norm',
        'dv_sum',
        'dv_sum_norm',
        'dv_avg',
        'dv_unique',
        'dv_unique_norm',
        'dv_min',
        'dv_max',
        '-',
        'cardinality_norm',
        'cardinality',
        'cu_cardinality',
      ];
    case 'mixed_p':
      return [
        'count_norm',
        'count',
        'avg',
        'min',
        'max',
        'sum_norm',
        'sum',
        'stddev',
        'unique_norm',
        'unique',
        '-',
        'cu_count',
        'cu_avg',
        'cu_sum',
        '-',
        'p25',
        'p50',
        'p75',
        'p90',
        'p95',
        'p99',
        'p999',
        '-',
        'max_host',
        'max_count_host',
        '-',
        'dv_count',
        'dv_count_norm',
        'dv_sum',
        'dv_sum_norm',
        'dv_avg',
        'dv_min',
        'dv_max',
        'dv_unique',
        'dv_unique_norm',
        '-',
        'cardinality_norm',
        'cardinality',
        'cu_cardinality',
      ];
    default:
      return [];
  }
}

// XXX: keep in sync with Go
export function whatToWhatDesc(what: queryWhat): string {
  switch (what) {
    case 'p999':
      return 'p99.9';
    case 'count_norm':
      return 'count/sec';
    case 'cu_count':
      return 'count (cumul)';
    case 'cardinality_norm':
      return 'cardinality/sec';
    case 'cu_cardinality':
      return 'cardinality (cumul)';
    case 'cu_avg':
      return 'avg (cumul)';
    case 'sum_norm':
      return 'sum/sec';
    case 'cu_sum':
      return 'sum (cumul)';
    case 'unique_norm':
      return 'unique/sec';
    case 'max_count_host':
      return 'max(count)@host';
    case 'max_host':
      return 'max(value)@host';
    case 'dv_count':
      return 'count (derivative)';
    case 'dv_sum':
      return 'sum (derivative)';
    case 'dv_avg':
      return 'avg (derivative)';
    case 'dv_count_norm':
      return 'count/sec (derivative)';
    case 'dv_sum_norm':
      return 'sum/sec (derivative)';
    case 'dv_min':
      return 'min (derivative)';
    case 'dv_max':
      return 'max (derivative)';
    case 'dv_unique':
      return 'unique (derivative)';
    case 'dv_unique_norm':
      return 'unique/sec (derivative)';
    default:
      return what as string;
  }
}

export const queryParamNumResults = 'n';
export const queryParamBackendVersion = 'v';
export const queryParamMetric = 's';
export const queryParamFromTime = 'f';
export const queryParamToTime = 't';
export const queryParamWidth = 'w';
export const queryParamWhat = 'qw';
export const queryParamTimeShifts = 'ts';
export const queryParamGroupBy = 'qb';
export const queryParamFilter = 'qf';
export const queryParamFilterSync = 'fs';
export const queryParamVerbose = 'qv';
export const queryParamTagID = 'k';
export const queryParamDownloadFile = 'df';
export const queryParamTabNum = 'tn';
export const queryParamLockMin = 'yl';
export const queryParamLockMax = 'yh';
export const queryParamAgg = 'g';
export const tabPrefix = 't';
export const queryDashboardID = 'id';
export const queryMetricsGroupID = 'id';
export const queryDashboardGroupInfoPrefix = 'g';
export const queryDashboardGroupInfoName = 't';
export const queryDashboardGroupInfoShow = 'v';
export const queryDashboardGroupInfoCount = 'n';

export const queryValueBackendVersion1 = '1';
export const queryValueBackendVersion2 = '2';

export const queryParamLive = 'live';

export function v2Value(useV2: boolean): string {
  return useV2 ? queryValueBackendVersion2 : queryValueBackendVersion1;
}

export function queryURL(
  sel: querySelector,
  timeRange: TimeRange,
  width: number | string,
  fetchBadges: boolean
): string {
  const params = [
    [queryParamNumResults, sel.numSeries.toString()],
    [queryParamBackendVersion, v2Value(sel.useV2)],
    [queryParamMetric, sel.metricName],
    [queryParamFromTime, timeRange.from.toString()],
    [queryParamToTime, (timeRange.to + 1).toString()],
    [queryParamWidth, width.toString()],
    ...sel.what.map((qw) => [queryParamWhat, qw.toString()]),
    // [queryParamWhat, sel.what.map((qw) => qw.toString())],
    [queryParamVerbose, fetchBadges ? '1' : '0'],
    ...sel.timeShifts.map((ts) => [queryParamTimeShifts, ts.toString()]),
    ...sel.groupBy.map((b) => [queryParamGroupBy, b]),
    ...filterParams(sel.filterIn, sel.filterNotIn),
  ];

  const strParams = new URLSearchParams(params).toString();
  return `/api/query?${strParams}`;
}

export function queryURLCSV(sel: querySelector, timeRange: TimeRange, width: number | string): string {
  const params = [
    [queryParamNumResults, sel.numSeries.toString()],
    [queryParamBackendVersion, v2Value(sel.useV2)],
    [queryParamMetric, sel.metricName],
    [queryParamFromTime, timeRange.from.toString()],
    [queryParamToTime, (timeRange.to + 1).toString()],
    [queryParamWidth, width.toString()],
    ...sel.what.map((qw) => [queryParamWhat, qw.toString()]),
    // [queryParamWhat, sel.what],
    ...sel.timeShifts.map((ts) => [queryParamTimeShifts, ts.toString()]),
    ...sel.groupBy.map((b) => [queryParamGroupBy, b]),
    ...filterParams(sel.filterIn, sel.filterNotIn),
    [queryParamDownloadFile, 'csv'],
  ];

  const strParams = new URLSearchParams(params).toString();
  return `/api/query?${strParams}`;
}

export interface metricsListResult {
  readonly metrics: readonly metricShortMeta[];
}

export interface metricShortMeta {
  readonly name: string;
}

export function metricsListURL(): string {
  return '/api/metrics-list';
}

export function dashboardURL(id?: number): string {
  if (!id) {
    return `/api/dashboard`;
  }

  const strParams = new URLSearchParams([[queryDashboardID, id.toString()]]).toString();
  return `/api/dashboard?${strParams}`;
}

export function dashboardListURL(): string {
  return '/api/dashboards-list';
}

export function metricsGroupListURL(): string {
  return '/api/group-list';
}

export function metricsGroupURL(id?: number): string {
  if (!id) {
    return `/api/group`;
  }

  const strParams = new URLSearchParams([[queryMetricsGroupID, id.toString()]]).toString();
  return `/api/group?${strParams}`;
}

export function promConfigURL(): string {
  return '/api/prometheus';
}

export interface metricResult {
  readonly metric: metricMeta;
}

export type metricKind = 'counter' | 'value' | 'value_p' | 'unique' | 'mixed' | 'mixed_p';

/**
 * uint:            interpret number bits as uint32, print as decimal number
 * ip:              167901850 (0xA01FA9A) -> 10.1.250.154, interpret number bits as uint32, high byte contains first element of IP address, lower byte contains last element of IP address
 * ip_bswap:        same as ip, but do bswap after interpreting number bits as uint32
 * hex:             interpret number bits as uint32, print as hex number, do not omit leading 000
 * hex_bswap:       same as hex, but do bswap after interpreting number bits as uint32
 * timestamp:       UNIX timestamp, show as is (in GMT)
 * timestamp_local: UNIX timestamp, show local time for this TS
 * EMPTY:           decimal number, can be negative
 */
export type RawValueKind =
  | 'uint'
  | 'hex'
  | 'hex_bswap'
  | 'timestamp'
  | 'timestamp_local'
  | 'ip'
  | 'ip_bswap'
  | 'lexenc_float';

export interface metricMeta {
  readonly name: string;
  readonly metric_id: number;
  readonly kind: metricKind;
  readonly description?: string;
  readonly tags?: readonly metricTag[];
  readonly string_top_name?: string;
  readonly string_top_description?: string;
  readonly resolution?: number;
  readonly pre_key_tag_id?: string;
  readonly pre_key_from?: number;
  readonly group_id?: number;
}

export interface metricTag {
  readonly name: string;
  readonly description?: string;
  readonly value_comments?: Readonly<Record<string, string>>;
  readonly raw?: boolean;
  readonly raw_kind?: RawValueKind;
}

export function metricURL(metric: string): string {
  const params = [[queryParamMetric, metric]];

  const strParams = new URLSearchParams(params).toString();
  return `/api/metric?${strParams}`;
}

export interface metricTagValuesResult {
  readonly tag_values: readonly metricTagValueInfo[];
  readonly tag_values_more?: boolean;
  readonly raw_kind?: RawValueKind;
}

export interface metricTagValueInfo {
  readonly value: string;
  readonly count: number;
}

export function metricTagValuesURL(
  numValues: number,
  useV2: boolean,
  metric: string,
  tagID: string,
  fromTime: number,
  toTime: number,
  what: queryWhat[],
  filterIn: Record<string, readonly string[]>,
  filterNotIn: Record<string, readonly string[]>
): string {
  const to = toTime <= 0 ? utils.now() + toTime : toTime;
  const from = fromTime <= 0 ? to + fromTime : fromTime;
  const params = [
    [queryParamNumResults, numValues.toString()],
    [queryParamBackendVersion, v2Value(useV2)],
    [queryParamMetric, metric],
    [queryParamTagID, tagID],
    [queryParamFromTime, from.toString()],
    [queryParamToTime, (to + 1).toString()],
    // [queryParamWhat, what],
    ...what.map((qw) => [queryParamWhat, qw.toString()]),
    ...filterParams(filterIn, filterNotIn),
  ];

  const strParams = new URLSearchParams(params).toString();
  return `/api/metric-tag-values?${strParams}`;
}

export const filterInSep = '-';
export const filterNotInSep = '~';

export function formatFilterIn(tagID: string, tagValue: string, full?: boolean): string {
  const indexTag = full ? tagID : tagID.replace('key', '').replace('s', '_s');
  return `${indexTag}${filterInSep}${tagValue}`;
}

export function formatFilterNotIn(tagID: string, tagValue: string, full?: boolean): string {
  const indexTag = full ? tagID : tagID.replace('key', '').replace('s', '_s');
  return `${indexTag}${filterNotInSep}${tagValue}`;
}

export function flattenFilterIn(filterIn: Record<string, readonly string[]>): string[] {
  return Object.entries(filterIn).flatMap(([tagID, tagValues]) => tagValues.map((v) => formatFilterIn(tagID, v)));
}

export function flattenFilterNotIn(filterNotIn: Record<string, readonly string[]>): string[] {
  return Object.entries(filterNotIn).flatMap(([tagID, tagValues]) => tagValues.map((v) => formatFilterNotIn(tagID, v)));
}

function filterParams(
  filterIn: Record<string, readonly string[]>,
  filterNotIn: Record<string, readonly string[]>
): string[][] {
  const paramsIn = Object.entries(filterIn).flatMap(([tagID, tagValues]) =>
    tagValues.map((v) => [queryParamFilter, formatFilterIn(tagID, v, true)])
  );
  const paramsNotIn = Object.entries(filterNotIn).flatMap(([tagID, tagValues]) =>
    tagValues.map((v) => [queryParamFilter, formatFilterNotIn(tagID, v, true)])
  );
  return [...paramsIn, ...paramsNotIn];
}

export function filterHasTagID(sel: querySelector, indexTag: number): boolean {
  const tagID = indexTag === -1 ? 'skey' : `key${indexTag}`;
  return (
    (sel.filterIn[tagID] !== undefined && sel.filterIn[tagID].length !== 0) ||
    (sel.filterNotIn[tagID] !== undefined && sel.filterNotIn[tagID].length !== 0) ||
    sel.groupBy.indexOf(tagID) >= 0
  );
}

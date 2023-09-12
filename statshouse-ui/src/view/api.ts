// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { convert, promQLMetric, timeShiftDesc } from './utils';
import { TimeRange } from '../common/TimeRange';
import { Column } from 'react-data-grid';
import { EventDataRow } from '../store';
import {
  EventFormatterData,
  EventFormatterDefault,
  EventFormatterHeaderDefault,
  EventFormatterHeaderTime,
} from '../components/Plot/EventFormatters';
import { uniqueArray } from '../common/helpers';
import { GET_PARAMS, METRIC_VALUE_BACKEND_VERSION, QueryWhat, QueryWhatSelector, TagKey } from '../api/enum';
import {
  encodeVariableConfig,
  encodeVariableValues,
  filterInSep,
  filterNotInSep,
  freeKeyPrefix,
  PlotParams,
  QueryParams,
  toIndexTag,
} from '../url/queryParams';
import { MetricMetaValue } from '../api/metric';

export interface queryResult {
  readonly series: querySeries;
  readonly receive_errors: number;
  readonly receive_warnings: number;
  readonly receive_errors_legacy: number;
  readonly sampling_factor_src: number;
  readonly sampling_factor_agg: number;
  readonly mapping_errors: number;
  readonly mapping_flood_events_legacy: number;
  readonly promqltestfailed?: boolean; // only dev param
  readonly promql: string;
  readonly metric: MetricMetaValue | null;
}

export interface querySeries {
  readonly time: readonly number[];
  readonly series_meta: readonly querySeriesMeta[];
  readonly series_data: readonly ReadonlyArray<number | null>[];
}

export interface querySeriesMeta {
  readonly name?: string;
  readonly time_shift: number;
  readonly tags: Readonly<Record<string, querySeriesMetaTag>>;
  readonly max_hosts: null | string[];
  readonly what: QueryWhat;
  readonly total: number;
  readonly color: string;
}

export interface querySeriesMetaTag {
  readonly value: string;
  readonly comment?: string;
  readonly raw?: boolean;
  readonly raw_kind?: RawValueKind;
}

export interface DashboardInfo {
  dashboard: DashboardMeta;
  delete_mark?: boolean;
}

export type queryTableRow = {
  time: number;
  data: number[];
  tags: Record<string, querySeriesMetaTag>;
  what?: QueryWhat;
};

export type queryTable = {
  rows: queryTableRow[] | null;
  from_row: string;
  to_row: string;
  more: boolean;
  what: QueryWhat[];
};

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

export const eventColumnDefault: Readonly<Partial<Column<EventDataRow>>> = {
  minWidth: 20,
  maxWidth: 300,
  resizable: true,
  headerRenderer: EventFormatterHeaderDefault,
  width: 'auto',
  formatter: EventFormatterDefault,
  // sortable: true,
  // headerCellClass: 'no-Focus',
};
export const getEventColumnsType = (what: string[] = []): Record<string, Column<EventDataRow>> => ({
  timeString: { key: 'timeString', name: 'Time', width: 165, headerRenderer: EventFormatterHeaderTime },
  ...Object.fromEntries(what.map((key) => [key, { key, name: whatToWhatDesc(key), formatter: EventFormatterData }])),
});

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

export function sortTagEntries([a]: [string, querySeriesMetaTag], [b]: [string, querySeriesMetaTag]) {
  let a0 = toIndexTag(a) ?? 0;
  let b0 = toIndexTag(b) ?? 0;
  if (a0 > 0) {
    a0 += 999999;
  }
  if (b0 > 0) {
    b0 += 999999;
  }
  return a0 - b0;
}

export function metaToBaseLabel(meta: querySeriesMeta, uniqueWhatLength: number): string {
  let desc =
    Object.entries(meta.tags)
      .sort(sortTagEntries)
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
  const tsd = timeShiftDesc(meta.time_shift);
  return tsd !== '' ? `${tsd} ${desc}` : desc;
}

export function metricKindToWhat(kind?: metricKind): QueryWhatSelector[] {
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
        'p0_1',
        'p1',
        'p5',
        'p10',
        'p25',
        'p50',
        'p75',
        'p90',
        'p95',
        'p99',
        'p999',
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
        'p0_1',
        'p1',
        'p5',
        'p10',
        'p25',
        'p50',
        'p75',
        'p90',
        'p95',
        'p99',
        'p999',
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
      return ['count_norm'];
  }
}

// XXX: keep in sync with Go
export function whatToWhatDesc(what: QueryWhat | string): string {
  switch (what) {
    case 'p0_1':
      return 'p0.1';
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

export function v2Value(useV2: boolean): string {
  return useV2 ? METRIC_VALUE_BACKEND_VERSION.v2 : METRIC_VALUE_BACKEND_VERSION.v1;
}

export function queryURL(
  sel: PlotParams,
  timeRange: TimeRange,
  timeShifts: number[],
  width: number | string,
  fetchBadges: boolean,
  allParams?: QueryParams
): string {
  let params: string[][];
  if (sel.metricName === promQLMetric) {
    params = [
      [GET_PARAMS.fromTime, timeRange.from.toString()],
      [GET_PARAMS.toTime, (timeRange.to + 1).toString()],
      [GET_PARAMS.width, width.toString()],
      ...timeShifts.map((ts) => [GET_PARAMS.metricTimeShifts, ts.toString()]),
    ];
    if (allParams) {
      params.push(...encodeVariableValues(allParams));
      params.push(...encodeVariableConfig(allParams));
    }
  } else {
    params = [
      [GET_PARAMS.numResults, sel.numSeries.toString()],
      [GET_PARAMS.version, v2Value(sel.useV2)],
      [GET_PARAMS.metricName, sel.metricName],
      [GET_PARAMS.fromTime, timeRange.from.toString()],
      [GET_PARAMS.toTime, (timeRange.to + 1).toString()],
      [GET_PARAMS.width, width.toString()],
      ...sel.what.map((qw) => [GET_PARAMS.metricWhat, qw.toString()]),
      ...timeShifts.map((ts) => [GET_PARAMS.metricTimeShifts, ts.toString()]),
      ...sel.groupBy.map((b) => [GET_PARAMS.metricGroupBy, freeKeyPrefix(b)]),
      ...filterParams(sel.filterIn, sel.filterNotIn),
    ];
  }

  if (sel.maxHost) {
    params.push([GET_PARAMS.metricMaxHost, '1']);
  }
  params.push([GET_PARAMS.excessPoints, '1']);
  params.push([GET_PARAMS.metricVerbose, fetchBadges ? '1' : '0']);
  const strParams = new URLSearchParams(params).toString();
  return `/api/query?${strParams}`;
}

export function queryURLCSV(
  sel: PlotParams,
  timeRange: TimeRange,
  timeShifts: number[],
  width: number | string,
  allParams?: QueryParams
): string {
  let params: string[][];
  if (sel.metricName === promQLMetric) {
    params = [
      [GET_PARAMS.fromTime, timeRange.from.toString()],
      [GET_PARAMS.toTime, (timeRange.to + 1).toString()],
      [GET_PARAMS.width, width.toString()],
      [GET_PARAMS.metricPromQL, sel.promQL],
      ...timeShifts.map((ts) => [GET_PARAMS.metricTimeShifts, ts.toString()]),
      [GET_PARAMS.metricDownloadFile, 'csv'],
    ];
    if (allParams) {
      params.push(...encodeVariableValues(allParams));
      params.push(...encodeVariableConfig(allParams));
    }
  } else {
    params = [
      [GET_PARAMS.numResults, sel.numSeries.toString()],
      [GET_PARAMS.version, v2Value(sel.useV2)],
      [GET_PARAMS.metricName, sel.metricName],
      [GET_PARAMS.fromTime, timeRange.from.toString()],
      [GET_PARAMS.toTime, (timeRange.to + 1).toString()],
      [GET_PARAMS.width, width.toString()],
      ...sel.what.map((qw) => [GET_PARAMS.metricWhat, qw.toString()]),
      ...timeShifts.map((ts) => [GET_PARAMS.metricTimeShifts, ts.toString()]),
      ...sel.groupBy.map((b) => [GET_PARAMS.metricGroupBy, b]),
      ...filterParams(sel.filterIn, sel.filterNotIn),
      [GET_PARAMS.metricDownloadFile, 'csv'],
    ];
  }
  if (sel.maxHost) {
    params.push([GET_PARAMS.metricMaxHost, '1']);
  }

  const strParams = new URLSearchParams(params).toString();
  return `/api/query?${strParams}`;
}

export function queryTableURL(
  sel: PlotParams,
  timeRange: TimeRange,
  width: number | string,
  key?: string,
  fromEnd: boolean = false,
  limit: number = 1000
): string {
  let params: string[][];
  if (sel.metricName === promQLMetric) {
    params = [
      [GET_PARAMS.fromTime, timeRange.from.toString()],
      [GET_PARAMS.toTime, (timeRange.to + 1).toString()],
      [GET_PARAMS.width, width.toString()],
      // ...timeShifts.map((ts) => [queryParamTimeShifts, ts.toString()]),
    ];
  } else {
    params = [
      [GET_PARAMS.version, v2Value(sel.useV2)],
      [GET_PARAMS.metricName, sel.metricName],
      [GET_PARAMS.fromTime, timeRange.from.toString()],
      [GET_PARAMS.toTime, (timeRange.to + 1).toString()],
      [GET_PARAMS.width, width.toString()],
      ...sel.what.map((qw) => [GET_PARAMS.metricWhat, qw.toString()]),
      // [queryParamVerbose, fetchBadges ? '1' : '0'],
      // ...timeShifts.map((ts) => [queryParamTimeShifts, ts.toString()]),
      // ...sel.groupBy.map((b) => [queryParamGroupBy, freeKeyPrefix(b)]),
      ...uniqueArray([...sel.groupBy.map(freeKeyPrefix), ...sel.eventsBy]).map((b) => [GET_PARAMS.metricGroupBy, b]),
      ...filterParams(sel.filterIn, sel.filterNotIn),
    ];
  }
  if (sel.maxHost) {
    params.push([GET_PARAMS.metricMaxHost, '1']);
  }
  if (fromEnd) {
    params.push([GET_PARAMS.metricFromEnd, '1']);
  }
  if (key) {
    if (fromEnd) {
      params.push([GET_PARAMS.metricToRow, key]);
    } else {
      params.push([GET_PARAMS.metricFromRow, key]);
    }
  }
  params.push([GET_PARAMS.numResults, limit.toString()]);

  const strParams = new URLSearchParams(params).toString();
  return `/api/table?${strParams}`;
}

export function dashboardURL(id?: number): string {
  if (!id) {
    return `/api/dashboard`;
  }

  const strParams = new URLSearchParams([[GET_PARAMS.dashboardID, id.toString()]]).toString();
  return `/api/dashboard?${strParams}`;
}

export function metricsGroupListURL(): string {
  return '/api/group-list';
}

export function metricsGroupURL(id?: number): string {
  if (!id) {
    return `/api/group`;
  }

  const strParams = new URLSearchParams([[GET_PARAMS.metricsGroupID, id.toString()]]).toString();
  return `/api/group?${strParams}`;
}

export function promConfigURL(): string {
  return '/api/prometheus';
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
  | 'lexenc_float'
  | 'float';

export interface metricTag {
  readonly name: string;
  readonly description?: string;
  readonly value_comments?: Readonly<Record<string, string>>;
  readonly raw?: boolean;
  readonly raw_kind?: RawValueKind;
}

export interface metricTagValueInfo {
  readonly value: string;
  readonly count: number;
}

export function formatFilterIn(tagID: string, tagValue: string): string {
  return `${freeKeyPrefix(tagID)}${filterInSep}${tagValue}`;
}

export function formatFilterNotIn(tagID: string, tagValue: string): string {
  return `${freeKeyPrefix(tagID)}${filterNotInSep}${tagValue}`;
}

function filterParams(
  filterIn: Record<string, readonly string[]>,
  filterNotIn: Record<string, readonly string[]>
): string[][] {
  const paramsIn = Object.entries(filterIn).flatMap(([tagID, tagValues]) =>
    tagValues.map((v) => [GET_PARAMS.metricFilter, formatFilterIn(tagID, v)])
  );
  const paramsNotIn = Object.entries(filterNotIn).flatMap(([tagID, tagValues]) =>
    tagValues.map((v) => [GET_PARAMS.metricFilter, formatFilterNotIn(tagID, v)])
  );
  return [...paramsIn, ...paramsNotIn];
}
export function filterParamsArr(
  filterIn: Record<string, readonly string[]>,
  filterNotIn: Record<string, readonly string[]>
): string[] {
  const paramsIn = Object.entries(filterIn).flatMap(([tagID, tagValues]) =>
    tagValues.map((v) => formatFilterIn(tagID, v))
  );
  const paramsNotIn = Object.entries(filterNotIn).flatMap(([tagID, tagValues]) =>
    tagValues.map((v) => formatFilterNotIn(tagID, v))
  );
  return [...paramsIn, ...paramsNotIn];
}

export function filterHasTagID(sel: PlotParams, tagKey: TagKey): boolean {
  return (
    (sel.filterIn[tagKey] !== undefined && sel.filterIn[tagKey]?.length !== 0) ||
    (sel.filterNotIn[tagKey] !== undefined && sel.filterNotIn[tagKey]?.length !== 0) ||
    sel.groupBy.indexOf(tagKey) >= 0
  );
}

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { filterInSep, filterNotInSep, freeKeyPrefix, PlotParams, toIndexTag } from '@/url/queryParams';
import { MetricMetaValue } from '@/api/metric';
import {
  GET_PARAMS,
  METRIC_VALUE_BACKEND_VERSION,
  QUERY_WHAT,
  type QueryWhat,
  type QueryWhatSelector,
  type TagKey,
} from '@/api/enum';
import { whatToWhatDesc } from './whatToWhatDesc';
import { convert, timeShiftDesc } from './utils2';

export interface queryResult {
  readonly series: querySeries;
  readonly receive_errors: number;
  readonly receive_warnings: number;
  readonly sampling_factor_src: number;
  readonly sampling_factor_agg: number;
  readonly mapping_errors: number;
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
  readonly metric_type?: string;
}

export interface querySeriesMetaTag {
  readonly value: string;
  readonly comment?: string;
  readonly raw?: boolean;
  readonly raw_kind?: RawValueKind;
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
  raw = raw || kind != null; // fix raw deprecated
  if (raw || kind != null) {
    return `⚡ ${convert(kind, value)}`;
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
  return String(desc);
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
        QUERY_WHAT.countNorm,
        QUERY_WHAT.count,
        QUERY_WHAT.cuCount,
        QUERY_WHAT.maxCountHost,
        QUERY_WHAT.dvCount,
        QUERY_WHAT.dvCountNorm,
        '-',
        QUERY_WHAT.cardinalityNorm,
        QUERY_WHAT.cardinality,
        QUERY_WHAT.cuCardinality,
      ];
    case 'value':
      return [
        QUERY_WHAT.avg,
        QUERY_WHAT.min,
        QUERY_WHAT.max,
        QUERY_WHAT.sumNorm,
        QUERY_WHAT.sum,
        QUERY_WHAT.stddev,
        QUERY_WHAT.countNorm,
        QUERY_WHAT.count,
        '-',
        QUERY_WHAT.cuAvg,
        QUERY_WHAT.cuSum,
        QUERY_WHAT.cuCount,
        '-',
        QUERY_WHAT.dvCount,
        QUERY_WHAT.dvCountNorm,
        QUERY_WHAT.dvSum,
        QUERY_WHAT.dvSumNorm,
        QUERY_WHAT.dvAvg,
        QUERY_WHAT.dvMin,
        QUERY_WHAT.dvMax,
        '-',
        QUERY_WHAT.cardinalityNorm,
        QUERY_WHAT.cardinality,
        QUERY_WHAT.cuCardinality,
      ];
    case 'value_p':
      return [
        QUERY_WHAT.avg,
        QUERY_WHAT.min,
        QUERY_WHAT.max,
        QUERY_WHAT.sumNorm,
        QUERY_WHAT.sum,
        QUERY_WHAT.stddev,
        QUERY_WHAT.countNorm,
        QUERY_WHAT.count,
        '-',
        QUERY_WHAT.cuAvg,
        QUERY_WHAT.cuSum,
        QUERY_WHAT.cuCount,
        '-',
        QUERY_WHAT.p0_1,
        QUERY_WHAT.p1,
        QUERY_WHAT.p5,
        QUERY_WHAT.p10,
        QUERY_WHAT.p25,
        QUERY_WHAT.p50,
        QUERY_WHAT.p75,
        QUERY_WHAT.p90,
        QUERY_WHAT.p95,
        QUERY_WHAT.p99,
        QUERY_WHAT.p999,
        '-',
        QUERY_WHAT.dvCount,
        QUERY_WHAT.dvCountNorm,
        QUERY_WHAT.dvSum,
        QUERY_WHAT.dvSumNorm,
        QUERY_WHAT.dvAvg,
        QUERY_WHAT.dvMin,
        QUERY_WHAT.dvMax,
        '-',
        QUERY_WHAT.cardinalityNorm,
        QUERY_WHAT.cardinality,
        QUERY_WHAT.cuCardinality,
      ];
    case 'unique':
      return [
        QUERY_WHAT.uniqueNorm,
        QUERY_WHAT.unique,
        QUERY_WHAT.countNorm,
        QUERY_WHAT.count,
        '-',
        QUERY_WHAT.cuCount,
        '-',
        QUERY_WHAT.avg,
        QUERY_WHAT.min,
        QUERY_WHAT.max,
        QUERY_WHAT.stddev,
        '-',
        QUERY_WHAT.dvCount,
        QUERY_WHAT.dvCountNorm,
        QUERY_WHAT.dvAvg,
        QUERY_WHAT.dvMin,
        QUERY_WHAT.dvMax,
        QUERY_WHAT.dvUnique,
        QUERY_WHAT.dvUniqueNorm,
        '-',
        QUERY_WHAT.cardinalityNorm,
        QUERY_WHAT.cardinality,
        QUERY_WHAT.cuCardinality,
      ];
    case 'mixed':
      return [
        QUERY_WHAT.countNorm,
        QUERY_WHAT.count,
        QUERY_WHAT.avg,
        QUERY_WHAT.min,
        QUERY_WHAT.max,
        QUERY_WHAT.sumNorm,
        QUERY_WHAT.sum,
        QUERY_WHAT.stddev,
        QUERY_WHAT.uniqueNorm,
        QUERY_WHAT.unique,
        '-',
        QUERY_WHAT.cuAvg,
        QUERY_WHAT.cuSum,
        QUERY_WHAT.cuCount,
        '-',
        QUERY_WHAT.dvCount,
        QUERY_WHAT.dvCountNorm,
        QUERY_WHAT.dvSum,
        QUERY_WHAT.dvSumNorm,
        QUERY_WHAT.dvAvg,
        QUERY_WHAT.dvMin,
        QUERY_WHAT.dvMax,
        QUERY_WHAT.dvUnique,
        QUERY_WHAT.dvUniqueNorm,
        '-',
        QUERY_WHAT.cardinalityNorm,
        QUERY_WHAT.cardinality,
        QUERY_WHAT.cuCardinality,
      ];
    case 'mixed_p':
      return [
        QUERY_WHAT.countNorm,
        QUERY_WHAT.count,
        QUERY_WHAT.avg,
        QUERY_WHAT.min,
        QUERY_WHAT.max,
        QUERY_WHAT.sumNorm,
        QUERY_WHAT.sum,
        QUERY_WHAT.stddev,
        QUERY_WHAT.uniqueNorm,
        QUERY_WHAT.unique,
        '-',
        QUERY_WHAT.cuAvg,
        QUERY_WHAT.cuSum,
        QUERY_WHAT.cuCount,
        '-',
        QUERY_WHAT.p0_1,
        QUERY_WHAT.p1,
        QUERY_WHAT.p5,
        QUERY_WHAT.p10,
        QUERY_WHAT.p25,
        QUERY_WHAT.p50,
        QUERY_WHAT.p75,
        QUERY_WHAT.p90,
        QUERY_WHAT.p95,
        QUERY_WHAT.p99,
        QUERY_WHAT.p999,
        '-',
        QUERY_WHAT.dvCount,
        QUERY_WHAT.dvCountNorm,
        QUERY_WHAT.dvSum,
        QUERY_WHAT.dvSumNorm,
        QUERY_WHAT.dvAvg,
        QUERY_WHAT.dvMin,
        QUERY_WHAT.dvMax,
        QUERY_WHAT.dvUnique,
        QUERY_WHAT.dvUniqueNorm,
        '-',
        QUERY_WHAT.cardinalityNorm,
        QUERY_WHAT.cardinality,
        QUERY_WHAT.cuCardinality,
      ];
    default:
      return [QUERY_WHAT.countNorm];
  }
}

export function v2Value(useV2: boolean): string {
  return useV2 ? METRIC_VALUE_BACKEND_VERSION.v2 : METRIC_VALUE_BACKEND_VERSION.v1;
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
 * '', EMPTY:       default int32 decimal number, can be negative
 */
export type RawValueKind =
  | 'int'
  | 'uint'
  | 'hex'
  | 'hex_bswap'
  | 'timestamp'
  | 'timestamp_local'
  | 'ip'
  | 'ip_bswap'
  | 'lexenc_float'
  | 'int64'
  | 'uint64'
  | 'hex64'
  | 'hex64_bswap'
  /** @deprecated 'float' raw value kind */
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

export function filterParams(
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

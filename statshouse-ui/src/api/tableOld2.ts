// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_PARAMS, QueryWhat, TagKey } from './enum';
import { freeKeyPrefix, PlotParams, TimeRange } from '@/url2';
import { filterParams, formatTagValue, querySeriesMetaTag } from '@/view/api';
import { apiGet } from '@/view/utils';
import { uniqueArray } from '../common/helpers';
import { promQLMetric } from '../view/promQLMetric';
import { fmtInputDateTime } from '../view/utils2';

const abortControllers: Map<unknown, AbortController> = new Map();
export type WhatCollection = Record<QueryWhat, number>;

export type TagCollection = Record<TagKey, string>;

export type ApiTableRowNormalize = {
  key: string;
  idChunk: number;
  timeString: string;
  time: number;
  data: number[];
} & Partial<WhatCollection> &
  Partial<TagCollection>;

export function apiTableRowNormalize(plot: PlotParams, chunk: QueryTable): ApiTableRowNormalize[] {
  if (chunk.rows) {
    return chunk.rows?.map((row, index) => {
      const whatData = Object.fromEntries(plot.what.map((whatKey, indexWhat) => [whatKey, row.data[indexWhat]]));
      const tagData = Object.fromEntries(
        Object.entries(row.tags).map(([key, tag]) => [
          freeKeyPrefix(key),
          formatTagValue(tag.value, tag.comment, tag.raw, tag.raw_kind),
        ])
      );
      return {
        key: `${chunk.from_row}_${index}`,
        idChunk: 0,
        timeString: fmtInputDateTime(new Date(row.time * 1000)),
        data: row.data,
        time: row.time,
        ...whatData,
        ...tagData,
      };
    });
  }
  return [];
}
export type ApiTable = QueryTable & { rowsNormalize: ApiTableRowNormalize[] };

export type QueryTableRow = {
  time: number;
  data: number[];
  tags: Record<string, querySeriesMetaTag>;
  what?: QueryWhat;
};

export type QueryTable = {
  rows: QueryTableRow[] | null;
  from_row: string;
  to_row: string;
  more: boolean;
  what: QueryWhat[];
};

export async function apiTable(
  plot: PlotParams,
  range: TimeRange,
  agg: string,
  key?: string | undefined,
  fromEnd?: boolean,
  limit?: number,
  keyRequest?: unknown
): Promise<ApiTable> {
  // const agg =
  //   plot.customAgg === -1
  //     ? `${Math.floor(width / 4)}`
  //     : plot.customAgg === 0
  //     ? `${Math.floor(width * devicePixelRatio)}`
  //     : `${plot.customAgg}s`;
  // const agg = `${range.to - range.from}s`;

  const url = queryTableURL(plot, range, agg, key, fromEnd, limit);

  let controller;
  if (keyRequest instanceof AbortController) {
    controller = keyRequest;
  } else {
    controller = new AbortController();
  }
  if (keyRequest) {
    abortControllers.get(keyRequest)?.abort();
    abortControllers.set(keyRequest, controller);
  }

  const result = await apiGet<QueryTable>(url, controller.signal, true).finally(() => {
    if (keyRequest) {
      abortControllers.delete(keyRequest);
    }
  });

  return {
    ...result,
    rows:
      result.rows?.map(
        (value) =>
          ({
            ...value,
            tags:
              value.tags &&
              Object.fromEntries(
                Object.entries(value.tags).map(([tagKey, tagValue]) => [freeKeyPrefix(tagKey), tagValue])
              ),
          }) as QueryTableRow
      ) ?? null,
    rowsNormalize: apiTableRowNormalize(plot, result),
  };
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
      [GET_PARAMS.version, sel.backendVersion],
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

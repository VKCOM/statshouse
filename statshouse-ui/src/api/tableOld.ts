// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { formatTagValue, queryTable, queryTableRow, queryTableURL } from '../view/api';
import { TimeRange } from '../common/TimeRange';
import { apiGet } from '../view/utils';
import { QueryWhat, TagKey } from './enum';
import { freeKeyPrefix, PlotParams } from '../url/queryParams';
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

export function apiTableRowNormalize(plot: PlotParams, chunk: queryTable): ApiTableRowNormalize[] {
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
export type ApiTable = queryTable & { rowsNormalize: ApiTableRowNormalize[] };
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

  const result = await apiGet<queryTable>(url, controller.signal, true).finally(() => {
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
          }) as queryTableRow
      ) ?? null,
    rowsNormalize: apiTableRowNormalize(plot, result),
  };
}

import { formatTagValue, queryTable, queryTableURL, queryWhat } from '../view/api';
import { PlotParams } from '../common/plotQueryParams';
import { TimeRange } from '../common/TimeRange';
import { apiGet, fmtInputDateTime, freeKeyPrefix } from '../view/utils';

const abortControllers: Map<unknown, AbortController> = new Map();

export type TagKey =
  | 'skey'
  | 'key0'
  | 'key1'
  | 'key2'
  | 'key3'
  | 'key4'
  | 'key5'
  | 'key6'
  | 'key7'
  | 'key8'
  | 'key9'
  | 'key10'
  | 'key11'
  | 'key12'
  | 'key13'
  | 'key14'
  | 'key15'
  | '_s'
  | '0'
  | '1'
  | '2'
  | '3'
  | '4'
  | '5'
  | '6'
  | '7'
  | '8'
  | '9'
  | '10'
  | '11'
  | '12'
  | '13'
  | '14'
  | '15';

export type WhatCollection = Record<queryWhat, number>;

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
  width: number,
  key?: string | undefined,
  fromEnd?: boolean,
  limit?: number,
  keyRequest?: unknown
): Promise<ApiTable> {
  const agg =
    plot.customAgg === -1
      ? `${Math.floor(width / 4)}`
      : plot.customAgg === 0
      ? `${Math.floor(width * devicePixelRatio)}`
      : `${plot.customAgg}s`;

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

  return { ...result, rowsNormalize: apiTableRowNormalize(plot, result) };
}

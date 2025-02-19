// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { ReactNode } from 'react';
import { RenderCellProps, RenderHeaderCellProps } from 'react-data-grid';
import { formatTagValue, querySeriesMetaTag } from '@/view/api';
import { Tooltip } from '@/components/UI';
import { formatLegendValue } from '@/view/utils2';
import { EventDataRow } from '@/common/getEventColumnsType';

export function EventFormatterDefault({ row, column }: RenderCellProps<EventDataRow>): ReactNode {
  const tag = row?.[column.key] as querySeriesMetaTag | undefined | string | number;
  const value: string =
    (typeof tag === 'object' && tag !== null
      ? formatTagValue(tag.value, tag.comment, tag.raw, tag.raw_kind)
      : (tag?.toString() ?? '')) ?? '';
  return (
    <div className="text-truncate" title={value}>
      {value}
    </div>
  );
}

function isFormatValue(v: unknown): v is { value: number; formatValue: string } {
  return (
    v != null &&
    typeof v === 'object' &&
    Object.hasOwn(v, 'formatValue') &&
    typeof (v as { formatValue: unknown }).formatValue === 'string'
  );
}
export function EventFormatterData({ row, column }: RenderCellProps<EventDataRow>): ReactNode {
  const tag = row?.[column.key] as
    | querySeriesMetaTag
    | undefined
    | string
    | number
    | { value: number; formatValue: string };
  let value = '';
  let title = '';
  if (isFormatValue(tag)) {
    value = tag.formatValue;
    title = tag.value.toString();
  } else if (tag === null || typeof tag === 'number') {
    title = value = formatLegendValue(tag);
  }
  return (
    <Tooltip className="text-truncate" title={title}>
      {value}
    </Tooltip>
  );
}

export function EventFormatterHeaderDefault({ column }: RenderHeaderCellProps<EventDataRow>): ReactNode {
  return column.name;
}

export function EventFormatterHeaderTime({ column }: RenderHeaderCellProps<EventDataRow>): ReactNode {
  return <span className="ps-4">{column.name}</span>;
}

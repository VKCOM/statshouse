import React, { ReactNode } from 'react';
import { RenderCellProps, RenderHeaderCellProps } from 'react-data-grid';
import { EventDataRow } from '../../store';
import { formatTagValue, querySeriesMetaTag } from '../../view/api';
import { formatLegendValue } from '../../view/utils';

export function EventFormatterDefault({ row, column }: RenderCellProps<EventDataRow>): ReactNode {
  const tag = row?.[column.key] as querySeriesMetaTag | undefined | string | number;
  const value: string =
    (typeof tag === 'object' && tag !== null
      ? formatTagValue(tag.value, tag.comment, tag.raw, tag.raw_kind)
      : tag?.toString() ?? '') ?? '';
  return (
    <div className="text-truncate" title={value}>
      {value}
    </div>
  );
}
export function EventFormatterData({ row, column }: RenderCellProps<EventDataRow>): ReactNode {
  const tag = row?.[column.key] as querySeriesMetaTag | undefined | string | number;
  const value: string = tag === null || typeof tag === 'number' ? formatLegendValue(tag) : '';
  return (
    <div className="text-truncate" title={value}>
      {value}
    </div>
  );
}

export function EventFormatterHeaderDefault({ column }: RenderHeaderCellProps<EventDataRow>): ReactNode {
  return column.name;
}

export function EventFormatterHeaderTime({ column }: RenderHeaderCellProps<EventDataRow>): ReactNode {
  return <span className="ps-4">{column.name}</span>;
}

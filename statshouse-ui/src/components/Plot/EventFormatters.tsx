import React, { ReactNode } from 'react';
import { FormatterProps, HeaderRendererProps } from 'react-data-grid';
import { EventDataRow } from '../../store/statshouse';
import { formatTagValue, querySeriesMetaTag } from '../../view/api';

export function EventFormatterDefault({ row, column }: FormatterProps<EventDataRow>): ReactNode {
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

export function EventFormatterHeaderDefault({ column }: HeaderRendererProps<EventDataRow>): ReactNode {
  return column.name;
}

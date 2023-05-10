import React, { ReactNode } from 'react';
import { FormatterProps, HeaderRendererProps } from 'react-data-grid';
import { EventDataRow } from '../../store/statshouse';

export function EventFormatterDefault({ row, column }: FormatterProps<EventDataRow>): ReactNode {
  const value: string =
    (typeof row?.[column.key] !== 'object'
      ? (row?.[column.key] as any)?.toString() ?? ''
      : row?.[column.key]?.comment || row?.[column.key]?.value) ?? '';

  return (
    <div className="text-truncate" title={value}>
      {value}
    </div>
  );
}

export function EventFormatterHeaderDefault({ column }: HeaderRendererProps<EventDataRow>): ReactNode {
  return column.name;
}

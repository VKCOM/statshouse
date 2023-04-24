import React, { ReactNode } from 'react';
import { FormatterProps, HeaderRendererProps } from 'react-data-grid';
import { EventDataRow } from '../../store/statshouse';

export function EventFormatterDefault({ row, column }: FormatterProps<EventDataRow>): ReactNode {
  return (
    <div className="text-truncate" title={row?.[column.key]?.comment || row?.[column.key]?.value}>
      {row?.[column.key]?.comment || row?.[column.key]?.value}
    </div>
  );
}

export function EventFormatterHeaderDefault({ column }: HeaderRendererProps<EventDataRow>): ReactNode {
  return column.name;
}

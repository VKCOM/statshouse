import { Column } from 'react-data-grid';

import {
  EventFormatterData,
  EventFormatterDefault,
  EventFormatterHeaderDefault,
  EventFormatterHeaderTime,
} from '../components/Plot/EventFormatters';
import { whatToWhatDesc } from './whatToWhatDesc';
import { querySeriesMetaTag } from './api';

export type EventDataRow = {
  key: string;
  idChunk: number;
  timeString: string;
  time: number;
  data: number[];
} & Partial<Record<string, querySeriesMetaTag>>;

export const eventColumnDefault: Readonly<Partial<Column<EventDataRow>>> = {
  minWidth: 20,
  maxWidth: 300,
  resizable: true,
  renderHeaderCell: EventFormatterHeaderDefault,
  width: 'auto',
  renderCell: EventFormatterDefault,
  // sortable: true,
  // headerCellClass: 'no-Focus',
};
export const getEventColumnsType = (what: string[] = []): Record<string, Column<EventDataRow>> => ({
  timeString: { key: 'timeString', name: 'Time', width: 165, renderHeaderCell: EventFormatterHeaderTime },
  ...Object.fromEntries(what.map((key) => [key, { key, name: whatToWhatDesc(key), renderCell: EventFormatterData }])),
});

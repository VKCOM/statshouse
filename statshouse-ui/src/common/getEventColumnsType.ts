// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Column } from 'react-data-grid';

import {
  EventFormatterData,
  EventFormatterDefault,
  EventFormatterHeaderDefault,
  EventFormatterHeaderTime,
} from '@/components2/Plot/PlotView/EventFormatters';
import { whatToWhatDesc } from '@/view/whatToWhatDesc';
import { querySeriesMetaTag } from '@/view/api';

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
  width: 120,
  renderCell: EventFormatterDefault,
  // sortable: true,
  // headerCellClass: 'no-Focus',
};
export const getEventColumnsType = (what: string[] = []): Record<string, Column<EventDataRow>> => ({
  timeString: { key: 'timeString', name: 'Time', width: 165, renderHeaderCell: EventFormatterHeaderTime },
  ...Object.fromEntries(what.map((key) => [key, { key, name: whatToWhatDesc(key), renderCell: EventFormatterData }])),
});

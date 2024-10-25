// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { Key, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import DataGrid, { Column, DataGridHandle, RenderRowProps, Row, SortColumn } from 'react-data-grid';
import cn from 'classnames';
import css from './style.module.css';
import 'react-data-grid/lib/styles.css';
import { PlotEventsButtonColumns } from './PlotEventsButtonColumns';
import { Button } from 'components/UI';
import { EventDataRow } from 'store2/plotEventsDataStore';
import { useEventTagColumns2 } from 'hooks/useEventTagColumns2';
import { PlotKey } from 'url2';
import { useStatsHouseShallow } from 'store2';

import { eventColumnDefault, getEventColumnsType } from 'view/getEventColumnsType';

export type PlotEventsProps = {
  plotKey: PlotKey;
  className?: string;
  onCursor?: (time: number) => void;
  cursor?: number;
};

const rowKeyGetter = (row: EventDataRow) => row.key;
const rowHeight = 28;
const mouseOverRowRenderer = (
  onMouseOver: (row: EventDataRow, event: React.MouseEvent) => void,
  key: Key,
  props: RenderRowProps<EventDataRow>
) => {
  const onMouseOverBind = onMouseOver.bind(undefined, props.row);
  return (
    <Row
      key={key}
      {...props}
      onMouseOver={onMouseOverBind}
      className={cn(props.isRowSelected && css.plotEventsSelectRow)}
    />
  );
};

// const setParams = useStore.getState().setParams;
// const loadEvent = useStore.getState().loadEvents;

export function PlotEvents({ plotKey, className, onCursor, cursor }: PlotEventsProps) {
  const { plot, plotEventsData, setParams, timeRangeFromAbs, loadPlotEvents, clearPlotEvents } = useStatsHouseShallow(
    ({ params: { plots, timeRange }, setParams, plotsEventsData, loadPlotEvents, clearPlotEvents }) => ({
      plot: plots[plotKey],
      plotEventsData: plotsEventsData[plotKey],
      timeRangeFromAbs: timeRange.to + timeRange.from,
      setParams,
      loadPlotEvents,
      clearPlotEvents,
    })
  );
  // const selectorEvent = useMemo(() => selectorEventsByIndex.bind(undefined, indexPlot), [indexPlot]);
  // const event = useStore(selectorEvent);
  // const timeRange = useStore(selectorTimeRange);
  // const clearEvents = useStore(selectorClearEvents);
  const gridRef = useRef<DataGridHandle>(null);
  const [sort, setSort] = useState<SortColumn[]>([]);
  // const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  // const paramsPlot = useStore(selectorParamsPlot);
  const eventColumns = useEventTagColumns2(plot, true);
  const columns = useMemo<Column<EventDataRow, unknown>[]>(
    () => [
      ...Object.values(getEventColumnsType(plotEventsData?.what)),
      ...(eventColumns.map((tag) => ({
        ...eventColumnDefault,
        name: tag.name,
        key: tag.keyTag,
      })) as Column<EventDataRow, unknown>[]),
    ],
    [eventColumns, plotEventsData?.what]
  );
  const loadPrev = useCallback(() => {
    if (!!plotEventsData?.prevKey) {
      loadPlotEvents(plotKey, plotEventsData.prevKey, true)
        .then((res) => {
          if (gridRef.current?.element && res) {
            if (plotEventsData.chunks.length === 1) {
              gridRef.current.element.scrollTop = 0;
            }
            if (res.chunks.length > 1) {
              gridRef.current.element.scrollTop += rowHeight * (res.chunks[0]?.rows?.length ?? 0);
            }
          }
        })
        .catch(() => undefined);
    }
  }, [loadPlotEvents, plotEventsData?.chunks.length, plotEventsData?.prevKey, plotKey]);

  const loadNext = useCallback(() => {
    if (!!plotEventsData?.nextKey) {
      loadPlotEvents(plotKey, plotEventsData.nextKey, false).catch(() => undefined);
    }
  }, [loadPlotEvents, plotEventsData?.nextKey, plotKey]);

  const onScroll = useCallback(
    (e: React.UIEvent<HTMLDivElement>) => {
      const row = Math.floor((gridRef.current?.element?.scrollTop ?? 0) / rowHeight);
      const idChunk = plotEventsData?.rows[row]?.idChunk;
      if (idChunk) {
        const eventFrom = plotEventsData.chunks[idChunk]?.rows?.[0]?.time ?? 0;
        if (eventFrom) {
          setParams((p) => {
            const trFrom = p.timeRange.to + p.timeRange.from;
            p.eventFrom = eventFrom > trFrom ? eventFrom : 0;
          }, true);
        }
      } else {
        setParams((p) => {
          if (p.eventFrom !== 0) {
            p.eventFrom = 0;
          }
        }, true);
      }
      if (
        !plotEventsData?.nextAbortController &&
        e.currentTarget.scrollTop + e.currentTarget.clientHeight >=
          e.currentTarget.scrollHeight - e.currentTarget.clientHeight * 0.2
      ) {
        loadNext();
      }
      if (!plotEventsData?.prevAbortController && e.currentTarget.scrollTop <= e.currentTarget.clientHeight * 0.2) {
        loadPrev();
      }
    },
    [
      loadNext,
      loadPrev,
      plotEventsData?.chunks,
      plotEventsData?.nextAbortController,
      plotEventsData?.prevAbortController,
      plotEventsData?.rows,
      setParams,
    ]
  );
  const clearError = useCallback(() => {
    clearPlotEvents(plotKey);
  }, [clearPlotEvents, plotKey]);

  const onOverRow = useCallback(
    (row: EventDataRow) => {
      onCursor?.(row.time);
    },
    [onCursor]
  );

  const renderRow = useMemo(() => mouseOverRowRenderer.bind(undefined, onOverRow), [onOverRow]);

  const selectedRows = useMemo(() => {
    const selected = new Set<string>();
    if (cursor) {
      plotEventsData?.rows.forEach((row) => {
        if (row.time === cursor) {
          selected.add(row.key);
        }
      });
    }
    return selected;
  }, [cursor, plotEventsData?.rows]);

  useEffect(() => {
    const index = plotEventsData?.rows.findIndex((row) => selectedRows.has(row.key)) ?? -1;
    const length = selectedRows.size + 1;
    if (index > -1 && gridRef.current?.element) {
      const positionStart = rowHeight * index;
      const positionEnd = rowHeight * (index + length) - gridRef.current.element.clientHeight;

      if (gridRef.current.element.scrollTop > positionStart) {
        gridRef.current.element.scrollTop = positionStart;
      }
      if (gridRef.current.element.scrollTop < positionEnd) {
        gridRef.current.element.scrollTop = positionEnd;
      }
    }
  }, [plotEventsData?.rows, selectedRows]);

  useEffect(() => {
    if (plotEventsData?.chunks.length === 1 && timeRangeFromAbs < plotEventsData.range.from) {
      loadPrev();
    }
  }, [loadPrev, plotEventsData?.chunks.length, plotEventsData?.range.from, timeRangeFromAbs]);

  return (
    <div className={cn(className, 'd-flex flex-column')}>
      {!!plotEventsData?.error && (
        <div className="alert alert-danger d-flex align-items-center justify-content-between" role="alert">
          <small className="overflow-force-wrap font-monospace">{plotEventsData?.error}</small>
          <Button type="button" className="btn-close" aria-label="Close" onClick={clearError} />
        </div>
      )}

      <div className="d-flex flex-column flex-grow-1 w-100 position-relative" style={{ minHeight: '400px' }}>
        <div className="position-absolute z-1 top-0 start-0">
          {!!plotEventsData?.rows?.length && (
            <PlotEventsButtonColumns
              plotKey={plotKey}
              loader={!!plotEventsData.nextAbortController || !!plotEventsData.prevAbortController}
            />
          )}
        </div>
        <div className="flex-row flex-grow-1 w-100">
          {!!plotEventsData?.rows?.length ? (
            <DataGrid<EventDataRow>
              className={cn('z-0 position-absolute top-0 start-0 w-100 h-100', css.rdgTheme)}
              ref={gridRef}
              rowHeight={rowHeight}
              columns={columns}
              rows={plotEventsData.rows}
              enableVirtualization
              defaultColumnOptions={eventColumnDefault}
              rowKeyGetter={rowKeyGetter}
              onScroll={onScroll}
              onSortColumnsChange={setSort}
              sortColumns={sort}
              selectedRows={selectedRows}
              renderers={{
                renderRow,
              }}
            />
          ) : (
            <div className="bg-body-tertiary position-absolute top-0 start-0 w-100 h-100"></div>
          )}
        </div>
      </div>
    </div>
  );
}

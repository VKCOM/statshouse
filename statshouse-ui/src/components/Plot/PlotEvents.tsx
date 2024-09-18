import React, { Key, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import DataGrid, { Column, DataGridHandle, Row, RenderRowProps, SortColumn } from 'react-data-grid';
import cn from 'classnames';
import {
  selectorClearEvents,
  selectorEventsByIndex,
  selectorParamsPlotsByIndex,
  selectorTimeRange,
  EventDataRow,
  useStore,
} from '../../store';
import 'react-data-grid/lib/styles.css';
import { produce } from 'immer';
import { TimeRange } from '../../common/TimeRange';
import css from './style.module.css';
import { useEventTagColumns } from '../../hooks/useEventTagColumns';
import { PlotEventsButtonColumns } from './PlotEventsButtonColumns';
import { Button } from '../UI';
import { eventColumnDefault, getEventColumnsType } from '../../view/getEventColumnsType';

export type PlotEventsProps = {
  indexPlot: number;
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

const setParams = useStore.getState().setParams;
const loadEvent = useStore.getState().loadEvents;

export function PlotEvents({ indexPlot, className, onCursor, cursor }: PlotEventsProps) {
  const selectorEvent = useMemo(() => selectorEventsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const event = useStore(selectorEvent);
  const timeRange = useStore(selectorTimeRange);
  const clearEvents = useStore(selectorClearEvents);
  const gridRef = useRef<DataGridHandle>(null);
  const [sort, setSort] = useState<SortColumn[]>([]);
  const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const paramsPlot = useStore(selectorParamsPlot);
  const eventColumns = useEventTagColumns(paramsPlot, true);
  const columns = useMemo<Column<EventDataRow, unknown>[]>(
    () => [
      ...Object.values(getEventColumnsType(event.what)),
      ...(eventColumns.map((tag) => ({
        ...eventColumnDefault,
        name: tag.name,
        key: tag.keyTag,
      })) as Column<EventDataRow, unknown>[]),
    ],
    [event.what, eventColumns]
  );
  const loadPrev = useCallback(() => {
    !!event.prevKey &&
      loadEvent(indexPlot, event.prevKey, true)
        .then((res) => {
          if (gridRef.current?.element && res) {
            if (event.chunks.length === 1) {
              gridRef.current.element.scrollTop = 0;
            }
            if (res.chunks.length > 1) {
              gridRef.current.element.scrollTop += rowHeight * (res.chunks[0]?.rows?.length ?? 0);
            }
          }
        })
        .catch(() => undefined);
  }, [event.chunks, event.prevKey, indexPlot]);

  const loadNext = useCallback(() => {
    !!event.nextKey && loadEvent(indexPlot, event.nextKey, false).catch(() => undefined);
  }, [event.nextKey, indexPlot]);

  const onScroll = useCallback(
    (e: React.UIEvent<HTMLDivElement>) => {
      const row = Math.floor((gridRef.current?.element?.scrollTop ?? 0) / rowHeight);
      const idChunk = event.rows[row]?.idChunk;
      if (idChunk) {
        const eventFrom = event.chunks[idChunk]?.rows?.[0]?.time ?? 0;
        if (eventFrom) {
          setParams(
            produce((p) => {
              const tr = new TimeRange(p.timeRange);
              p.eventFrom = eventFrom > tr.from ? eventFrom : 0;
            }),
            true
          );
        }
      } else {
        setParams(
          produce((p) => {
            p.eventFrom = 0;
          }),
          true
        );
      }
      if (
        !event.nextAbortController &&
        e.currentTarget.scrollTop + e.currentTarget.clientHeight >=
          e.currentTarget.scrollHeight - e.currentTarget.clientHeight * 0.2
      ) {
        loadNext();
      }
      if (!event.prevAbortController && e.currentTarget.scrollTop <= e.currentTarget.clientHeight * 0.2) {
        loadPrev();
      }
    },
    [event.chunks, event.nextAbortController, event.prevAbortController, event.rows, loadNext, loadPrev]
  );
  const clearError = useCallback(() => {
    clearEvents(indexPlot);
  }, [clearEvents, indexPlot]);

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
      event.rows.forEach((row) => {
        if (row.time === cursor) {
          selected.add(row.key);
        }
      });
    }
    return selected;
  }, [cursor, event.rows]);

  useEffect(() => {
    const index = event.rows.findIndex((row) => selectedRows.has(row.key));
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
  }, [event.rows, selectedRows]);

  useEffect(() => {
    if (event.chunks.length === 1 && timeRange.from < event.range.from) {
      loadPrev();
    }
  }, [event.chunks, event.range.from, loadPrev, timeRange.from]);

  return (
    <div className={cn(className, 'd-flex flex-column')}>
      {!!event.error && (
        <div className="alert alert-danger d-flex align-items-center justify-content-between" role="alert">
          <small className="overflow-force-wrap font-monospace">{event.error}</small>
          <Button type="button" className="btn-close" aria-label="Close" onClick={clearError} />
        </div>
      )}

      <div className="d-flex flex-column flex-grow-1 w-100 position-relative" style={{ minHeight: '400px' }}>
        <div className="position-absolute z-1 top-0 start-0">
          {!!event.rows?.length && (
            <PlotEventsButtonColumns
              indexPlot={indexPlot}
              loader={!!event.nextAbortController || !!event.prevAbortController}
            />
          )}
        </div>
        <div className="flex-row flex-grow-1 w-100">
          {!!event.rows?.length ? (
            <DataGrid<EventDataRow>
              className={cn('z-0 position-absolute top-0 start-0 w-100 h-100', css.rdgTheme)}
              ref={gridRef}
              rowHeight={rowHeight}
              columns={columns}
              rows={event.rows}
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

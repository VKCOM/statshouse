import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import DataGrid, { DataGridHandle, SortColumn } from 'react-data-grid';
import cn from 'classnames';
import {
  selectorClearEvents,
  selectorEventsByIndex,
  selectorLoadEvents,
  selectorSetParams,
  selectorTimeRange,
  useStore,
} from '../../store';
import 'react-data-grid/lib/styles.css';
import { EventDataRow } from '../../store/statshouse';
import { eventColumnDefault } from '../../view/api';
import produce from 'immer';
import { TimeRange } from '../../common/TimeRange';

export type PlotEventsProps = {
  indexPlot: number;
  className?: string;
};

const rowKeyGetter = (row: EventDataRow) => row.key;
const rowHeight = 28;

export function PlotEvents({ indexPlot, className }: PlotEventsProps) {
  const selectorEvent = useMemo(() => selectorEventsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const event = useStore(selectorEvent);
  const timeRange = useStore(selectorTimeRange);
  const setParams = useStore(selectorSetParams);
  const loadEvent = useStore(selectorLoadEvents);
  const clearEvents = useStore(selectorClearEvents);
  const gridRef = useRef<DataGridHandle>(null);
  const [sort, setSort] = useState<SortColumn[]>([]);

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
  }, [event.chunks, event.prevKey, indexPlot, loadEvent]);

  const loadNext = useCallback(() => {
    !!event.nextKey && loadEvent(indexPlot, event.nextKey, false).catch(() => undefined);
  }, [event.nextKey, indexPlot, loadEvent]);

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
          e.currentTarget.scrollHeight - e.currentTarget.clientHeight
      ) {
        loadNext();
      }
      if (!event.prevAbortController && e.currentTarget.scrollTop <= e.currentTarget.clientHeight) {
        loadPrev();
      }
    },
    [event.chunks, event.nextAbortController, event.prevAbortController, event.rows, loadNext, loadPrev, setParams]
  );
  const clearError = useCallback(() => {
    clearEvents(indexPlot);
  }, [clearEvents, indexPlot]);

  useEffect(() => {
    if (event.chunks.length === 1 && timeRange.from < event.range.from) {
      loadPrev();
    }
  }, [event.chunks, event.range.from, loadPrev, timeRange.from]);

  return (
    <div className={cn(className, 'position-relative')}>
      {!!event.error && (
        <div className="alert alert-danger d-flex align-items-center justify-content-between" role="alert">
          <small className="overflow-force-wrap font-monospace">{event.error}</small>
          <button type="button" className="btn-close" aria-label="Close" onClick={clearError} />
        </div>
      )}
      <div className="position-absolute z-1 top-0 start-0 pt-3 ps-4">
        {(!!event.nextAbortController || !!event.prevAbortController) && (
          <div className="text-info spinner-border spinner-border-sm m-1" role="status" aria-hidden="true" />
        )}
      </div>
      {!!event.rows?.length && (
        <DataGrid<EventDataRow>
          className="z-0"
          style={{ height: '200px' }}
          ref={gridRef}
          rowHeight={rowHeight}
          columns={event.columns}
          rows={event.rows}
          enableVirtualization
          defaultColumnOptions={eventColumnDefault}
          rowKeyGetter={rowKeyGetter}
          onScroll={onScroll}
          onSortColumnsChange={setSort}
          sortColumns={sort}
        />
      )}
    </div>
  );
}

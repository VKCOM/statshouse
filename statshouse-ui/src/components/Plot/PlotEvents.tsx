import React, { Key, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import DataGrid, { Column, DataGridHandle, Row, RowRendererProps, SortColumn } from 'react-data-grid';
import { ReactComponent as SVGListCheck } from 'bootstrap-icons/icons/list-check.svg';
import cn from 'classnames';
import {
  selectorClearEvents,
  selectorEventsByIndex,
  selectorLoadEvents,
  selectorMetricsMetaByName,
  selectorParamsPlotsByIndex,
  selectorSetParams,
  selectorTimeRange,
  useStore,
} from '../../store';
import 'react-data-grid/lib/styles.css';
import { EventDataRow } from '../../store/statshouse';
import { eventColumnDefault, getEventColumnsType } from '../../view/api';
import produce from 'immer';
import { TimeRange } from '../../common/TimeRange';
import css from './style.module.css';
import { PlotEventsSelectColumns } from './PlotEventsSelectColumns';

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
  props: RowRendererProps<EventDataRow>
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

export function PlotEvents({ indexPlot, className, onCursor, cursor }: PlotEventsProps) {
  const selectorEvent = useMemo(() => selectorEventsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const event = useStore(selectorEvent);
  const timeRange = useStore(selectorTimeRange);
  const setParams = useStore(selectorSetParams);
  const loadEvent = useStore(selectorLoadEvents);
  const clearEvents = useStore(selectorClearEvents);
  const gridRef = useRef<DataGridHandle>(null);
  const [sort, setSort] = useState<SortColumn[]>([]);
  const [eventColumnShow, setEventColumnShow] = useState(false);
  const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const paramsPlot = useStore(selectorParamsPlot);
  const selectorMetricsMeta = useMemo(
    () => selectorMetricsMetaByName.bind(undefined, paramsPlot.metricName),
    [paramsPlot.metricName]
  );
  const meta = useStore(selectorMetricsMeta);

  const columns = useMemo<Column<EventDataRow, unknown>[]>(
    () => [
      ...Object.values(getEventColumnsType(event.what)),
      ...((meta.tags
        ?.map((tag, indexTag) => {
          if (
            paramsPlot.eventsBy.indexOf(indexTag.toString()) > -1 ||
            paramsPlot.groupBy.indexOf(`key${indexTag}`) > -1
          ) {
            return {
              ...eventColumnDefault,
              name: tag.description ? tag.description : tag.name ? tag.name : `tag ${indexTag}`,
              key: `key${indexTag}`,
            };
          }
          return false;
        })
        .filter(Boolean) ?? []) as Column<EventDataRow, unknown>[]),
    ],
    [event.what, meta.tags, paramsPlot.eventsBy, paramsPlot.groupBy]
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
          e.currentTarget.scrollHeight - e.currentTarget.clientHeight * 0.2
      ) {
        loadNext();
      }
      if (!event.prevAbortController && e.currentTarget.scrollTop <= e.currentTarget.clientHeight * 0.2) {
        loadPrev();
      }
    },
    [event.chunks, event.nextAbortController, event.prevAbortController, event.rows, loadNext, loadPrev, setParams]
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

  const rowRenderer = useMemo(() => mouseOverRowRenderer.bind(undefined, onOverRow), [onOverRow]);

  const toggleEventColumnShow = useCallback((event?: React.MouseEvent) => {
    setEventColumnShow((s) => !s);
    event?.stopPropagation();
  }, []);

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
    <div className={cn(className, 'position-relative d-flex flex-column')}>
      {!!event.error && (
        <div className="alert alert-danger d-flex align-items-center justify-content-between" role="alert">
          <small className="overflow-force-wrap font-monospace">{event.error}</small>
          <button type="button" className="btn-close" aria-label="Close" onClick={clearError} />
        </div>
      )}
      <div className="position-absolute z-1 top-0 start-0 pt-3 ps-4">
        {!!event.nextAbortController || !!event.prevAbortController ? (
          <div className="text-info spinner-border spinner-border-sm m-1" role="status" aria-hidden="true" />
        ) : (
          <button className="btn btn-sm" onClick={toggleEventColumnShow} title="select table column">
            <SVGListCheck />
          </button>
        )}
        {eventColumnShow && (
          <PlotEventsSelectColumns
            indexPlot={indexPlot}
            className={cn('position-absolute card p-2', css.plotEventsSelectColumns)}
            onClose={toggleEventColumnShow}
          />
        )}
      </div>
      <div className="d-flex flex-column flex-grow-1">
        {!!event.rows?.length && (
          <DataGrid<EventDataRow>
            className={cn('z-0 flex-grow-1', css.rdgTheme)}
            style={{ height: '400px' }}
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
              rowRenderer,
            }}
          />
        )}
      </div>
    </div>
  );
}

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { Key, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Column, DataGrid, DataGridHandle, RenderRowProps, Row, SortColumn } from 'react-data-grid';
import cn from 'classnames';
import css from './style.module.css';
import 'react-data-grid/lib/styles.css';
import { PlotEventsButtonColumns } from './PlotEventsButtonColumns';
import { Button } from '@/components/UI';
import { useEventTagColumns2 } from '@/hooks/useEventTagColumns2';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { eventColumnDefault, EventDataRow, getEventColumnsType } from '@/common/getEventColumnsType';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useApiTableInfinite } from '@/api/table';
import { fmtInputDateTime, formatLegendValue } from '@/view/utils2';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import { useMetricName } from '@/hooks/useMetricName';
import { METRIC_TYPE, TagKey } from '@/api/enum';
import { formatByMetricType } from '@/common/formatByMetricType';
import { emptyArray, isNotNil } from '@/common/helpers';
import { freeKeyPrefix } from '@/url2';
import { SeriesMetaTag } from '@/api/query';
import { StatsHouseStore, useStatsHouseShallow } from '@/store2';
import { usePlotsDataStore } from '@/store2/plotDataStore';

export type PlotEventsProps = {
  className?: string;
  onCursor?: (time: number) => void;
  cursor?: number;
  setTimeRange?: (range?: { from: number; to: number }) => void;
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

function freeKeyTag(tags: Record<TagKey, SeriesMetaTag>) {
  return Object.fromEntries(Object.entries(tags).map(([tagKey, tagValue]) => [freeKeyPrefix(tagKey), tagValue]));
}

const selectorStore = ({ params: { timeRange, variables } }: StatsHouseStore) => ({ timeRange, variables });

export function PlotEvents({ className, onCursor, cursor, setTimeRange }: PlotEventsProps) {
  const { plot } = useWidgetPlotContext();

  const metricUnit = usePlotsDataStore(
    useCallback(({ plotsData }) => plotsData[plot.id]?.metricUnit ?? METRIC_TYPE.none, [plot.id])
  );

  const { timeRange, variables } = useStatsHouseShallow(selectorStore);

  const meta = useMetricMeta(useMetricName(true));

  const metricUnitData = metricUnit ?? meta?.metric_type;
  const query = useApiTableInfinite(plot, timeRange, variables);
  const queryData = query.data;
  const queryIsLoading = query.isLoading || query.isRefetching;
  const queryHasNextPage = query.hasNextPage;
  const queryHasPreviousPage = query.hasPreviousPage;
  const queryFetchNextPage = query.fetchNextPage;
  const queryFetchPreviousPage = query.fetchPreviousPage;
  const queryRefetch = query.refetch;

  const queryWhat = useMemo(() => queryData?.pages[0]?.data.what, [queryData?.pages]);
  const queryRows = useMemo<EventDataRow[]>(() => {
    const queryWhat = queryData?.pages[0]?.data.what ?? [];
    const formatMetric = metricUnitData !== METRIC_TYPE.none && formatByMetricType(metricUnitData);
    return (
      queryData?.pages
        .flatMap((page, pageIndex) =>
          page.data.rows?.map(
            (row, indexRow) =>
              ({
                key: `table_row_${pageIndex}_${indexRow}`,
                idChunk: pageIndex,
                timeString: fmtInputDateTime(new Date(row.time * 1000)),
                data: row.data,
                time: row.time,
                ...Object.fromEntries(
                  queryWhat.map((whatKey, indexWhat) => [
                    whatKey,
                    {
                      value: row.data[indexWhat],
                      formatValue: formatMetric
                        ? formatMetric(row.data[indexWhat])
                        : formatLegendValue(row.data[indexWhat]),
                    },
                  ])
                ),
                ...freeKeyTag(row.tags),
              }) as EventDataRow
          )
        )
        .filter(isNotNil) ?? emptyArray
    );
  }, [metricUnitData, queryData?.pages]);

  useEffect(() => {
    const to = queryRows?.[0]?.time;
    const from = queryRows?.[queryRows?.length - 1]?.time;
    if (from && to && (queryHasNextPage || queryHasPreviousPage)) {
      setTimeRange?.({ from, to });
    } else {
      setTimeRange?.(undefined);
    }
  }, [queryHasNextPage, queryHasPreviousPage, queryRows, setTimeRange]);

  const gridRef = useRef<DataGridHandle>(null);
  const [sort, setSort] = useState<SortColumn[]>([]);
  const eventColumns = useEventTagColumns2(plot, true);
  const columns = useMemo<Column<EventDataRow, unknown>[]>(
    () => [
      ...Object.values(getEventColumnsType(queryWhat)),
      ...(eventColumns.map((tag) => ({
        ...eventColumnDefault,
        name: tag.name,
        key: tag.keyTag,
      })) as Column<EventDataRow, unknown>[]),
    ],
    [eventColumns, queryWhat]
  );

  const loadPrev = useCallback(() => {
    if (!queryIsLoading && queryHasPreviousPage) {
      queryFetchPreviousPage();
    }
  }, [queryFetchPreviousPage, queryHasPreviousPage, queryIsLoading]);

  const loadNext = useCallback(() => {
    if (!queryIsLoading && queryHasNextPage) {
      queryFetchNextPage();
    }
  }, [queryFetchNextPage, queryHasNextPage, queryIsLoading]);

  const onScroll = useCallback(
    (e: React.UIEvent<HTMLDivElement>) => {
      if (
        e.currentTarget.scrollTop + e.currentTarget.clientHeight >=
        e.currentTarget.scrollHeight - e.currentTarget.clientHeight * 0.2
      ) {
        loadNext();
      }
    },
    [loadNext]
  );
  const reload = useCallback(() => {
    queryRefetch();
  }, [queryRefetch]);

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
      queryRows?.forEach((row) => {
        if (row.time === cursor) {
          selected.add(row.key);
        }
      });
    }
    return selected;
  }, [cursor, queryRows]);

  useEffect(() => {
    const index = queryRows?.findIndex((row) => selectedRows.has(row.key)) ?? -1;
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
  }, [queryRows, selectedRows]);

  useEffect(() => {
    if (query.hasPreviousPage) {
      loadPrev();
    }
  }, [loadPrev, query.hasPreviousPage]);

  return (
    <div className={cn(className, 'd-flex flex-column')}>
      {!!query.error && (
        <div className="alert alert-danger d-flex align-items-center justify-content-between py-0" role="alert">
          <Button type="button" className="btn" aria-label="Reload" onClick={reload}>
            <SVGArrowCounterclockwise />
          </Button>
          <small className="overflow-force-wrap font-monospace">{query.error.toString()}</small>
        </div>
      )}

      <div className="d-flex flex-column flex-grow-1 w-100 position-relative" style={{ minHeight: '400px' }}>
        <div className="position-absolute z-1 top-0 start-0">
          <PlotEventsButtonColumns loader={queryIsLoading} />
        </div>
        <div className="flex-row flex-grow-1 w-100">
          <DataGrid<EventDataRow>
            className={cn('z-0 position-absolute top-0 start-0 w-100 h-100', css.rdgTheme)}
            ref={gridRef}
            rowHeight={rowHeight}
            columns={columns}
            rows={queryRows}
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
        </div>
      </div>
    </div>
  );
}

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback, useMemo, useState } from 'react';
import { apiTableRowNormalize, QueryTableRow } from '@/api/tableOld2';

import { Button } from '@/components/UI';
import cn from 'classnames';
import css from './style.module.css';
import { freeKeyPrefix, PlotParams, TimeRange } from '@/url2';
import { useEventTagColumns2 } from '@/hooks/useEventTagColumns2';
import { ApiTable, useApiTableNoLive } from '@/api/table';
import { toNumber } from '@/common/helpers';
import { StatsHouseStore, useStatsHouse } from '@/store2';

export type PlotEventOverlayTableProps = {
  plot: PlotParams;
  agg: string;
  range: TimeRange;
  width: number;
};

const selectorStore = ({ params: { variables } }: StatsHouseStore) => variables;

export const PlotEventOverlayTable = memo(function PlotEventOverlayTable({
  plot,
  range,
  agg,
}: PlotEventOverlayTableProps) {
  const variables = useStatsHouse(selectorStore);
  const [error, setError] = useState<Error>();

  const errorText = useMemo(() => {
    if (error instanceof Error && error.name !== 'AbortError') {
      return error.toString();
    }
    return '';
  }, [error]);
  const clearError = useCallback(() => {
    setError(undefined);
  }, []);

  const columns = useEventTagColumns2(plot, true);
  const select = useCallback(
    (response?: ApiTable) => {
      if (response) {
        return {
          ...response.data,
          rows:
            response.data.rows?.map(
              (value) =>
                ({
                  ...value,
                  tags:
                    value.tags &&
                    Object.fromEntries(
                      Object.entries(value.tags).map(([tagKey, tagValue]) => [freeKeyPrefix(tagKey), tagValue])
                    ),
                }) as QueryTableRow
            ) ?? null,
          rowsNormalize: apiTableRowNormalize(plot, response.data),
        };
      }
      return undefined;
    },
    [plot]
  );

  const queryTable = useApiTableNoLive(
    { ...plot, customAgg: toNumber(agg, 1) },
    range,
    variables,
    undefined,
    undefined,
    undefined,
    select
  );
  const isLoading = queryTable.isLoading;
  const chunk = queryTable.data;

  return (
    <div className={cn('position-relative flex-grow-1 d-flex flex-column', css.overlayCardTableItem)}>
      {isLoading && (
        <div className=" position-absolute top-50 start-50 translate-middle">
          <div className="text-info spinner-border spinner-border-sm " role="status" aria-hidden="true" />
        </div>
      )}
      {!!errorText && (
        <div className="alert alert-danger d-flex align-items-center justify-content-between" role="alert">
          <small className="overflow-force-wrap font-monospace">{errorText}</small>
          <Button type="button" className="btn-close" aria-label="Close" onClick={clearError} />
        </div>
      )}
      <table className="table table-sm m-0 table-borderless">
        <tbody>
          {columns.length
            ? chunk?.rowsNormalize?.map((row, indexRow) => (
                <tr key={indexRow}>
                  {columns.map((tag) => (
                    <td key={tag.keyTag} className="text-nowrap" title={tag.name}>
                      {row[tag.keyTag]}
                    </td>
                  ))}
                </tr>
              ))
            : !isLoading && (
                <tr>
                  <td>
                    <div className="text-danger text-nowrap">no columns for show</div>
                    <div className="text-body-tertiary text-nowrap">{chunk?.rowsNormalize.length || 'no'} events</div>
                  </td>
                </tr>
              )}
        </tbody>
      </table>
      {chunk?.more && <div className="text-secondary pb-3">more...</div>}
    </div>
  );
});

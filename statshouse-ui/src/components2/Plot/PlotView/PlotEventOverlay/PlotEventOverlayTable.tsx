// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useEffect, useMemo, useState } from 'react';
import { type ApiTable } from 'api/tableOld';

import { Button } from 'components/UI';
import cn from 'classnames';
import css from './style.module.css';
import { type PlotParams, type TimeRange } from 'url2';
import { useEventTagColumns2 } from 'hooks/useEventTagColumns2';
import { apiTable } from 'api/tableOld2';

export type PlotEventOverlayTableProps = {
  plot: PlotParams;
  agg: string;
  range: TimeRange;
  width: number;
};

export function _PlotEventOverlayTable({ plot, range, agg }: PlotEventOverlayTableProps) {
  const [chunk, setChunk] = useState<ApiTable>();
  const [loader, setLoader] = useState(false);
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

  useEffect(() => {
    const controller = new AbortController();
    setLoader(true);
    apiTable(plot, range, agg, undefined, undefined, undefined, controller)
      .then(setChunk)
      .catch(setError)
      .finally(() => setLoader(false));
    return () => {
      if (!controller.signal.aborted) {
        controller.abort();
      }
    };
  }, [plot, range, agg]);

  return (
    <div className={cn('position-relative flex-grow-1 d-flex flex-column', css.overlayCardTableItem)}>
      {loader && (
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
          {!!columns.length
            ? chunk?.rowsNormalize?.map((row, indexRow) => (
                <tr key={indexRow}>
                  {columns.map((tag) => (
                    <td key={tag.keyTag} className="text-nowrap" title={tag.name}>
                      {row[tag.keyTag]}
                    </td>
                  ))}
                </tr>
              ))
            : !loader && (
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
}

export const PlotEventOverlayTable = memo(_PlotEventOverlayTable);

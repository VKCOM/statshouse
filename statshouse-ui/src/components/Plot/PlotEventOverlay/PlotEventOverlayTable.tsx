import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { ApiTable, apiTable } from '../../../api/table';
import { PlotParams } from '../../../common/plotQueryParams';
import { TimeRange } from '../../../common/TimeRange';
import { useEventTagColumns } from '../../../hooks/useEventTagColumns';

export type PlotEventOverlayTableProps = {
  plot: PlotParams;
  range: TimeRange;
  width: number;
};

export function PlotEventOverlayTable({ plot, width, range }: PlotEventOverlayTableProps) {
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

  const columns = useEventTagColumns(plot, true);

  useEffect(() => {
    const controller = new AbortController();
    setLoader(true);
    apiTable(plot, range, width, undefined, undefined, undefined, controller)
      .then(setChunk)
      .catch(setError)
      .finally(() => setLoader(false));
    return () => {
      if (!controller.signal.aborted) {
        controller.abort();
      }
    };
  }, [plot, range, width]);

  return (
    <div className="position-relative flex-grow-1 d-flex flex-column" style={{ minWidth: 100, minHeight: 20 }}>
      {loader && (
        <div className=" position-absolute top-50 start-50 translate-middle">
          <div className="text-info spinner-border spinner-border-sm " role="status" aria-hidden="true" />
        </div>
      )}
      {!!errorText && (
        <div className="alert alert-danger d-flex align-items-center justify-content-between" role="alert">
          <small className="overflow-force-wrap font-monospace">{errorText}</small>
          <button type="button" className="btn-close" aria-label="Close" onClick={clearError} />
        </div>
      )}
      <table className="table table-sm m-0 table-borderless">
        <tbody>
          {chunk?.rowsNormalize?.map((row, indexRow) => (
            <tr key={indexRow}>
              {columns.map((tag) => (
                <td key={tag.keyTag} className="text-nowrap" title={tag.name}>
                  {row[tag.keyTag]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {chunk?.more && <div className="text-secondary pb-3">more...</div>}
    </div>
  );
}

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { ApiTable, apiTable, ApiTableRowNormalize } from '../../../api/table';
import { PlotParams } from '../../../common/plotQueryParams';
import { TimeRange } from '../../../common/TimeRange';

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

  useEffect(() => {
    const controller = new AbortController();
    setLoader(true);
    apiTable(plot, range, width, undefined, undefined, 100, controller)
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
    <div className="position-relative flex-grow-1 d-flex flex-column" style={{ minWidth: 100, minHeight: 50 }}>
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
      <table className="table">
        <tbody>
          {chunk?.rowsNormalize?.map((row, index) => (
            <tr key={index}>
              <td className="text-nowrap">{row.timeString}</td>
              {(plot.eventsBy as (keyof ApiTableRowNormalize)[]).map((tagKey) => (
                <td key={tagKey} className="text-nowrap">
                  {row[tagKey]}
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

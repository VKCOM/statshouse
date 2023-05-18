import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { ApiTable, apiTable, ApiTableRowNormalize, TagKey } from '../../../api/table';
import { PlotParams } from '../../../common/plotQueryParams';
import { TimeRange } from '../../../common/TimeRange';

export type PlotEventOverlayTableProps = {
  plot: PlotParams;
  range: TimeRange;
  width: number;
};

const arr16 = new Array(16).fill(0).map((v, i) => i.toString()) as TagKey[];

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

  const columns = useMemo<(keyof ApiTableRowNormalize)[]>(
    () => [
      // ...plot.what,
      ...arr16.filter((i) => plot.eventsBy.indexOf(i.toString()) > -1 || plot.groupBy.indexOf(`key${i}`) > -1),
    ],
    [plot.eventsBy, plot.groupBy]
  );

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
              {columns.map((tagKey) => (
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

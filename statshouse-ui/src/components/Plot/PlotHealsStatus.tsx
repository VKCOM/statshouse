import { Button, Tooltip } from '../UI';
import React from 'react';
import { ReactComponent as SVGExclamationTriangleFill } from 'bootstrap-icons/icons/exclamation-triangle-fill.svg';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';

export type PlotHealsStatusProps = {
  numQueries: number;
  lastError: string;
  clearLastError: () => void;
  live: boolean;
  reload: () => void;
  timeoutInfo?: string;
};

export function PlotHealsStatus({ numQueries, lastError, reload, clearLastError, timeoutInfo }: PlotHealsStatusProps) {
  return (
    <Tooltip
      titleClassName="alert alert-danger p-0"
      horizontal="left"
      vertical="out-bottom"
      hover
      style={{ width: 24, height: 24 }}
      open={lastError ? undefined : false}
      title={
        !!lastError && (
          <div className="d-flex flex-nowrap align-items-center justify-content-between" role="alert">
            <Button type="button" className="btn" aria-label="Reload" onClick={reload}>
              <SVGArrowCounterclockwise />
            </Button>
            <div>
              <pre className="my-0 mx-1 overflow-force-wrap font-monospace">{lastError}</pre>
              {!!timeoutInfo && (
                <pre className="my-0 mx-1 overflow-force-wrap font-monospace text-secondary">{timeoutInfo}</pre>
              )}
            </div>
            <Button type="button" className="btn btn-close" aria-label="Close" onClick={clearLastError} />
          </div>
        )
      }
    >
      {numQueries > 0 ? (
        <div className="text-info spinner-border spinner-border-sm" role="status" aria-hidden="true" />
      ) : !!lastError ? (
        <div>
          <SVGExclamationTriangleFill className="text-danger" />
        </div>
      ) : null}
    </Tooltip>
  );
}

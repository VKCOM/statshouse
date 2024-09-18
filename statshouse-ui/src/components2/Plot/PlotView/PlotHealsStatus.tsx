// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useMemo } from 'react';
import { type PlotKey } from 'url2';
import { Button, Tooltip } from 'components/UI';
import { ReactComponent as SVGExclamationTriangleFill } from 'bootstrap-icons/icons/exclamation-triangle-fill.svg';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { useStatsHouseShallow } from 'store2';
import cn from 'classnames';
import { useLiveModeStore } from 'store2/liveModeStore';
import { usePlotLoader } from 'store2/plotQueryStore';

export type PlotHealsStatusProps = {
  className?: string;
  plotKey: PlotKey;
};
export function _PlotHealsStatus({ className, plotKey }: PlotHealsStatusProps) {
  const interval = useLiveModeStore(({ interval }) => interval);
  const loader = usePlotLoader(plotKey);
  const { lastError, plotHealsTimeout, clearPlotError, loadPlotData } = useStatsHouseShallow(
    ({ plotsData, plotHeals, clearPlotError, loadPlotData }) => ({
      lastError: plotsData[plotKey]?.error,
      plotHealsTimeout: plotHeals[plotKey]?.timeout,
      clearPlotError,
      loadPlotData,
    })
  );
  const healsInfo = useMemo(() => {
    if (plotHealsTimeout && interval < plotHealsTimeout) {
      return `plot update timeout ${plotHealsTimeout} sec`;
    }
    return undefined;
  }, [interval, plotHealsTimeout]);
  const clearLastError = useCallback(() => {
    clearPlotError(plotKey);
  }, [clearPlotError, plotKey]);
  const reload = useCallback(() => {
    clearPlotError(plotKey);
    loadPlotData(plotKey);
  }, [clearPlotError, loadPlotData, plotKey]);
  return (
    <Tooltip
      titleClassName={cn('alert alert-danger p-0', className)}
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
              {!!healsInfo && (
                <pre className="my-0 mx-1 overflow-force-wrap font-monospace text-secondary">{healsInfo}</pre>
              )}
            </div>
            <Button type="button" className="btn btn-close" aria-label="Close" onClick={clearLastError} />
          </div>
        )
      }
    >
      {loader ? (
        <div className="text-info spinner-border spinner-border-sm" role="status" aria-hidden="true" />
      ) : !!lastError ? (
        <div>
          <SVGExclamationTriangleFill className="text-danger" />
        </div>
      ) : null}
    </Tooltip>
  );
}
export const PlotHealsStatus = memo(_PlotHealsStatus);

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback, useMemo } from 'react';
import { Button, Tooltip } from '@/components/UI';
import { ReactComponent as SVGExclamationTriangleFill } from 'bootstrap-icons/icons/exclamation-triangle-fill.svg';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { useStatsHouse, useStatsHouseShallow } from '@/store2';
import cn from 'classnames';
import { useLiveModeStore } from '@/store2/liveModeStore';
import { usePlotLoader } from '@/store2/plotQueryStore';
import { useWidgetParamsContext, useWidgetPlotContext } from '@/contexts';
import { refetchQuery } from '@/api/query';

export type PlotHealsStatusProps = {
  className?: string;
};
const removePlotHeals = useStatsHouse.getState().removePlotHeals;

export const PlotHealsStatus = memo(function PlotHealsStatus({ className }: PlotHealsStatusProps) {
  const {
    plot,
    plotData: { error },
    setPlotData,
  } = useWidgetPlotContext();
  const { params } = useWidgetParamsContext();
  const id = plot.id;
  const interval = useLiveModeStore(({ interval }) => interval);
  const loader = usePlotLoader(id);
  const { plotHealsTimeout } = useStatsHouseShallow(
    useCallback(
      ({ plotHeals }) => ({
        plotHealsTimeout: plotHeals[id]?.timeout,
      }),
      [id]
    )
  );
  const healsInfo = useMemo(() => {
    if (plotHealsTimeout && interval < plotHealsTimeout) {
      return `plot update timeout ${plotHealsTimeout} sec`;
    }
    return undefined;
  }, [interval, plotHealsTimeout]);
  const clearLastError = useCallback(() => {
    setPlotData((d) => {
      d.error = '';
    });
    removePlotHeals(id);
  }, [id, setPlotData]);
  const reload = useCallback(() => {
    refetchQuery(plot, params);
    clearLastError();
    removePlotHeals(id);
  }, [clearLastError, id, params, plot]);
  return (
    <Tooltip
      titleClassName={cn('alert alert-danger p-0', className)}
      horizontal="left"
      vertical="out-bottom"
      hover
      style={{ width: 24, height: 24 }}
      open={error ? undefined : false}
      title={
        !!error && (
          <div className="d-flex flex-nowrap align-items-center justify-content-between" role="alert">
            <Button type="button" className="btn" aria-label="Reload" onClick={reload}>
              <SVGArrowCounterclockwise />
            </Button>
            <div>
              <pre className="my-0 mx-1 overflow-force-wrap font-monospace">{error}</pre>
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
      ) : error ? (
        <div>
          <SVGExclamationTriangleFill className="text-danger" />
        </div>
      ) : null}
    </Tooltip>
  );
});

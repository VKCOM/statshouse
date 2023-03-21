// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useMemo } from 'react';
import cn from 'classnames';
import produce from 'immer';
import { promQLMetric } from '../../view/utils';
import { whatToWhatDesc } from '../../view/api';
import {
  selectorDashboardLayoutEdit,
  selectorParamsPlotsByIndex,
  selectorPlotsDataByIndex,
  selectorSetParamsPlots,
  useStore,
} from '../../store';
import { PlotLink } from './PlotLink';
import css from './style.module.css';
import { TextEditable } from '../TextEditable';

export type PlotHeaderTitleProps = {
  indexPlot: number;
  compact?: boolean;
  dashboard?: boolean;
};
export function PlotHeaderTitle({ indexPlot, compact, dashboard }: PlotHeaderTitleProps) {
  const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const params = useStore(selectorParamsPlot);
  const setParamsPlots = useStore(selectorSetParamsPlots);
  const setParams = useMemo(() => setParamsPlots.bind(undefined, indexPlot), [indexPlot, setParamsPlots]);
  const selectorPlotsData = useMemo(() => selectorPlotsDataByIndex.bind(undefined, indexPlot), [indexPlot]);
  const plotData = useStore(selectorPlotsData);
  const dashboardLayoutEdit = useStore(selectorDashboardLayoutEdit);

  const metricName = useMemo(
    () => (params.metricName !== promQLMetric ? params.metricName : plotData.nameMetric),
    [params.metricName, plotData.nameMetric]
  );
  const what = useMemo(
    () =>
      params.metricName === promQLMetric
        ? plotData.whats.map((qw) => whatToWhatDesc(qw)).join(', ')
        : params.what.map((qw) => whatToWhatDesc(qw)).join(', '),
    [params.metricName, params.what, plotData.whats]
  );
  const metricFullName = useMemo(() => metricName + (what ? ': ' + what : ''), [metricName, what]);

  const editCustomName = useCallback(
    (value: string) => {
      setParams(
        produce((p) => {
          p.customName = value !== metricFullName ? value : '';
        })
      );
    },
    [metricFullName, setParams]
  );

  const stopPropagation = useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
  }, []);

  const onInputCustomInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const value = e.currentTarget.value;
      editCustomName(value !== metricFullName ? value : '');
    },
    [editCustomName, metricFullName]
  );

  if (dashboard && compact) {
    return dashboardLayoutEdit ? (
      <input
        type="text"
        className={cn(css.plotInputName, 'form-control form-control-sm mb-1')}
        defaultValue={params.customName || metricFullName}
        placeholder={metricFullName}
        onPointerDown={stopPropagation}
        onInput={onInputCustomInput}
      />
    ) : (
      <PlotLink
        className="text-secondary text-decoration-none overflow-hidden w-100"
        indexPlot={indexPlot}
        target={dashboard ? '_self' : '_blank'}
      >
        {params.customName ? (
          <span className="text-body me-3 text-truncate" title={params.customName}>
            {params.customName}
          </span>
        ) : (
          <span className="overflow-hidden d-flex flex-row w-100 justify-content-center" title={metricFullName}>
            <span className="text-body text-truncate">{metricName}</span>
            {!!what && (
              <>
                <span>: </span>
                <span className="me-3 text-truncate">{what}</span>
              </>
            )}
          </span>
        )}
      </PlotLink>
    );
  }

  return compact ? (
    <PlotLink
      className="text-secondary text-decoration-none"
      indexPlot={indexPlot}
      target={dashboard ? '_self' : '_blank'}
    >
      {params.customName ? (
        <span className="text-body me-3">{params.customName}</span>
      ) : (
        <>
          <span className="text-body">{metricName}</span>
          {!!what && (
            <>
              :<span className="me-3"> {what}</span>
            </>
          )}
        </>
      )}
    </PlotLink>
  ) : (
    <TextEditable
      className="flex-grow-1"
      defaultValue={params.customName || metricFullName}
      placeholder={
        params.customName || (
          <>
            <span>{metricName}</span>
            {!!what && (
              <>
                :<span className="text-secondary"> {what}</span>
              </>
            )}
          </>
        )
      }
      inputPlaceholder={metricFullName}
      onSave={editCustomName}
    />
  );
}

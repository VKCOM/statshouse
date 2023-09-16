// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo } from 'react';
import cn from 'classnames';
import produce from 'immer';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { ReactComponent as SVGBoxArrowUpRight } from 'bootstrap-icons/icons/box-arrow-up-right.svg';
import { promQLMetric } from '../../view/utils';
import { whatToWhatDesc } from '../../view/api';
import { Store, useStore } from '../../store';
import { PlotLink } from './PlotLink';
import css from './style.module.css';
import { TextEditable } from '../TextEditable';
import { useDebounceState } from '../../hooks';
import { shallow } from 'zustand/shallow';
import { PlotName } from './PlotName';
import { Link } from 'react-router-dom';

const stopPropagation = (e: React.MouseEvent) => {
  e.stopPropagation();
};

export type PlotHeaderTitleProps = {
  indexPlot: number;
  compact?: boolean;
  dashboard?: boolean;
  outerLink?: string;
  embed?: boolean;
};

const { removePlot, setPlotParams } = useStore.getState();

const selectorPlotInfoByIndex = (indexPlot: number, { params, plotsData, dashboardLayoutEdit }: Store) => ({
  plot: params.plots[indexPlot],
  plotData: plotsData[indexPlot],
  dashboardLayoutEdit,
  plotCount: params.plots.length,
});

export function PlotHeaderTitle({ indexPlot, compact, dashboard, outerLink, embed }: PlotHeaderTitleProps) {
  const selectorPlotInfo = useMemo(() => selectorPlotInfoByIndex.bind(undefined, indexPlot), [indexPlot]);
  const { plot, plotData, dashboardLayoutEdit, plotCount } = useStore(selectorPlotInfo, shallow);
  const setParams = useMemo(() => setPlotParams.bind(undefined, indexPlot), [indexPlot]);

  const metricName = useMemo(
    () => (plot.metricName !== promQLMetric ? plot.metricName : plotData.nameMetric),
    [plot.metricName, plotData.nameMetric]
  );
  const what = useMemo(
    () =>
      plot.metricName === promQLMetric
        ? plotData.whats.map((qw) => whatToWhatDesc(qw)).join(', ')
        : plot.what.map((qw) => whatToWhatDesc(qw)).join(', '),
    [plot.metricName, plot.what, plotData.whats]
  );
  const metricFullName = useMemo(() => (metricName ? metricName + (what ? ': ' + what : '') : ''), [metricName, what]);
  const [customName, debounceCustomName, setCustomName] = useDebounceState(plot.customName, 200);
  const editCustomName = useCallback(
    (value: string) => {
      setCustomName(value !== metricFullName ? value : '');
    },
    [metricFullName, setCustomName]
  );
  useEffect(() => {
    setParams(
      produce((p) => {
        p.customName = debounceCustomName;
      })
    );
  }, [debounceCustomName, setParams]);

  useEffect(() => {
    setCustomName(plot.customName);
  }, [plot.customName, setCustomName]);

  const onInputCustomInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const value = e.currentTarget.value;
      editCustomName(value !== metricFullName ? value : '');
    },
    [editCustomName, metricFullName]
  );
  const onRemove = useCallback(() => {
    removePlot(indexPlot);
  }, [indexPlot]);

  if (dashboard && compact && !embed) {
    return dashboardLayoutEdit ? (
      <div className="w-100 d-flex flex-row">
        <input
          type="text"
          className={cn(css.plotInputName, 'form-control form-control-sm mb-1 flex-grow-1')}
          value={customName || metricFullName}
          placeholder={metricFullName}
          onPointerDown={stopPropagation}
          onInput={onInputCustomInput}
        />
        {plotCount > 1 && (
          <button
            className={cn(css.plotRemoveBtn, 'btn btn-sm mb-1 ms-1 border-0')}
            title="Remove"
            onPointerDown={stopPropagation}
            onClick={onRemove}
          >
            <SVGTrash />
          </button>
        )}
      </div>
    ) : (
      <div className="me-3 d-flex w-100">
        <PlotLink
          className="text-decoration-none overflow-hidden"
          title={customName || metricFullName}
          indexPlot={indexPlot}
          target={dashboard ? '_self' : '_blank'}
        >
          <PlotName plot={plot} plotData={plotData} />
        </PlotLink>
        {!!outerLink && (
          <Link to={outerLink} target="_blank" className="ms-2">
            <SVGBoxArrowUpRight width={10} height={10} />
          </Link>
        )}
      </div>
    );
  }

  return compact ? (
    <PlotLink
      className="text-secondary text-decoration-none me-3"
      indexPlot={indexPlot}
      target={embed ? '_blank' : '_self'}
    >
      <PlotName plot={plot} plotData={plotData} />
    </PlotLink>
  ) : (
    <TextEditable
      className="flex-grow-1"
      defaultValue={plot.customName || metricFullName}
      placeholder={
        <span>
          <PlotName plot={plot} plotData={plotData} />
          {!!outerLink && (
            <Link to={outerLink} target="_blank" className="ms-2">
              <SVGBoxArrowUpRight width={10} height={10} />
            </Link>
          )}
        </span>
      }
      inputPlaceholder={metricFullName}
      onSave={editCustomName}
    />
  );
}

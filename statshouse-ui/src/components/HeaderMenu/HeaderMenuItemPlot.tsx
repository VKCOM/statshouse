// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation } from 'react-router-dom';

import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { ReactComponent as SVGXSquare } from 'bootstrap-icons/icons/x-square.svg';

import { Store, useStore } from '../../store';
import { PlotLink } from '../Plot/PlotLink';
import { whatToWhatDesc } from '../../view/api';

import cn from 'classnames';
import css from './style.module.css';
import { promQLMetric } from '../../view/utils';
import { PLOT_TYPE } from '../../common/plotQueryParams';
import { shallow } from 'zustand/shallow';

export type HeaderMenuItemPlotProps = {
  indexPlot: number;
};

const { removePlot } = useStore.getState();
const selectorPlotInfoByIndex = (indexPlot: number, { params, previews, numQueriesPlot, plotsData }: Store) => ({
  plot: params.plots[indexPlot],
  preview: previews[indexPlot],
  numQueries: numQueriesPlot[indexPlot],
  plotData: plotsData[indexPlot],
  tabNum: params.tabNum,
  plotCount: params.plots.length,
});

export const HeaderMenuItemPlot: React.FC<HeaderMenuItemPlotProps> = ({ indexPlot }) => {
  const touchToggle = useRef<HTMLAnchorElement>(null);
  const sub = useRef<HTMLUListElement>(null);
  const [open, setOpen] = useState(false);
  const location = useLocation();
  const isView = location.pathname === '/view';
  const selectorPlotInfo = useMemo(() => selectorPlotInfoByIndex.bind(undefined, indexPlot), [indexPlot]);
  const { plot, preview, numQueries, plotData, tabNum, plotCount } = useStore(selectorPlotInfo, shallow);

  const onRemovePlot = useCallback(() => {
    removePlot(indexPlot);
  }, [indexPlot]);

  const active = useRef(false);

  const metricName = useMemo(
    () => (plot.metricName !== promQLMetric ? plot.metricName : plotData.nameMetric),
    [plotData.nameMetric, plot.metricName]
  );

  const what = useMemo(
    () =>
      plot.metricName === promQLMetric
        ? plotData.whats.map((qw) => whatToWhatDesc(qw)).join(', ')
        : plot.what.map((qw) => whatToWhatDesc(qw)).join(', '),
    [plot.metricName, plot.what, plotData.whats]
  );

  const title = useMemo(
    () => (plot.customName || metricName ? `${metricName}${!!what && ': ' + what}` : ''),
    [metricName, plot.customName, what]
  );

  const onOpen = useCallback(() => {
    setOpen(true);
  }, []);

  const onClose = useCallback(() => {
    setOpen(false);
  }, []);

  useEffect(() => {
    active.current = indexPlot === tabNum;
  }, [indexPlot, tabNum]);

  useEffect(() => {
    const onTouchToggle = (event: Event) => {
      let t = event.target as HTMLElement;
      while (t.parentElement && !(t === touchToggle.current || t === sub.current)) {
        t = t.parentElement;
      }
      if (t === touchToggle.current && active.current) {
        setOpen((s) => !s);
        event.preventDefault();
      } else if (t !== sub.current) {
        setOpen(false);
      }
    };
    document.addEventListener('touchstart', onTouchToggle, { passive: false });
    return () => {
      document.removeEventListener('touchstart', onTouchToggle);
    };
  }, []);

  return (
    <li
      className={cn('position-relative', css.plotItem, indexPlot === tabNum && isView && css.activePlotItem)}
      onMouseOver={onOpen}
      onMouseOut={onClose}
      onClick={onClose}
    >
      <PlotLink
        className={cn(
          'nav-link',
          !plotData.error403 && ['p-0', css.preview],
          plot.type === PLOT_TYPE.Event && css.previewEvent
        )}
        indexPlot={indexPlot}
        title={title}
        ref={touchToggle}
      >
        {!!plotData.error403 && <SVGXSquare className={css.icon} />}
        {!!preview && !plotData.error403 && <img alt={title} src={preview} className="w-100 h-100" />}
        {(!preview || numQueries > 0) && !plotData.error403 && !plotData.error && (
          <div className="position-absolute top-50 start-50 translate-middle show-delay">
            <div className="spinner-white-bg spinner-border spinner-border-sm" role="status" aria-hidden="true"></div>
          </div>
        )}
      </PlotLink>

      <ul hidden={!open} className={cn(`nav d-flex flex-column position-absolute start-100 top-0`, css.sub)} ref={sub}>
        <li className={cn('nav-item d-flex flex-row', css.bigPreview)}>
          <PlotLink
            className="nav-link text-nowrap flex-grow-1 text-body fw-bold font-monospace text-decoration-none d-flex flex-row w-0"
            indexPlot={indexPlot}
            title={title}
          >
            {plot.customName ? (
              <span className="text-truncate">{plot.customName}</span>
            ) : (
              <>
                <span className="text-truncate">{metricName}</span>
                {!!metricName && !!what && (
                  <>
                    <span className="pe-1">:</span>
                    <span className="text-secondary text-truncate">{what}</span>
                  </>
                )}
              </>
            )}
          </PlotLink>
          {plotCount > 1 && (
            <span role="button" title="Remove" className="d-block p-2 text-body" onClick={onRemovePlot}>
              <SVGTrash />
            </span>
          )}
        </li>
        {!!preview && !plotData.error403 && (
          <li className="nav-item p-1">
            <img alt={title} src={preview} className={css.bigPreview} />
          </li>
        )}
      </ul>
    </li>
  );
};
